#!/usr/bin/env bash
# diag_wire_compare.sh
#
# Run pg_recvlogical and our raw tokio-postgres reader against two
# replication slots watching the same publication, fire identical DML,
# and compare what each surfaces. Diagnoses whether DML loss is
# happening below us (PG-side or libpq-vs-tokio-postgres) or in our
# decoder.

set -uo pipefail

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5434}"
PG_DB="${PG_DB:-testdb}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"

PSQL="env PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -X -q"

OUT_DIR=$(mktemp -d)
echo "out=$OUT_DIR"

cleanup() {
    [ -n "${RECV_PID:-}" ] && kill "$RECV_PID" 2>/dev/null
    [ -n "${TRACE_PID:-}" ] && kill "$TRACE_PID" 2>/dev/null
    [ -n "${WRAP_PID:-}" ] && kill "$WRAP_PID" 2>/dev/null
    wait 2>/dev/null

    $PSQL -c "DROP TABLE IF EXISTS diag_cmp CASCADE;" >/dev/null 2>&1
    $PSQL -c "DROP PUBLICATION IF EXISTS diag_cmp_pub;" >/dev/null 2>&1
    $PSQL -c "SELECT pg_drop_replication_slot('diag_cmp_a') FROM pg_replication_slots WHERE slot_name='diag_cmp_a' AND NOT active;" >/dev/null 2>&1
    $PSQL -c "SELECT pg_drop_replication_slot('diag_cmp_b') FROM pg_replication_slots WHERE slot_name='diag_cmp_b' AND NOT active;" >/dev/null 2>&1
    $PSQL -c "SELECT pg_drop_replication_slot('diag_cmp_c') FROM pg_replication_slots WHERE slot_name='diag_cmp_c' AND NOT active;" >/dev/null 2>&1
}
trap cleanup EXIT

cleanup  # in case of leftovers
$PSQL <<'SQL'
CREATE TABLE diag_cmp (id INT PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE diag_cmp REPLICA IDENTITY FULL;
CREATE PUBLICATION diag_cmp_pub FOR TABLE diag_cmp;
SELECT pg_create_logical_replication_slot('diag_cmp_a', 'pgoutput');
SELECT pg_create_logical_replication_slot('diag_cmp_b', 'pgoutput');
SELECT pg_create_logical_replication_slot('diag_cmp_c', 'pgoutput');
SQL

WIRE_TRACE_BIN=/Users/hasyimibahrudin/workspace/pg2iceberg/pg2iceberg-rust/target/release/wire_trace
[ -x "$WIRE_TRACE_BIN" ] || { echo "missing $WIRE_TRACE_BIN"; exit 1; }

# Channel A: pg_recvlogical.
PGPASSWORD=$PG_PASSWORD pg_recvlogical \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    --slot=diag_cmp_a --start --no-loop \
    --option=proto_version=1 \
    --option=publication_names=diag_cmp_pub \
    -f "$OUT_DIR/recv_a.bin" 2> "$OUT_DIR/recv_a.log" &
RECV_PID=$!

# Channel B: our tokio-postgres raw reader (no postgres-replication wrapper).
"$WIRE_TRACE_BIN" diag_cmp_b diag_cmp_pub --raw \
    > "$OUT_DIR/recv_b.bin" 2> "$OUT_DIR/trace_b.log" &
TRACE_PID=$!

# Channel C: tokio-postgres + postgres-replication's LogicalReplicationStream
# AND an initial standby_status_update — mirrors our prod path:
# `start_replication(slot, snap_lsn, ...)` immediately followed by
# `stream.send_standby(snap_lsn, snap_lsn).await` at runtime.rs:533.
# We capture the slot's consistent_point and use that as both the start
# LSN and the initial ack target — same shape as snap_lsn in prod.
SLOT_C_LSN=$($PSQL --no-align --tuples-only -c "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='diag_cmp_c'")
echo "diag_cmp_c starts at $SLOT_C_LSN"
"$WIRE_TRACE_BIN" diag_cmp_c diag_cmp_pub --spawned --hold-snapshot \
    --start-lsn="$SLOT_C_LSN" --initial-ack="$SLOT_C_LSN" \
    > /dev/null 2> "$OUT_DIR/trace_c.log" &
WRAP_PID=$!

sleep 1  # let all three connect

echo "fire DML..."
$PSQL <<'SQL'
INSERT INTO diag_cmp (id, name) VALUES (1, 'alice'), (2, 'bob');
SQL
sleep 5
$PSQL <<'SQL'
INSERT INTO diag_cmp (id, name) VALUES (3, 'charlie'), (4, 'dave');
SQL
sleep 3

# Stop all readers.
kill "$RECV_PID" 2>/dev/null
kill "$TRACE_PID" 2>/dev/null
kill "$WRAP_PID" 2>/dev/null
wait "$RECV_PID" 2>/dev/null
wait "$TRACE_PID" 2>/dev/null
wait "$WRAP_PID" 2>/dev/null
RECV_PID=""; TRACE_PID=""; WRAP_PID=""

echo
echo "=== sizes ==="
ls -la "$OUT_DIR"/*.bin
echo
echo "=== pg_recvlogical hex (first 256B) ==="
xxd "$OUT_DIR/recv_a.bin" | head -16
echo
echo "=== wire_trace hex (first 256B) ==="
xxd "$OUT_DIR/recv_b.bin" | head -16
echo
echo "=== wire_trace raw (--raw) stderr summary ==="
grep -E "wal_data|keepalives|EOF|w pg_tag|UNKNOWN" "$OUT_DIR/trace_b.log" | head -30
echo
echo "=== wire_trace wrapped (--wrapped) stderr summary ==="
grep -E "wal_data|keepalives|EOF|pg_tag|OTHER" "$OUT_DIR/trace_c.log" | head -30
echo
echo "=== pgoutput message-type histogram ==="
echo "  pg_recvlogical:"
python3 -c "
import sys
data = open('$OUT_DIR/recv_a.bin','rb').read()
# pg_recvlogical -f writes pgoutput messages back-to-back; first byte of each
# is the type. We can't trivially split without parsing lengths, so just
# count how often each ASCII 'B','I','C','R','U','D','T' appears as byte[0]
# of a record. For a simple count, just histogram the first byte of each
# pgoutput message — pg_recvlogical actually writes contiguous records, so
# we read the file and step through using known lengths. Easier: emit total
# bytes and call out.
print('    total bytes:', len(data))
print('    first 8 bytes:', data[:8].hex())
"
echo "  wire_trace --raw:"
python3 -c "
data = open('$OUT_DIR/recv_b.bin','rb').read()
print('    total bytes:', len(data))
print('    first 8 bytes:', data[:8].hex())
"

echo
echo "kept output in $OUT_DIR"
