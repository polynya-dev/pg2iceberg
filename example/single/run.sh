#!/bin/bash
# Smoke test: real Postgres -> pg2iceberg -> Iceberg REST + MinIO, under a
# mixed workload (DURATION seconds, default 300) with external compactions,
# a crash and snapshot expiry alongside. Ends with `pg2iceberg verify` and a
# row-by-row comparison of Postgres against ClickHouse reading the Iceberg
# tables. Exits non-zero if they differ. See README.md.
set -u
cd "$(dirname "$0")"
DURATION=${DURATION:-300}
export DURATION
DC="docker compose"
P2I="$DC run --rm -T pg2iceberg"
CFG="--config /etc/pg2iceberg/config.yaml"
mkdir -p out
log() { echo "[$(date +%H:%M:%S)] $*"; }
psql() { $DC exec -T postgres psql -U postgres -d rideshare -At -c "$1"; }

log "build pg2iceberg"
$DC build -q pg2iceberg || exit 1

log "stack up"
$DC up -d postgres minio create-bucket iceberg-postgres iceberg-rest clickhouse
for _ in $(seq 1 90); do
  curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 && break; sleep 2
done
until psql "SELECT 1" >/dev/null 2>&1; do sleep 1; done

log "pg2iceberg up"
$DC up -d pg2iceberg
sleep 20

log "workload start (${DURATION}s)"
$DC --profile workload up -d workload
START=$(date +%s)
at() { while [ $(( $(date +%s) - START )) -lt "$1" ]; do sleep 1; done; }
# Things that happen to pg2iceberg in production, timed into the workload.
step() {
  [ "$1" -lt "$DURATION" ] || return 0
  at "$1"; log "$2"; shift 2; "$@"; log "  exit $?"
}
step 90  "external compact #1"           sh -c "$P2I compact $CFG > out/compact1.log 2>&1"
step 150 "SIGKILL pg2iceberg + restart"  sh -c "$DC kill -s SIGKILL pg2iceberg && $DC up -d pg2iceberg"
step 210 "external compact #2"           sh -c "$P2I compact $CFG > out/compact2.log 2>&1"
step 240 "maintain (retention 1m)"       sh -c "$P2I maintain $CFG --retention 1m > out/maintain.log 2>&1"

log "waiting for the workload to finish"
while [ -n "$($DC ps -q workload)" ]; do sleep 2; done
$DC --profile workload logs --no-log-prefix workload > out/workload.log 2>&1
# The coordinator's state lives in this Postgres too, so its own writes
# keep the slot a few hundred bytes behind: wait for it to stop moving.
log "waiting for the slot to catch up"
for _ in $(seq 1 120); do
  LAG=$(psql "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'")
  [ "${LAG:-1000000}" -lt 4096 ] && break; sleep 2
done
log "slot lag: $LAG bytes; letting the materializer drain"
sleep 40
$DC logs --no-log-prefix pg2iceberg > out/pg2iceberg.log 2>&1

log "pg2iceberg verify"
$P2I verify $CFG > out/verify.log 2>&1
log "  exit $?"
log "Postgres vs ClickHouse"
python3 compare.py
