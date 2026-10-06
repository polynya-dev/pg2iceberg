#!/usr/bin/env bash
#
# End-to-end integration test runner for pg2iceberg.
#
# Test cases live in tests/cases/ with these files:
#   <name>__input.sql      – SQL run on PostgreSQL in sequential steps
#   <name>__query.sql      – query run on ClickHouse to verify results
#   <name>__reference.tsv  – expected ClickHouse output
#
# Input SQL is split into steps using markers:
#   -- SETUP --     DDL phase: runs before pg2iceberg starts
#   -- DATA --      DML phase: runs after pg2iceberg connects to replication
#   -- SLEEP <N> -- pause for N seconds (useful between DDL and DML batches)
#
# Multiple DATA steps can be interleaved with SLEEP to test schema evolution:
#   -- SETUP --
#   CREATE TABLE ...;
#   -- DATA --
#   INSERT INTO ...;
#   -- SLEEP 3 --
#   ALTER TABLE ... ADD COLUMN ...;
#   -- DATA --
#   INSERT INTO ... (new_col) VALUES ...;
#
# The table name, publication, and slot are auto-derived from the test name.
# SQL files just use the actual table name directly.
#
# Usage:
#   ./tests/run.sh                        # run all tests sequentially
#   ./tests/run.sh 00001_basic_insert     # run a specific test
#   PARALLEL=4 ./tests/run.sh             # run 4 tests concurrently

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CASES_DIR="$SCRIPT_DIR/cases"

# Endpoints (override via env vars).
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5434}"
PG_DB="${PG_DB:-testdb}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CATALOG_URI="${CATALOG_URI:-http://localhost:8181}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-password}"

# Test settings.
FLUSH_INTERVAL="${FLUSH_INTERVAL:-3s}"
FLUSH_ROWS="${FLUSH_ROWS:-10000}"
NAMESPACE="default"

# Parallelism: number of tests to run concurrently (1 = sequential).
PARALLEL="${PARALLEL:-1}"

# pg2iceberg binary. Built once via `cargo build --release` below.
PG2ICEBERG_BIN="${PG2ICEBERG_BIN:-$PROJECT_DIR/target/release/pg2iceberg}"

passed=0
failed=0
errors=()

# ── Helpers ──────────────────────────────────────────────────────────────

die()  { echo "FATAL: $*" >&2; exit 1; }
info() { echo "--- $*"; }

# Run SQL on PostgreSQL (supports multiple statements).
pg_exec() {
    local sql="$1"
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -X -q -c "$sql" 2>&1
}

# Run SQL on PostgreSQL, return single value.
pg_query() {
    local sql="$1"
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -X -q --no-align --tuples-only -c "$sql" 2>/dev/null
}

# Run SQL on ClickHouse.
ch_query() {
    local sql="$1"
    curl -sf "$CLICKHOUSE_URL" --data-binary "$sql" 2>/dev/null
}

# Run a SELECT against DuckDB's Iceberg REST integration. The user-authored
# query body (e.g. `SELECT … FROM lake.default.<table> ORDER BY id`) is
# wrapped in a `COPY … TO '/dev/stdout'` so the output is deterministic
# TSV — same shape as ClickHouse's TabSeparated, including `\N` for null —
# which lets the same diff/reference machinery work for both engines.
#
# Used for cases that hit ClickHouse-side bugs in specific Iceberg types
# (e.g. UUID round-tripping). Opt in per-case by adding a leading comment
# line `# engine: duckdb` at the top of the `__reference.tsv`. Default
# remains ClickHouse. Comment lines are stripped before the diff so they
# don't contaminate the expected output.
dq_query() {
    local sql="$1"
    # Strip a trailing semicolon if the user-authored query has one — the
    # COPY wrapper takes a SELECT expression, not a statement.
    sql="${sql%;}"
    sql="${sql%$'\n'}"
    sql="${sql%;}"
    local s3_host="${S3_ENDPOINT#http://}"
    s3_host="${s3_host#https://}"
    # COPY targets a real temp file rather than `/dev/stdout` so the
    # ack/affected-row chatter from INSTALL / LOAD / CREATE SECRET /
    # ATTACH (which DuckDB always prints in batch mode) doesn't
    # contaminate the captured output. Only the file's contents go to
    # stdout. Stderr is left on the terminal so iceberg-extension load /
    # ATTACH / secret errors stay visible during iteration.
    local out_file
    out_file=$(mktemp)
    duckdb -batch >/dev/null <<DUCKDB
-- Pin the session TZ so TIMESTAMPTZ rendering matches across machines.
-- DuckDB defaults to the host's local TZ, which would make the same
-- reference TSV pass on UTC CI runners and fail on a +08 dev laptop.
SET TimeZone = 'UTC';
INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;
CREATE OR REPLACE SECRET s3_secret (
    TYPE S3,
    KEY_ID '${S3_ACCESS_KEY}',
    SECRET '${S3_SECRET_KEY}',
    ENDPOINT '${s3_host}',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
-- The dev/test iceberg-rest container is anonymous; DuckDB's iceberg
-- extension defaults to AUTHORIZATION_TYPE 'oauth2' which then
-- demands a SECRET / client_id+client_secret. Pin to 'none' so the
-- ATTACH succeeds against the unauthenticated localhost catalog.
ATTACH 'warehouse' AS lake (TYPE ICEBERG, ENDPOINT '${CATALOG_URI}', AUTHORIZATION_TYPE 'none');
COPY (
${sql}
) TO '${out_file}' WITH (FORMAT CSV, DELIMITER E'\t', HEADER false, NULLSTR '\N');
DUCKDB
    local rc=$?
    if [ "$rc" -eq 0 ] && [ -f "$out_file" ]; then
        cat "$out_file"
    fi
    if [ "${KEEP_TMP:-0}" = "1" ]; then
        # Surface where the raw duckdb output landed so a contributor
        # regenerating a reference TSV can copy the exact bytes.
        echo "  DQ_OUT: $out_file" >&2
    else
        rm -f "$out_file"
    fi
    return "$rc"
}

# Read the engine for a test (default: clickhouse). Opt-in per-case via a
# leading `# engine: <name>` comment line in the `__reference.tsv`. Only
# scans contiguous leading `#` lines so a stray `#` inside expected data
# can't accidentally toggle the engine.
read_engine() {
    local reference_file="$1"
    [ -f "$reference_file" ] || { echo "clickhouse"; return; }
    awk '
        /^#/ {
            sub(/^#[[:space:]]*/, "", $0)
            if (match($0, /^engine:[[:space:]]*[A-Za-z0-9_]+/)) {
                # Print the value after "engine:".
                sub(/^engine:[[:space:]]*/, "", $0)
                print tolower($0)
                exit
            }
            next
        }
        { exit }
    ' "$reference_file" | tr -d '[:space:]' | { read -r v; echo "${v:-clickhouse}"; }
}

# Strip leading comment lines (anything starting with `#`) from a
# reference file so they don't contaminate the diff. Stops at the first
# non-comment line.
strip_reference_comments() {
    local reference_file="$1"
    awk 'BEGIN { stripping = 1 }
         stripping && /^#/ { next }
         { stripping = 0; print }' "$reference_file"
}

# Extract table name from setup SQL (first CREATE TABLE statement).
extract_table_name() {
    local sql="$1"
    echo "$sql" | grep -ioE 'CREATE[[:space:]]+TABLE[[:space:]]+(IF[[:space:]]+NOT[[:space:]]+EXISTS[[:space:]]+)?[a-zA-Z0-9_]+' \
        | head -1 | awk '{print $NF}'
}

# Extract publication name from setup SQL.
extract_publication_name() {
    local sql="$1"
    echo "$sql" | { grep -ioE 'CREATE[[:space:]]+PUBLICATION[[:space:]]+[a-zA-Z0-9_]+' || true; } \
        | head -1 | awk '{print $NF}'
}

# Parse input SQL into steps. Outputs lines: "TYPE<TAB>content"
# TYPE is SETUP, DATA, or SLEEP.
# Using NUL-delimited output to handle multiline SQL.
parse_steps() {
    local file="$1"
    local current_type=""
    local current_buf=""

    emit() {
        if [ -n "$current_type" ] && [ -n "$current_buf" ]; then
            printf '%s\t%s\0' "$current_type" "$current_buf"
        fi
    }

    while IFS= read -r line; do
        if [[ "$line" =~ ^--\ SETUP\ --$ ]]; then
            emit
            current_type="SETUP"
            current_buf=""
            continue
        fi
        if [[ "$line" =~ ^--\ DATA\ --$ ]]; then
            emit
            current_type="DATA"
            current_buf=""
            continue
        fi
        if [[ "$line" =~ ^--\ SLEEP\ ([0-9]+)\ --$ ]]; then
            emit
            printf 'SLEEP\t%s\0' "${BASH_REMATCH[1]}"
            current_type=""
            current_buf=""
            continue
        fi

        if [ -n "$current_type" ]; then
            if [ -n "$current_buf" ]; then
                current_buf="${current_buf}
${line}"
            else
                current_buf="$line"
            fi
        fi
    done < "$file"

    emit
}

# Discover unique test names from query files.
discover_tests() {
    local filter="${1:-}"
    local names=()
    for f in "$CASES_DIR"/*__query.sql; do
        [ -f "$f" ] || continue
        local name
        name="$(basename "$f")"
        name="${name%%__query.sql}"
        if [ -n "$filter" ] && [ "$name" != "$filter" ]; then
            continue
        fi
        names+=("$name")
    done
    printf '%s\n' "${names[@]}" | sort -u
}

# Generate a test-specific config YAML: the case's table and tuning.
# Connection, catalog and storage settings come from the environment
# (see `run_pg2iceberg`), as a deployment's would.
# If an __extra.yaml file exists for the test, its contents are appended
# to the table entry (indented under the table).
gen_config() {
    local table="$1"
    local config_path="$2"
    local extra_file="$3"

    cat > "$config_path" <<YAML
tables:
  - name: public.${table}
YAML

    # Append extra table config (e.g. iceberg partition) if the file exists.
    if [ -n "$extra_file" ] && [ -f "$extra_file" ]; then
        cat "$extra_file" >> "$config_path"
    fi

    # Coord state lives in the source PG; its schema is per-table to
    # keep parallel runs isolated.
    cat >> "$config_path" <<YAML

sink:
  flush_interval: ${FLUSH_INTERVAL}
  flush_rows: ${FLUSH_ROWS}
  materializer_interval: 5s

state:
  coordinator_schema: _pg2iceberg_${table}
YAML
}

# Start pg2iceberg in the background with the test stack's settings in
# its environment. Ambient AWS session settings are dropped: the stack's
# MinIO takes its own keys only.
run_pg2iceberg() {
    local config_path="$1"
    local publication="$2"
    local slot="$3"
    local log="$4"
    env -u AWS_SESSION_TOKEN -u AWS_PROFILE \
        POSTGRES_URL="postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
        PG2ICEBERG_PUBLICATION="$publication" \
        PG2ICEBERG_SLOT="$slot" \
        ICEBERG_CATALOG_URL="$CATALOG_URI" \
        ICEBERG_WAREHOUSE="s3://warehouse/" \
        ICEBERG_NAMESPACE="$NAMESPACE" \
        AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
        AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
        AWS_ENDPOINT_URL_S3="$S3_ENDPOINT" \
        AWS_REGION=us-east-1 \
        "$PG2ICEBERG_BIN" run --config "$config_path" > "$log" 2>&1 &
}

# Wait for pg2iceberg replication slot to become active.
wait_for_replication() {
    local slot="$1"
    local max_wait=15
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        local active
        active=$(pg_query "SELECT active FROM pg_replication_slots WHERE slot_name = '$slot'" 2>/dev/null || echo "")
        if [ "$active" = "t" ]; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "  WARN: replication slot '$slot' not active after ${max_wait}s"
    return 1
}

# ── Test runner ──────────────────────────────────────────────────────────

run_test() {
    local test_name="$1"
    local input_file="$CASES_DIR/${test_name}__input.sql"
    local query_file="$CASES_DIR/${test_name}__query.sql"
    local reference_file="$CASES_DIR/${test_name}__reference.tsv"

    info "TEST: $test_name"

    # In PARALLEL mode each invocation runs in a `&` subshell, so any
    # `failed=…` / `errors+=…` mutations the parent expects are lost.
    # Counting must happen via files in $RESULT_DIR — record exactly one
    # PASS / FAIL marker per test, including for early aborts (missing
    # fixtures, setup/data failure, replication-start failure). Without
    # this, aborted tests vanish from the totals (40 cases → 38 results).
    record_result() {
        local result="$1"
        if [ -n "${tmp_dir:-}" ] && [ -d "$tmp_dir" ]; then
            echo "$result" > "${tmp_dir}/result"
        fi
        if [ -n "${RESULT_DIR:-}" ]; then
            mkdir -p "$RESULT_DIR"
            printf '%s\n' "$result" > "${RESULT_DIR}/${test_name}"
        fi
    }

    # Validate files exist.
    for f in "$input_file" "$query_file" "$reference_file"; do
        if [ ! -f "$f" ]; then
            echo "  SKIP: missing $(basename "$f")"
            record_result FAIL
            return
        fi
    done

    # Extract table and publication names from setup SQL.
    local setup_sql
    setup_sql="$(sed -n '/^-- SETUP --$/,/^-- \(DATA\|SLEEP\)/{ /^--/d; p; }' "$input_file" | head -50)"
    local table
    table="$(extract_table_name "$setup_sql")"
    if [ -z "$table" ]; then
        echo "  SKIP: could not extract table name from setup SQL"
        record_result FAIL
        return
    fi

    local publication
    publication="$(extract_publication_name "$setup_sql")"
    if [ -z "$publication" ]; then
        publication="pg2iceberg_pub_${table}"
    fi
    local slot="pg2iceberg_slot_${table}"

    local engine
    engine="$(read_engine "$reference_file")"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    local config_path="${tmp_dir}/config.yaml"
    local pg2iceberg_log="${tmp_dir}/pg2iceberg.log"
    local pg2iceberg_pid=""

    # ── Cleanup functions ──
    clean_resources() {
        if [ -n "${pg2iceberg_pid:-}" ] && kill -0 "$pg2iceberg_pid" 2>/dev/null; then
            kill "$pg2iceberg_pid" 2>/dev/null || true
            wait "$pg2iceberg_pid" 2>/dev/null || true
        fi

        pg_exec "DROP PUBLICATION IF EXISTS ${publication:-__noop__};" >/dev/null 2>&1 || true
        pg_exec "DROP TABLE IF EXISTS ${table:-__noop__} CASCADE;" >/dev/null 2>&1 || true
        pg_exec "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '${slot:-__noop__}' AND NOT active;" >/dev/null 2>&1 || true
        pg_exec "DROP SCHEMA IF EXISTS _pg2iceberg_${table:-__noop__} CASCADE;" >/dev/null 2>&1 || true
        curl -sf -X DELETE "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table:-__noop__}" >/dev/null 2>&1 || true
        curl -sf -X DELETE "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table:-__noop__}_events" >/dev/null 2>&1 || true
    }

    cleanup_test() {
        clean_resources
        if [ "${KEEP_TMP:-0}" = "1" ]; then
            echo "  TMP: $tmp_dir"
        else
            rm -rf "${tmp_dir:-}"
        fi
    }

    trap cleanup_test RETURN

    # ── Pre-clean ──
    clean_resources 2>/dev/null || true
    pg2iceberg_pid=""

    # ── Execute steps ──
    local replication_started=false

    while IFS=$'\t' read -r -d $'\0' step_type step_content; do
        case "$step_type" in
        SETUP)
            local out
            out=$(pg_exec "$step_content" 2>&1)
            if [ $? -ne 0 ]; then
                echo "  ERROR: setup failed"
                echo "$out" | head -5
                record_result FAIL
                return
            fi
            ;;

        DATA)
            # Start pg2iceberg on first DATA step.
            if [ "$replication_started" = false ]; then
                local extra_file="$CASES_DIR/${test_name}__extra.yaml"
                gen_config "$table" "$config_path" "$extra_file"
                run_pg2iceberg "$config_path" "$publication" "$slot" "$pg2iceberg_log"
                pg2iceberg_pid=$!

                if ! wait_for_replication "$slot"; then
                    echo "  ERROR: pg2iceberg failed to start replication"
                    if [ -f "$pg2iceberg_log" ]; then
                        echo "  Log tail:"
                        tail -5 "$pg2iceberg_log" | sed 's/^/    /'
                    fi
                    record_result FAIL
                    return
                fi
                replication_started=true
            fi

            local out
            out=$(pg_exec "$step_content" 2>&1)
            if [ $? -ne 0 ]; then
                echo "  ERROR: data step failed"
                echo "$out" | head -5
                record_result FAIL
                return
            fi
            ;;

        SLEEP)
            sleep "$step_content"
            ;;
        esac
    done < <(parse_steps "$input_file")

    # ── Wait for flush ──
    sleep 5

    # Graceful shutdown.
    if [ -n "$pg2iceberg_pid" ] && kill -0 "$pg2iceberg_pid" 2>/dev/null; then
        kill -INT "$pg2iceberg_pid" 2>/dev/null || true
        local wait_count=0
        while kill -0 "$pg2iceberg_pid" 2>/dev/null && [ $wait_count -lt 10 ]; do
            sleep 1
            wait_count=$((wait_count + 1))
        done
        if kill -0 "$pg2iceberg_pid" 2>/dev/null; then
            kill -9 "$pg2iceberg_pid" 2>/dev/null || true
        fi
        wait "$pg2iceberg_pid" 2>/dev/null || true
    fi

    # ── Verify via the per-case engine (default ClickHouse) ──
    local query_sql
    query_sql="$(cat "$query_file")"

    local expected
    expected="$(strip_reference_comments "$reference_file")"

    # Retry query a few times — ClickHouse schema cache invalidation is async,
    # so the first query after shutdown may still see stale (empty) state.
    # DuckDB has no cache to drop, so the same retry loop just re-issues the
    # query in case the metadata pointer is still settling on the catalog.
    local actual=""
    local retry=0
    while [ $retry -lt 5 ]; do
        case "$engine" in
        duckdb)
            sleep 1
            actual=$(dq_query "$query_sql") || true
            ;;
        *)
            ch_query "SYSTEM DROP SCHEMA CACHE FOR DATABASE iceberg" >/dev/null 2>&1 || true
            sleep 1
            actual=$(ch_query "$query_sql" 2>&1) || true
            ;;
        esac
        if [ "$actual" = "$expected" ]; then
            break
        fi
        retry=$((retry + 1))
    done

    if [ "$actual" = "$expected" ]; then
        echo "  PASS"
        record_result PASS
    else
        echo "  FAIL [engine=${engine}]"
        diff --color=auto -u \
            <(echo "$expected") \
            <(echo "$actual") \
            | head -30 || true
        if [ -f "$pg2iceberg_log" ]; then
            echo "  pg2iceberg log tail:"
            tail -10 "$pg2iceberg_log" | sed 's/^/    /'
        fi
        record_result FAIL
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────

main() {
    local filter="${1:-}"

    # Preflight checks.
    pg_query "SELECT 1" >/dev/null 2>&1 \
        || die "PostgreSQL not reachable at ${PG_HOST}:${PG_PORT}"
    curl -sf "$CLICKHOUSE_URL" --data-binary "SELECT 1" >/dev/null 2>&1 \
        || die "ClickHouse not reachable at $CLICKHOUSE_URL"
    curl -sf "${CATALOG_URI}/v1/config" >/dev/null 2>&1 \
        || die "Iceberg REST catalog not reachable at $CATALOG_URI"

    # DuckDB is only required if at least one case's reference file
    # opts in via a leading `# engine: duckdb` comment. Skip the check
    # otherwise so contributors who don't have duckdb installed locally
    # can still run the rest of the suite.
    local needs_duckdb=0
    for ref in "$CASES_DIR"/*__reference.tsv; do
        [ -f "$ref" ] || continue
        if [ "$(read_engine "$ref")" = "duckdb" ]; then
            needs_duckdb=1
            break
        fi
    done
    if [ "$needs_duckdb" = "1" ]; then
        command -v duckdb >/dev/null 2>&1 \
            || die "duckdb CLI not on PATH (required by at least one case's '# engine: duckdb' reference comment)"
    fi

    # Build pg2iceberg if the binary doesn't exist or BUILD=1 forces a rebuild.
    if [ ! -x "$PG2ICEBERG_BIN" ] || [ "${BUILD:-0}" = "1" ]; then
        info "Building pg2iceberg (release, --features prod)..."
        (cd "$PROJECT_DIR" && cargo build --release --features prod --bin pg2iceberg) \
            || die "build failed"
    fi
    [ -x "$PG2ICEBERG_BIN" ] || die "pg2iceberg binary not found at $PG2ICEBERG_BIN"

    echo ""
    echo "=== pg2iceberg e2e tests ==="
    echo ""

    local tests
    tests=$(discover_tests "$filter")
    if [ -z "$tests" ]; then
        die "no tests found in $CASES_DIR"
    fi

    # Collect results from parallel runs in a shared temp directory.
    RESULT_DIR=$(mktemp -d)
    export RESULT_DIR

    if [ "$PARALLEL" -le 1 ]; then
        # Sequential mode.
        for test_name in $tests; do
            run_test "$test_name"
            echo ""
        done
    else
        # Parallel mode: run up to $PARALLEL tests concurrently.
        local running=0
        local pids=()
        local pid_names=()

        for test_name in $tests; do
            run_test "$test_name" &
            pids+=($!)
            pid_names+=("$test_name")
            running=$((running + 1))

            # Throttle: wait for a slot when at max concurrency.
            if [ $running -ge "$PARALLEL" ]; then
                wait -n 2>/dev/null || true
                running=$((running - 1))
            fi
        done

        # Wait for remaining tests.
        for pid in "${pids[@]}"; do
            wait "$pid" 2>/dev/null || true
        done
        echo ""
    fi

    # ── Collect results ──
    # Iterate the discovered list so a test that crashed without writing
    # a result file (e.g. shell error before record_result) is still
    # accounted for as MISSING — otherwise the totals silently dip below
    # the test count.
    for test_name in $tests; do
        local result_file="$RESULT_DIR/${test_name}"
        if [ ! -f "$result_file" ]; then
            failed=$((failed + 1))
            errors+=("$test_name (no result recorded)")
            continue
        fi
        local result
        result="$(tr -d '[:space:]' < "$result_file")"
        if [ "$result" = "PASS" ]; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
            errors+=("$test_name")
        fi
    done
    rm -rf "$RESULT_DIR"

    # ── Summary ──
    echo "=== Results: $passed passed, $failed failed ==="
    if [ ${#errors[@]} -gt 0 ]; then
        echo "Failed tests:"
        for e in "${errors[@]}"; do
            echo "  - $e"
        done
        exit 1
    fi
}

main "$@"
