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

# Generate a test-specific config YAML.
# If an __extra.yaml file exists for the test, its contents are appended
# to the table entry (indented under the table).
gen_config() {
    local table="$1"
    local publication="$2"
    local slot="$3"
    local config_path="$4"
    local state_path="$5"
    local extra_file="$6"

    cat > "$config_path" <<YAML
tables:
  - name: public.${table}
YAML

    # Append extra table config (e.g. iceberg partition) if the file exists.
    if [ -n "$extra_file" ] && [ -f "$extra_file" ]; then
        cat "$extra_file" >> "$config_path"
    fi

    cat >> "$config_path" <<YAML

source:
  mode: logical
  postgres:
    host: ${PG_HOST}
    port: ${PG_PORT}
    database: ${PG_DB}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
  logical:
    publication_name: ${publication}
    slot_name: ${slot}

sink:
  catalog_uri: ${CATALOG_URI}
  warehouse: s3://warehouse/
  namespace: ${NAMESPACE}
  s3_endpoint: ${S3_ENDPOINT}
  s3_access_key: ${S3_ACCESS_KEY}
  s3_secret_key: ${S3_SECRET_KEY}
  s3_region: us-east-1
  flush_interval: ${FLUSH_INTERVAL}
  flush_rows: ${FLUSH_ROWS}
  materializer_interval: 5s

state:
  path: ${state_path}
YAML
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

    # Validate files exist.
    for f in "$input_file" "$query_file" "$reference_file"; do
        if [ ! -f "$f" ]; then
            echo "  SKIP: missing $(basename "$f")"
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
        return
    fi

    local publication
    publication="$(extract_publication_name "$setup_sql")"
    if [ -z "$publication" ]; then
        publication="pg2iceberg_pub_${table}"
    fi
    local slot="pg2iceberg_slot_${table}"

    local tmp_dir
    tmp_dir=$(mktemp -d)
    local config_path="${tmp_dir}/config.yaml"
    local state_path="${tmp_dir}/state.json"
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
        curl -sf -X DELETE "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table:-__noop__}" >/dev/null 2>&1 || true
        curl -sf -X DELETE "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table:-__noop__}_events" >/dev/null 2>&1 || true
    }

    cleanup_test() {
        clean_resources
        rm -rf "${tmp_dir:-}"
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
                failed=$((failed + 1))
                errors+=("$test_name")
                return
            fi
            ;;

        DATA)
            # Start pg2iceberg on first DATA step.
            if [ "$replication_started" = false ]; then
                local extra_file="$CASES_DIR/${test_name}__extra.yaml"
                gen_config "$table" "$publication" "$slot" "$config_path" "$state_path" "$extra_file"
                "$PROJECT_DIR/bin/pg2iceberg" --config "$config_path" > "$pg2iceberg_log" 2>&1 &
                pg2iceberg_pid=$!

                if ! wait_for_replication "$slot"; then
                    echo "  ERROR: pg2iceberg failed to start replication"
                    if [ -f "$pg2iceberg_log" ]; then
                        echo "  Log tail:"
                        tail -5 "$pg2iceberg_log" | sed 's/^/    /'
                    fi
                    failed=$((failed + 1))
                    errors+=("$test_name")
                    return
                fi
                replication_started=true
            fi

            local out
            out=$(pg_exec "$step_content" 2>&1)
            if [ $? -ne 0 ]; then
                echo "  ERROR: data step failed"
                echo "$out" | head -5
                failed=$((failed + 1))
                errors+=("$test_name")
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

    # ── Verify via ClickHouse ──
    local query_sql
    query_sql="$(cat "$query_file")"

    local expected
    expected="$(cat "$reference_file")"

    # Retry query a few times — ClickHouse schema cache invalidation is async,
    # so the first query after shutdown may still see stale (empty) state.
    local actual=""
    local retry=0
    while [ $retry -lt 5 ]; do
        ch_query "SYSTEM DROP SCHEMA CACHE FOR DATABASE iceberg" >/dev/null 2>&1 || true
        sleep 1
        actual=$(ch_query "$query_sql" 2>&1) || true
        if [ "$actual" = "$expected" ]; then
            break
        fi
        retry=$((retry + 1))
    done

    if [ "$actual" = "$expected" ]; then
        echo "  PASS"
        echo "PASS" > "${tmp_dir}/result"
    else
        echo "  FAIL"
        diff --color=auto -u \
            <(echo "$expected") \
            <(echo "$actual") \
            | head -30 || true
        if [ -f "$pg2iceberg_log" ]; then
            echo "  pg2iceberg log tail:"
            tail -10 "$pg2iceberg_log" | sed 's/^/    /'
        fi
        echo "FAIL" > "${tmp_dir}/result"
    fi
    # Preserve result dir for the collector (cleanup_test still runs on RETURN,
    # but we copy the result out first).
    if [ -n "${RESULT_DIR:-}" ]; then
        cp "${tmp_dir}/result" "${RESULT_DIR}/${test_name}"
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

    # Build pg2iceberg.
    info "Building pg2iceberg..."
    (cd "$PROJECT_DIR" && go build -o bin/pg2iceberg ./cmd/pg2iceberg) \
        || die "build failed"

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
    for result_file in "$RESULT_DIR"/*; do
        [ -f "$result_file" ] || continue
        local test_name
        test_name="$(basename "$result_file")"
        local result
        result="$(cat "$result_file")"
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
