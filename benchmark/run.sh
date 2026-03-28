#!/usr/bin/env bash
set -euo pipefail

# pg2iceberg benchmark runner
#
# Usage:
#   ./run.sh [scenario]
#
# Scenarios:
#   steady-state    - 1K rows/s for 15 min (default)
#   high-throughput - 10K rows/s for 15 min
#   toast           - 500 rows/s with 4KB+ large_text for 15 min
#   snapshot        - Seed SEED_SIZE data, then start pg2iceberg (initial snapshot)
#   snapshot-stream - Seed SEED_SIZE data, start pg2iceberg + concurrent 500 rows/s
#   catchup         - Seed, sync, stop pg2iceberg, stream while offline, restart

SCENARIO="${1:-steady-state}"
COMPOSE="docker compose -f docker-compose.yml"

# Configurable parameters (override via env).
SEED_SIZE="${SEED_SIZE:-10GB}"
STREAM_RATE="${STREAM_RATE:-1000}"
STREAM_DURATION="${STREAM_DURATION:-15m}"
INSERT_PCT="${INSERT_PCT:-70}"
UPDATE_PCT="${UPDATE_PCT:-20}"
LARGE_TEXT_PCT="${LARGE_TEXT_PCT:-0}"
SEED_CONCURRENCY="${SEED_CONCURRENCY:-4}"
SEED_BATCH_SIZE="${SEED_BATCH_SIZE:-5000}"
# Max seconds to wait for drain before giving up.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-0}"
STATUS_URL="http://localhost:9090/status"

cd "$(dirname "$0")"

cleanup() {
    echo "==> Cleaning up..."
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# wait_for_drain polls the /status endpoint until buffered_rows == 0 and
# status is "running" (not snapshotting).
#
# Usage:
#   wait_for_drain              # waits + extra flush interval for streaming scenarios
#   wait_for_drain --no-linger  # returns immediately once buffer is empty (snapshot-only)
wait_for_drain() {
    local linger=true
    local timeout="$DRAIN_TIMEOUT"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --no-linger) linger=false; shift ;;
            *) timeout="$1"; shift ;;
        esac
    done

    local start_time=$(date +%s)
    log "Waiting for pg2iceberg to drain (timeout=${timeout}s, linger=$linger)..."

    while true; do
        local elapsed=$(( $(date +%s) - start_time ))
        if [ "$timeout" -gt 0 ] && [ "$elapsed" -ge "$timeout" ]; then
            log "WARNING: drain timeout after ${elapsed}s, proceeding anyway"
            return 1
        fi

        # Fetch status from pg2iceberg.
        local metrics
        metrics=$(curl -sf "$STATUS_URL" 2>/dev/null || echo "")
        if [ -z "$metrics" ]; then
            sleep 2
            continue
        fi

        local buffered
        buffered=$(echo "$metrics" | grep -o '"buffered_rows":[0-9]*' | grep -o '[0-9]*' || echo "-1")
        local status
        status=$(echo "$metrics" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
        local rows_processed
        rows_processed=$(echo "$metrics" | grep -o '"rows_processed":[0-9]*' | grep -o '[0-9]*' || echo "0")
        local bytes_processed
        bytes_processed=$(echo "$metrics" | grep -o '"bytes_processed":[0-9]*' | grep -o '[0-9]*' || echo "0")

        # Compute human-readable size and throughput.
        local size_human=""
        local throughput=""
        if [ "$bytes_processed" -gt 0 ] 2>/dev/null; then
            if [ "$bytes_processed" -ge 1073741824 ]; then
                size_human="$(awk "BEGIN{printf \"%.2fGB\", $bytes_processed/1073741824}")"
            else
                size_human="$(awk "BEGIN{printf \"%.0fMB\", $bytes_processed/1048576}")"
            fi
            if [ "$elapsed" -gt 0 ]; then
                throughput=" $(awk "BEGIN{printf \"%.1fMB/s\", $bytes_processed/$elapsed/1048576}")"
            fi
        fi

        log "  status=$status buffered_rows=$buffered rows_processed=$rows_processed bytes=${size_human:-0}${throughput} elapsed=${elapsed}s"

        # Drained when running (not snapshotting) and no buffered rows.
        if [ "$buffered" = "0" ] && [ "$status" = "running" ]; then
            if [ "$linger" = "true" ]; then
                # Streaming scenarios: new events may still be in-flight between
                # PG and pg2iceberg. Wait one flush interval then re-check.
                log "Buffer empty, waiting one flush interval (10s) for stragglers..."
                sleep 10
                metrics=$(curl -sf "$STATUS_URL" 2>/dev/null || echo "")
                buffered=$(echo "$metrics" | grep -o '"buffered_rows":[0-9]*' | grep -o '[0-9]*' || echo "-1")
                if [ "$buffered" = "0" ]; then
                    log "Drain complete (${elapsed}s)"
                    return 0
                fi
                # Not empty yet — keep polling.
            else
                # No new writes expected — buffer empty means we're done.
                log "Drain complete (${elapsed}s)"
                return 0
            fi
        fi

        sleep 2
    done
}

start_infra() {
    log "Starting infrastructure (postgres, minio, iceberg-rest)..."
    $COMPOSE up -d postgres minio create-bucket iceberg-postgres iceberg-rest
    $COMPOSE up -d --wait postgres iceberg-rest
    log "Infrastructure ready."
}

start_pg2iceberg() {
    log "Starting pg2iceberg..."
    $COMPOSE up -d pg2iceberg
    # Wait for status endpoint to be available.
    local retries=0
    while ! curl -sf "$STATUS_URL" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "$retries" -ge 30 ]; then
            log "WARNING: pg2iceberg status not available after 30s"
            break
        fi
        sleep 1
    done
    log "pg2iceberg started."

    # Start monitoring stack (non-blocking, best-effort).
    log "Starting prometheus + grafana..."
    $COMPOSE up -d prometheus grafana
}

stop_pg2iceberg() {
    log "Stopping pg2iceberg (graceful)..."
    $COMPOSE stop -t 30 pg2iceberg
    log "pg2iceberg stopped."
}

run_seed() {
    local size="${1:-$SEED_SIZE}"
    log "Seeding $size of data..."
    $COMPOSE run --rm -p 6060:6060 bench-writer \
        --mode seed \
        --seed-size "$size" \
        --batch-size "$SEED_BATCH_SIZE" \
        --concurrency "$SEED_CONCURRENCY" \
        --large-text-pct "$LARGE_TEXT_PCT"
    log "Seed complete."
}

run_stream() {
    local rate="${1:-$STREAM_RATE}"
    local duration="${2:-$STREAM_DURATION}"
    log "Streaming at $rate rows/s for $duration..."
    $COMPOSE run --rm bench-writer \
        --mode stream \
        --rate "$rate" \
        --duration "$duration" \
        --insert-pct "$INSERT_PCT" \
        --update-pct "$UPDATE_PCT" \
        --large-text-pct "$LARGE_TEXT_PCT" \
        --batch-size 100
    log "Stream complete."
}

run_verify() {
    log "Running verification..."
    $COMPOSE run --rm bench-verify
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        log "VERIFICATION PASSED"
    else
        log "VERIFICATION FAILED (exit code $exit_code)"
    fi
    return $exit_code
}

monitor_replication_lag() {
    # Run in background, writing lag to stdout every 5s.
    while true; do
        local metrics
        metrics=$(curl -sf "$STATUS_URL" 2>/dev/null || echo "")
        local buffered="N/A"
        local status="N/A"
        if [ -n "$metrics" ]; then
            buffered=$(echo "$metrics" | grep -o '"buffered_rows":[0-9]*' | grep -o '[0-9]*' || echo "N/A")
            status=$(echo "$metrics" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "N/A")
        fi

        local lag
        lag=$($COMPOSE exec -T postgres psql -U postgres -d bench -tAc \
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_bench_slot'" 2>/dev/null || echo "N/A")
        echo "[$(date '+%H:%M:%S')] status=$status buffered_rows=$buffered wal_lag_bytes=$lag"
        sleep 5
    done
}

# ============================================================
# Scenarios
# ============================================================

scenario_steady_state() {
    log "=== Scenario: steady-state ==="
    log "Config: rate=${STREAM_RATE}/s duration=${STREAM_DURATION} insert=${INSERT_PCT}% update=${UPDATE_PCT}%"

    start_infra
    start_pg2iceberg

    # Start lag monitoring in background.
    monitor_replication_lag &
    MONITOR_PID=$!
    trap "kill $MONITOR_PID 2>/dev/null; cleanup" EXIT

    run_stream "$STREAM_RATE" "$STREAM_DURATION"
    wait_for_drain
    stop_pg2iceberg

    kill $MONITOR_PID 2>/dev/null || true
    run_verify
}

scenario_high_throughput() {
    STREAM_RATE=10000
    STREAM_DURATION="${STREAM_DURATION:-15m}"
    log "=== Scenario: high-throughput ==="
    scenario_steady_state
}

scenario_toast() {
    STREAM_RATE=500
    LARGE_TEXT_PCT=50
    STREAM_DURATION="${STREAM_DURATION:-15m}"
    log "=== Scenario: toast ==="
    scenario_steady_state
}

scenario_snapshot() {
    log "=== Scenario: snapshot (initial snapshot of ${SEED_SIZE}) ==="

    start_infra

    # Seed data BEFORE starting pg2iceberg so it must snapshot.
    run_seed "$SEED_SIZE"

    log "Starting pg2iceberg (will perform initial snapshot)..."
    local start_time=$(date +%s)
    start_pg2iceberg

    # No concurrent writes — once buffer is empty, snapshot is done.
    wait_for_drain --no-linger
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    log "Snapshot phase took ~${elapsed}s"

    stop_pg2iceberg
    run_verify
}

scenario_snapshot_stream() {
    log "=== Scenario: snapshot + concurrent stream ==="

    start_infra
    run_seed "$SEED_SIZE"

    start_pg2iceberg

    # Start streaming concurrently.
    log "Starting concurrent stream while snapshot is in progress..."
    run_stream 500 "$STREAM_DURATION"
    wait_for_drain

    stop_pg2iceberg
    run_verify
}

scenario_catchup() {
    log "=== Scenario: WAL catchup ==="

    start_infra
    start_pg2iceberg

    # Initial seed through pg2iceberg (live).
    log "Phase 1: seed ${SEED_SIZE} while pg2iceberg is running..."
    run_seed "$SEED_SIZE"
    wait_for_drain

    # Stop pg2iceberg but KEEP the replication slot.
    log "Phase 2: stopping pg2iceberg, streaming while offline..."
    stop_pg2iceberg

    # Write rows while pg2iceberg is offline.
    run_stream 5000 "100s"

    log "Phase 3: restarting pg2iceberg to drain WAL backlog..."
    local start_time=$(date +%s)

    monitor_replication_lag &
    MONITOR_PID=$!
    trap "kill $MONITOR_PID 2>/dev/null; cleanup" EXIT

    start_pg2iceberg
    wait_for_drain

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    log "Catchup phase took ~${elapsed}s"

    kill $MONITOR_PID 2>/dev/null || true
    stop_pg2iceberg
    run_verify
}

# ============================================================
# Main
# ============================================================

case "$SCENARIO" in
    steady-state)       scenario_steady_state ;;
    high-throughput)    scenario_high_throughput ;;
    toast)              scenario_toast ;;
    snapshot)           scenario_snapshot ;;
    snapshot-stream)    scenario_snapshot_stream ;;
    catchup)            scenario_catchup ;;
    *)
        echo "Unknown scenario: $SCENARIO"
        echo "Available: steady-state, high-throughput, toast, snapshot, snapshot-stream, catchup"
        exit 1
        ;;
esac
