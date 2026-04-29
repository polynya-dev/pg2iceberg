#!/bin/sh
# Bootstraps blue→green logical replication once both clusters are ready.
#
# Idempotent on re-run. Fails fast if the environment is misconfigured.
set -eu

BLUE_DSN="${BLUE_DSN:?BLUE_DSN is required}"
GREEN_DSN="${GREEN_DSN:?GREEN_DSN is required}"
GREEN_TO_BLUE_DSN="${GREEN_TO_BLUE_DSN:?GREEN_TO_BLUE_DSN is required}"

log() { echo "[bootstrap] $*"; }

# Run a SQL statement, tolerating "already exists" errors so the script is
# safe to re-run.
try_sql() {
    _dsn="$1"; _sql="$2"; _label="$3"
    if out=$(psql "$_dsn" -v ON_ERROR_STOP=1 -c "$_sql" 2>&1); then
        log "$_label: ok"
    else
        case "$out" in
            *"already exists"*) log "$_label: already exists (skipping)" ;;
            *)                  log "$_label failed: $out"; exit 1 ;;
        esac
    fi
}

log "creating bluegreen publication on blue (accounts, transfers, markers)"
try_sql "$BLUE_DSN" "
    CREATE PUBLICATION bluegreen FOR TABLE
        public.accounts,
        public.transfers,
        _pg2iceberg.markers
" "bluegreen publication"

log "creating bluegreen subscription on green"
# DNS-on-green: since this script runs in its own container, we can't directly
# create the subscription from blue. Do it from green's perspective.
try_sql "$GREEN_DSN" "
    CREATE SUBSCRIPTION bluegreen_sub
    CONNECTION '$GREEN_TO_BLUE_DSN'
    PUBLICATION bluegreen
    WITH (copy_data = true, create_slot = true, slot_name = 'bluegreen_slot')
" "bluegreen subscription"

log "waiting for green to catch up with blue's initial data"
deadline=$(( $(date +%s) + 60 ))
while :; do
    # srsubstate 'r' = ready (not in initial sync). All our tables should
    # be 'r' once the initial COPY has finished.
    pending=$(psql "$GREEN_DSN" -Atc "
        SELECT COUNT(*)
        FROM pg_subscription_rel sr
        JOIN pg_subscription s ON s.oid = sr.srsubid
        WHERE s.subname = 'bluegreen_sub' AND sr.srsubstate <> 'r'
    ")
    if [ "$pending" = "0" ]; then
        log "green is caught up"
        break
    fi
    if [ "$(date +%s)" -gt "$deadline" ]; then
        log "timed out waiting for green to catch up (pending tables: $pending)"
        exit 1
    fi
    sleep 1
done

# Sanity: blue and green should have the same account count now.
blue_count=$(psql "$BLUE_DSN" -Atc "SELECT count(*) FROM accounts")
green_count=$(psql "$GREEN_DSN" -Atc "SELECT count(*) FROM accounts")
log "account counts: blue=$blue_count green=$green_count"
if [ "$blue_count" != "$green_count" ]; then
    log "mismatch — aborting"
    exit 1
fi

log "done."
