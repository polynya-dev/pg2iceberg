#!/usr/bin/env bash
# pg2iceberg on Cloudflare, end to end: an ephemeral Neon Postgres, the
# rideshare workload from example/single, pg2iceberg in a Cloudflare
# container writing to an R2 bucket's Iceberg catalog, and DuckDB reading
# it back — every row compared with Postgres. See README.md.
#
#   BUCKET=my-bucket ./run.sh            # DURATION=120 seconds of workload
#   BUCKET=my-bucket ./run.sh sql "..."  # query the tables with Basin SQL
#   BUCKET=my-bucket ./run.sh measure    # freshness; ROWS=10000 in one go
#   BUCKET=my-bucket ./run.sh loadtest   # RATE=1000 rows/s for DURATION=300s
#   ./run.sh settings                    # the database's replication settings
#   ./run.sh down                        # delete the Worker and its container
#
# Needs: Node 20+, Python 3, wrangler logged in to an account on
# Workers Paid, an R2 bucket with its catalog enabled, and an R2 API token
# in .dev.vars:
#
#   ICEBERG_CATALOG_TOKEN=...
set -euo pipefail
cd "$(dirname "$0")"

step() { printf '\n== %s\n' "$*"; }
die() { echo "error: $*" >&2; exit 1; }

wrangler() { npx --no-install wrangler "$@"; }

# The running deployment's connections: the token from .dev.vars, the
# bucket's catalog from wrangler, Postgres from .env.neon.
connections() {
    BUCKET="${BUCKET:?set BUCKET to the R2 bucket}"
    [ -f .dev.vars ] || die "put ICEBERG_CATALOG_TOKEN=<R2 API token> in .dev.vars"
    set -a; . ./.dev.vars; set +a
    local catalog
    catalog="$(wrangler r2 bucket catalog get "$BUCKET")"
    ICEBERG_CATALOG_URL="$(sed -n 's/^Catalog URI: *//p' <<<"$catalog")"
    ICEBERG_WAREHOUSE="$(sed -n 's/^Warehouse: *//p' <<<"$catalog")"
    [ -n "$ICEBERG_WAREHOUSE" ] || die "no catalog on $BUCKET: wrangler r2 bucket catalog enable $BUCKET"
    ICEBERG_NAMESPACE=rideshare
    POSTGRES_URL="$(sed -n 's/^DATABASE_URL_DIRECT=//p' .env.neon 2>/dev/null || true)"
    export POSTGRES_URL ICEBERG_CATALOG_URL ICEBERG_WAREHOUSE ICEBERG_NAMESPACE ICEBERG_CATALOG_TOKEN
}

case "${1:-}" in
    down)
        # The Worker first, so its cron trigger can't start the container
        # again; then the container application, which `wrangler delete`
        # leaves behind — its instance still running.
        app="$(wrangler containers list --json | python3 -c '
import json, sys
print(next((a["id"] for a in json.load(sys.stdin) if a["name"] == "pg2iceberg-pg2iceberg"), ""))')"
        wrangler delete --force || echo "(no Worker to delete)"
        [ -z "$app" ] || yes | wrangler containers delete "$app"
        echo "Deleted. The Neon database expires on its own (see .env.neon)."
        exit 0
        ;;
    sql)
        # Basin SQL, Cloudflare's query engine for the catalog. The R2 API
        # token needs its permission ("Basin SQL Read").
        [ -n "${2:-}" ] || die 'usage: ./run.sh sql "SELECT count(*) FROM rideshare.riders"'
        connections
        WRANGLER_BASIN_SQL_AUTH_TOKEN="$ICEBERG_CATALOG_TOKEN" \
            wrangler basin sql query "$ICEBERG_WAREHOUSE" "$2"
        exit 0
        ;;
    measure)
        # How fresh the tables are: one change, then ROWS rows in one
        # transaction, each timed from Postgres to DuckDB.
        connections
        .venv/bin/python freshness.py
        .venv/bin/python freshness.py bulk "${ROWS:-10000}"
        exit 0
        ;;
    loadtest)
        # Sustained load, sampling the slot's lag and retained WAL, then
        # every row compared.
        connections
        .venv/bin/python -u loadtest.py
        for attempt in 1 2 3 4 5 6; do
            .venv/bin/python compare.py && break
            [ "$attempt" = 6 ] && die "Iceberg still differs from Postgres"
            sleep 20
        done
        exit 0
        ;;
    settings)
        POSTGRES_URL="$(sed -n 's/^DATABASE_URL_DIRECT=//p' .env.neon)" .venv/bin/python - <<'PY'
import os, psycopg2
cur = psycopg2.connect(os.environ["POSTGRES_URL"]).cursor()
for name in ["server_version", "wal_level", "wal_sender_timeout", "max_slot_wal_keep_size"]:
    cur.execute("SELECT current_setting(%s, true)", (name,))
    print(f"{name:24} {cur.fetchone()[0]}")
PY
        exit 0
        ;;
esac

BUCKET="${BUCKET:?set BUCKET to an R2 bucket with its catalog enabled}"
DURATION="${DURATION:-120}"
[ -f .dev.vars ] || die "put ICEBERG_CATALOG_TOKEN=<R2 API token> in .dev.vars"
set -a; . ./.dev.vars; set +a
[ -n "${ICEBERG_CATALOG_TOKEN:-}" ] || die "ICEBERG_CATALOG_TOKEN is empty in .dev.vars"

step "Tools"
npm install --no-audit --no-fund --silent
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install --quiet psycopg2-binary duckdb
PY=.venv/bin/python

step "Postgres: an ephemeral Neon database with logical replication"
if [ ! -f .env.neon ]; then
    npx --yes neon-new@latest --yes --logical-replication --env .env.neon
fi
POSTGRES_URL="$(sed -n 's/^DATABASE_URL_DIRECT=//p' .env.neon)"
[ -n "$POSTGRES_URL" ] || die ".env.neon has no DATABASE_URL_DIRECT"
export POSTGRES_URL
grep -i "expires" .env.neon || true
$PY - <<'PY'
import os, psycopg2
conn = psycopg2.connect(os.environ["POSTGRES_URL"])
conn.autocommit = True
cur = conn.cursor()
cur.execute("SHOW wal_level")
assert cur.fetchone()[0] == "logical", "wal_level isn't logical"
cur.execute("SELECT to_regclass('public.riders') IS NOT NULL")
if cur.fetchone()[0]:
    print("schema already loaded")
else:
    for f in ["schema.sql", "seed.sql", "setup.sql"]:
        cur.execute(open(f"../single/{f}").read())
    print("loaded example/single's schema and seed rows")
PY

step "Catalog: the bucket's Iceberg catalog"
CATALOG="$(wrangler r2 bucket catalog get "$BUCKET")"
ICEBERG_CATALOG_URL="$(sed -n 's/^Catalog URI: *//p' <<<"$CATALOG")"
ICEBERG_WAREHOUSE="$(sed -n 's/^Warehouse: *//p' <<<"$CATALOG")"
[ -n "$ICEBERG_CATALOG_URL" ] && [ -n "$ICEBERG_WAREHOUSE" ] \
    || die "no catalog on $BUCKET: wrangler r2 bucket catalog enable $BUCKET"
ICEBERG_NAMESPACE=rideshare
export ICEBERG_CATALOG_URL ICEBERG_WAREHOUSE ICEBERG_NAMESPACE
echo "catalog $ICEBERG_CATALOG_URL, warehouse $ICEBERG_WAREHOUSE"

step "Deploy: the Worker, and pg2iceberg's container"
SECRETS=.env.secrets
trap 'rm -f "$SECRETS"' EXIT
printf 'POSTGRES_URL=%s\nICEBERG_CATALOG_TOKEN=%s\n' "$POSTGRES_URL" "$ICEBERG_CATALOG_TOKEN" > "$SECRETS"
DEPLOY="$(wrangler deploy --secrets-file "$SECRETS" \
    --var "ICEBERG_CATALOG_URL:$ICEBERG_CATALOG_URL" \
    --var "ICEBERG_WAREHOUSE:$ICEBERG_WAREHOUSE" \
    --var "ICEBERG_NAMESPACE:$ICEBERG_NAMESPACE" 2>&1 | tee /dev/stderr)"
rm -f "$SECRETS"
URL="$(grep -oE 'https://[a-z0-9.-]+\.workers\.dev' <<<"$DEPLOY" | head -1)"
[ -n "$URL" ] || die "couldn't find the Worker's URL in the deploy output"

step "Start: pg2iceberg snapshots the tables, then streams changes"
# The cron trigger starts it within a minute anyway.
curl -fsS "$URL" || true
echo
$PY - <<'PY'
import os, time, psycopg2
conn = psycopg2.connect(os.environ["POSTGRES_URL"])
conn.autocommit = True
deadline = time.time() + 600
while time.time() < deadline:
    with conn.cursor() as cur:
        cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'")
        row = cur.fetchone()
    if row and row[0]:
        print("replicating")
        break
    time.sleep(5)
else:
    raise SystemExit("pg2iceberg didn't start replicating within 10 minutes: "
                     "see its logs in the dashboard (Workers & Pages > pg2iceberg > Containers)")
PY

step "Workload: example/single's, for ${DURATION}s"
DATABASE_URL="$POSTGRES_URL" DURATION="$DURATION" $PY -u ../single/workload.py

step "Compare: Postgres against DuckDB reading the Iceberg tables"
$PY - <<'PY'
import os, time, psycopg2
conn = psycopg2.connect(os.environ["POSTGRES_URL"])
conn.autocommit = True
deadline = time.time() + 300
while time.time() < deadline:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                    "FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'")
        lag = cur.fetchone()[0]
    if lag is not None and lag < 1 << 20:
        print(f"caught up (slot lag {int(lag)} bytes)")
        break
    time.sleep(5)
PY
for attempt in 1 2 3 4 5 6; do
    # The materializer commits on its own cadence after the slot catches up.
    if ICEBERG_CATALOG_TOKEN="$ICEBERG_CATALOG_TOKEN" $PY compare.py; then
        break
    fi
    [ "$attempt" = 6 ] && die "Iceberg still differs from Postgres"
    sleep 20
done

step "Freshness: one change, from Postgres to DuckDB"
ICEBERG_CATALOG_TOKEN="$ICEBERG_CATALOG_TOKEN" $PY freshness.py || true

cat <<EOF

Done. Query the tables with DuckDB:

    BUCKET=$BUCKET ./duckdb.sh

pg2iceberg keeps running in its container. Tear it down with:

    ./run.sh down
EOF
