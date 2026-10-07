#!/usr/bin/env bash
# DuckDB with the R2 bucket's Iceberg catalog attached as `lake`:
#
#   BUCKET=my-bucket ./duckdb.sh
#   BUCKET=my-bucket ./duckdb.sh -c "SELECT status, count(*) FROM lake.rideshare.rides GROUP BY ALL"
#
# Needs DuckDB 1.4+ and ICEBERG_CATALOG_TOKEN in .dev.vars.
set -euo pipefail
cd "$(dirname "$0")"
BUCKET="${BUCKET:?set BUCKET to the R2 bucket}"
set -a; . ./.dev.vars; set +a

CATALOG="$(npx --no-install wrangler r2 bucket catalog get "$BUCKET")"
URI="$(sed -n 's/^Catalog URI: *//p' <<<"$CATALOG")"
WAREHOUSE="$(sed -n 's/^Warehouse: *//p' <<<"$CATALOG")"

INIT="$(mktemp)"
trap 'rm -f "$INIT"' EXIT
cat > "$INIT" <<SQL
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE ICEBERG, TOKEN '${ICEBERG_CATALOG_TOKEN}');
ATTACH '${WAREHOUSE}' AS lake (TYPE ICEBERG, ENDPOINT '${URI}');
SQL
duckdb -init "$INIT" "$@"
