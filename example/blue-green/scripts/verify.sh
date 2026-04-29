#!/usr/bin/env bash
# Row-level blue/green verification.
#
#   1. Read the pair (blue_snapshot_id, green_snapshot_id) for every tracked
#      table from the markers index tables (via PyIceberg in a throwaway
#      Python container — the markers table is append-only so equality
#      deletes don't matter here).
#   2. For each pair, invoke iceberg-diff over the REST catalog to do a
#      hash-bucket scan of the row contents. iceberg-diff handles the
#      equality-deletes path, so this works for any workload (INSERT,
#      UPDATE, DELETE).
#
# Usage: ./scripts/verify.sh <marker_uuid>
#
# Exit code: 0 if every table pair diffs equal, 1 otherwise.
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <marker_uuid>" >&2
    exit 2
fi

uuid="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK="${VERIFY_NETWORK:-blue-green_default}"

common_s3_props=(
    --prop s3.endpoint=http://minio:9000
    --prop s3.access-key-id=admin
    --prop s3.secret-access-key=password
    --prop s3.path-style-access=true
    --prop s3.region=us-east-1
)

echo "marker: $uuid"
echo

# Step 1: dump marker pairs as TSV.
pairs="$(
    docker run --rm \
        --network "$NETWORK" \
        -v "${SCRIPT_DIR}/dump-markers.py:/app/dump.py:ro" \
        -e CATALOG_URI=http://iceberg-rest:8181 \
        -e S3_ENDPOINT=http://minio:9000 \
        -e S3_ACCESS_KEY=admin \
        -e S3_SECRET_KEY=password \
        python:3.12-slim \
        bash -c "pip install -q 'pyiceberg[pyarrow,s3fs]==0.8.1' pandas >&2 && python /app/dump.py '$uuid'"
)" || {
    echo "(no marker pairs — give pg2iceberg a few more seconds to observe both sides)" >&2
    exit 2
}

if [ -z "$pairs" ]; then
    echo "(no marker pairs for $uuid)" >&2
    exit 2
fi

echo "$pairs" | sed 's/^/  pair: /'
echo

# Step 2: iceberg-diff per pair.
pass=0
fail=0
while IFS=$'\t' read -r pg_table blue_sid green_sid; do
    # pg_table is like "public.accounts"; Iceberg table is <namespace>.<last segment>.
    iceberg_table="${pg_table##*.}"
    blue_dotted="app_blue.${iceberg_table}"
    green_dotted="app_green.${iceberg_table}"

    printf 'diff %-24s blue@%s vs green@%s ... ' "$pg_table" "$blue_sid" "$green_sid"

    if docker run --rm \
        --network "$NETWORK" \
        ghcr.io/polynya-dev/iceberg-diff:latest \
        --a-uri http://iceberg-rest:8181 \
        --a-table "$blue_dotted" \
        --a-snapshot "$blue_sid" \
        --b-uri http://iceberg-rest:8181 \
        --b-table "$green_dotted" \
        --b-snapshot "$green_sid" \
        "${common_s3_props[@]}" \
        >/tmp/iceberg-diff.out 2>&1; then
        echo "EQUAL"
        pass=$((pass + 1))
    else
        code=$?
        echo "UNEQUAL (exit=$code)"
        sed 's/^/    /' /tmp/iceberg-diff.out
        fail=$((fail + 1))
    fi
done <<< "$pairs"

echo
echo "summary: ${pass} equal, ${fail} unequal"
[ "$fail" -eq 0 ]
