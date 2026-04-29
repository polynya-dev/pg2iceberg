#!/usr/bin/env bash
# Insert an alignment marker on blue. The bluegreen subscription replicates it
# to green; both pg2iceberg instances will observe it at the next COMMIT and
# record a row per tracked table in their respective markers index table.
#
# Usage: ./insert-marker.sh [uuid]
#   If omitted, a random UUID is generated.
set -euo pipefail

uuid="${1:-$(python3 -c 'import uuid; print(uuid.uuid4())')}"

PGPASSWORD=postgres psql \
  -h localhost -p 5610 -U postgres -d app \
  -v ON_ERROR_STOP=1 \
  -c "INSERT INTO _pg2iceberg.markers (uuid) VALUES ('$uuid')"

echo "inserted marker: $uuid"
echo "wait a few seconds for both pipelines to observe and record it, then run:"
echo "  ./scripts/verify.sh $uuid"
