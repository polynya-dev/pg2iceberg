#!/usr/bin/env bash
# Convenience wrapper: run verify.py inside a throwaway python container so
# users don't need a local Python + PyIceberg install.
#
# Usage: ./scripts/verify.sh <marker_uuid>
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <marker_uuid>" >&2
    exit 2
fi

uuid="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker run --rm \
    --network blue-green_default \
    -v "${SCRIPT_DIR}/verify.py:/app/verify.py" \
    -e CATALOG_URI=http://iceberg-rest:8181 \
    -e S3_ENDPOINT=http://minio:9000 \
    -e S3_ACCESS_KEY=admin \
    -e S3_SECRET_KEY=password \
    python:3.12-slim \
    bash -c "pip install -q 'pyiceberg[pyarrow,s3fs]==0.8.1' pandas && python /app/verify.py '$uuid'"
