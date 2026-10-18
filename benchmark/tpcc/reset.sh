#!/usr/bin/env bash
# Forgets everything pg2iceberg did — its slot, publication and state in
# Postgres, and the Iceberg catalog and warehouse — keeping the TPROC-C
# data, so the next start snapshots it again.
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile pg2iceberg rm -sf pg2iceberg
docker compose exec -T postgres psql -U postgres -d tpcc -v ON_ERROR_STOP=1 -c "
  SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot';
  DROP PUBLICATION IF EXISTS pg2iceberg_pub;
  DROP SCHEMA IF EXISTS _pg2iceberg CASCADE;"
docker compose rm -sfv iceberg-rest iceberg-postgres minio create-bucket
docker compose up -d --wait minio create-bucket iceberg-postgres iceberg-rest
