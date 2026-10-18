#!/usr/bin/env bash
# One benchmark run from scratch: a fresh TPROC-C schema, pg2iceberg's
# snapshot of it, VUS virtual users for RAMPUP + DURATION minutes, then
# up to DRAIN_TIMEOUT seconds for pg2iceberg to catch up.
#
#   OUT=out/default ./run.sh
#   PG2ICEBERG_IMAGE=... PG2ICEBERG_CONFIG=./config-nocompact.yaml OUT=out/nocompact ./run.sh
#   LATENCY=1 ./run.sh       # with cloud latency: proxy/envoy.yaml + latency.sh
#
# Writes to OUT: snapshot.csv and cdc.csv (sample.sh), hammerdb.log,
# pg2iceberg.log, summary.txt.
set -euo pipefail
cd "$(dirname "$0")"
OUT="${OUT:-out/run}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-1200}"
export WAREHOUSES="${WAREHOUSES:-10}" VUS="${VUS:-8}" RAMPUP="${RAMPUP:-1}" DURATION="${DURATION:-5}"
mkdir -p "$OUT"

running() { [[ "$(docker inspect -f '{{.State.Running}}' pg2iceberg-tpcc-pg2iceberg-1 2>/dev/null)" == true ]]; }
last() { tail -1 "$1" | cut -d, -f"$2"; }
sampler=""
stop_sampler() { [[ -n "$sampler" ]] && kill "$sampler" 2>/dev/null; sampler=""; }
trap stop_sampler EXIT

echo "== fresh stack and schema ($WAREHOUSES warehouses)"
docker compose --profile pg2iceberg --profile hammerdb down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --wait postgres minio create-bucket iceberg-postgres iceberg-rest s3-proxy catalog-proxy >/dev/null
docker compose --profile hammerdb run --rm -T hammerdb ./hammerdbcli auto /bench/build.tcl >"$OUT/build.log" 2>&1
# HammerDB gives `history` no primary key, which pg2iceberg needs.
docker compose exec -T postgres psql -U postgres -d tpcc -qc \
  "ALTER TABLE history ADD COLUMN h_id bigserial PRIMARY KEY; CHECKPOINT;"

if [[ "${LATENCY:-0}" == 1 ]]; then
  # The object store and catalog through their proxies, a request's
  # latency away.
  sed -e 's#http://minio:9000#http://s3-proxy:9000#' \
      -e 's#http://iceberg-rest:8181#http://catalog-proxy:8181#' \
      "${PG2ICEBERG_CONFIG:-./config.yaml}" >"$OUT/config.yaml"
  export PG2ICEBERG_CONFIG="./$OUT/config.yaml"
fi

echo "== snapshot"
INTERVAL=3 ./sample.sh >"$OUT/snapshot.csv" 2>/dev/null & sampler=$!
docker compose --profile pg2iceberg up -d pg2iceberg >/dev/null
# Network round trips (latency.sh), from the snapshot on.
if [[ "${LATENCY:-0}" == 1 ]]; then ./latency.sh | tee "$OUT/latency.txt"; fi
until docker compose logs pg2iceberg 2>&1 | grep -q 'snapshot phase complete'; do
  running || { echo "pg2iceberg exited during the snapshot"; exit 1; }
  sleep 3
done
# Snapshot staged; wait until it's materialized too.
until [[ "$(last "$OUT/snapshot.csv" 5)" == 0 ]]; do
  running || { echo "pg2iceberg exited applying the snapshot"; exit 1; }
  sleep 3
done
stop_sampler

echo "== TPROC-C: $VUS VUs, ${RAMPUP}m ramp-up + ${DURATION}m"
INTERVAL=3 ./sample.sh >"$OUT/cdc.csv" 2>/dev/null & sampler=$!
docker compose --profile hammerdb run --rm -T hammerdb ./hammerdbcli auto /bench/run.tcl >"$OUT/hammerdb.log" 2>&1
load_end=$(last "$OUT/cdc.csv" 1)

echo "== drain (up to ${DRAIN_TIMEOUT}s)"
deadline=$(( $(date +%s) + DRAIN_TIMEOUT ))
until [[ "$(last "$OUT/cdc.csv" 3)" -lt 64 && "$(last "$OUT/cdc.csv" 5)" == 0 ]]; do
  running || { echo "pg2iceberg exited"; break; }
  (( $(date +%s) < deadline )) || { echo "not drained after ${DRAIN_TIMEOUT}s"; break; }
  sleep 3
done
drain_end=$(last "$OUT/cdc.csv" 1)
peak=$(docker compose exec -T pg2iceberg cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo "?")
stop_sampler
docker compose logs --no-log-prefix pg2iceberg >"$OUT/pg2iceberg.log" 2>&1
curl -sf localhost:9391/metrics >"$OUT/metrics.txt" 2>/dev/null || true

{
  grep -h 'TEST RESULT' "$OUT/hammerdb.log" | sed 's/^Vuser 1://'
  echo "pg2iceberg: $(docker inspect -f '{{.State.Status}} oom={{.State.OOMKilled}}' pg2iceberg-tpcc-pg2iceberg-1), peak memory $(( ${peak//[^0-9]/} / 1048576 )) MiB"
  echo "load ended at ${load_end}s, drained (or gave up) at ${drain_end}s: $(( drain_end - load_end ))s to drain"
  echo "-- snapshot"; python3 summarize.py "$OUT/snapshot.csv"
  echo "-- under load"; python3 summarize.py "$OUT/cdc.csv" 0 "$load_end"
  echo "-- drain"; python3 summarize.py "$OUT/cdc.csv" "$load_end" "$drain_end"
  grep -E '^pg2iceberg_(compaction_duration_seconds|materializer_cycle_duration_seconds)_(sum|count)' "$OUT/metrics.txt" || true
} | tee "$OUT/summary.txt"
