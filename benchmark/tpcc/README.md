# TPROC-C benchmark

HammerDB's TPROC-C (TPC-C) workload against Postgres, replicated by
pg2iceberg into an Iceberg REST catalog on MinIO, all in Docker. A run
measures how fast pg2iceberg takes changes from the slot and lands them in
Iceberg under a write-heavy OLTP load, and how long it takes to catch up
after.

## Running

```sh
# An image of the pg2iceberg to measure: the repo root, or any worktree.
docker build -f pg2iceberg.Dockerfile -t pg2iceberg-tpcc:dev ../..

OUT=out/dev ./run.sh
```

`run.sh` starts from scratch every time:

1. a fresh stack and a TPROC-C schema of `WAREHOUSES` warehouses
   (`history` gets an `h_id bigserial` primary key: pg2iceberg replicates
   only tables with one);
2. pg2iceberg's snapshot of it, until it's materialized;
3. `VUS` virtual users for `RAMPUP` + `DURATION` minutes;
4. up to `DRAIN_TIMEOUT` seconds for pg2iceberg to catch up: slot lag
   under 64 MB and nothing staged left to materialize.

It writes to `OUT`:

| File | What |
|---|---|
| `summary.txt` | NOPM, peak memory, and rates and lag for the snapshot, the load and the catch-up |
| `snapshot.csv`, `cdc.csv` | `sample.sh`'s samples every 3 s (columns described in it) |
| `metrics.txt` | pg2iceberg's `/metrics` at the end |
| `pg2iceberg.log`, `hammerdb.log`, `build.log` | logs |

The stack stays up after a run; `docker compose --profile pg2iceberg
--profile hammerdb down -v` removes it.

| Variable | Default | |
|---|---|---|
| `WAREHOUSES` | 10 | schema size (~1 GB per 10) |
| `VUS` | 8 | virtual users |
| `RAMPUP` / `DURATION` | 1 / 5 | minutes |
| `DRAIN_TIMEOUT` | 1200 | seconds to catch up before giving up |
| `PG2ICEBERG_IMAGE` | `pg2iceberg-tpcc:dev` | the image to run |
| `PG2ICEBERG_CONFIG` | `./config.yaml` | its config: `config-flush100k.yaml` (10x larger flushes), `config-nocompact.yaml` (no compaction) |

## Pinning CPUs

pg2iceberg's CPU use changes how much load Postgres can drive on a shared
machine. To compare runs fairly, pin them (8 cores: Postgres 0–4,
HammerDB and the stores 5, pg2iceberg 6–7):

```sh
COMPOSE_FILE=docker-compose.yml:pinned.yml OUT=out/dev ./run.sh
```

## Cloud latency

Locally every request takes a millisecond; against a cloud object store
and catalog, tens. `LATENCY=1` puts pg2iceberg a request's latency away
from them:

```sh
COMPOSE_FILE=docker-compose.yml:pinned.yml LATENCY=1 \
  PG2ICEBERG_CONFIG=./config-flush100k.yaml OUT=out/dev-latency ./run.sh
```

- The object store and catalog sit behind Envoy proxies
  (`proxy/envoy.yaml`) that hold each request for `S3_DELAY` (25 ms) and
  `CATALOG_DELAY` (50 ms) before forwarding it, and stream a response at
  most `S3_RATE_KIBPS` (~90 MB/s, one S3 connection's).
- `latency.sh` adds network round trips from pg2iceberg's network
  namespace with netem: `NET_DELAY` (1 ms, another availability zone) to
  the proxies, `PG_DELAY` (1 ms) to Postgres.

The delay is per request, not per packet: netem's 25 ms on every packet
would make every TCP round trip 25 ms, which caps a large transfer at a
few MB/s, far below what S3 serves.

## Notes

- On macOS, run under `caffeinate -ims`: if the machine sleeps mid-run,
  the VM's clock jumps, MinIO rejects requests as skewed, and pg2iceberg
  exits.
- The image keeps line tables and has `perf` installed, and the container
  may trace itself:
  `docker compose exec pg2iceberg sh -c 'perf record -F 99 --call-graph dwarf,16384 -p $(pgrep -x pg2iceberg) -o /tmp/perf.data -- sleep 20'`.
- `reset.sh` drops pg2iceberg's state and the warehouse but keeps the
  TPROC-C data, for another snapshot without rebuilding the schema.
