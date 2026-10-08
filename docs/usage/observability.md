---
icon: lucide/activity
---

# Observability

pg2iceberg has three observability surfaces:

1. **Prometheus metrics and health checks** on an HTTP endpoint, served by every long-running subcommand.
2. **Structured logs** on stdout, via `tracing`.
3. **Iceberg control-plane meta tables**, when `sink.meta_namespace` is set. See [metadata-tables.md](metadata-tables.md).

Use the metrics for live dashboards and alerts, and the meta tables for history, such as a per-commit audit trail you can query with SQL. Traces aren't exported yet; OTLP export is planned.

## The endpoint

`run`, `stream-only`, `materializer-only` and `snapshot` serve three paths:

| Path | Answers |
|---|---|
| `/metrics` | Every metric below, in the Prometheus text format. |
| `/healthz` | `200 ok` while the process is making progress; `503` once it's stuck (see [Health checks](#health-checks)). |
| `/readyz` | `200` while snapshotting or running; `503` while starting up or draining. |

The one-shot subcommands (`compact`, `maintain`, `verify`, `cleanup`, ...) don't serve the endpoint.

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| `metrics_addr` | `PG2ICEBERG_METRICS_ADDR` | `:9090` | `host:port`, or `:port` for every interface. `off` serves nothing. |
| `liveness_timeout` | `PG2ICEBERG_LIVENESS_TIMEOUT` | `5m` | How long nothing may complete before `/healthz` reports the process stuck. |

A `metrics_addr` you set must be free, or pg2iceberg won't start. If you leave it at the default and port 9090 is already taken, for example by a second pg2iceberg on the same host, pg2iceberg logs a warning and runs without the endpoint.

```sh
curl -s localhost:9090/metrics | grep '^pg2iceberg_replication_lag_bytes'
```

## Health checks

pg2iceberg records a metric only when it finishes something: a request to the catalog, object store or coordinator, a flush, an ack, a materializer cycle, or a watcher tick. Nothing records on a timer of its own.

So if nothing has been recorded for `liveness_timeout`, the process is stuck, not idle. Typical causes are a request that never returns or a deadlock. `/healthz` then answers `503`, and restarting the process is the fix.

A process that is merely slow keeps passing. For example, a materializer cycle can take many minutes against a distant catalog, but its requests keep completing in the meantime. An idle `run` or `stream-only` still records every 30 seconds through its watcher.

`materializer-only` completes nothing between cycles, so its liveness timeout is at least three `sink.materializer_interval`s.

Kubernetes probes:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
  periodSeconds: 30
  failureThreshold: 2
readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
  periodSeconds: 10
```

`/readyz` reports ready during the initial snapshot, which can take hours. Rollouts don't need to wait for it to finish.

## Metrics

Every metric pg2iceberg emits is listed below. The `table` label holds the Iceberg table, qualified with its namespace (`public.orders`). Every duration is a histogram in seconds, with buckets from 5 ms to 5 min.

### Process

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_build_info` | gauge | `version`, `revision` | Always 1; the labels name the build. |
| `pg2iceberg_phase` | gauge | `phase` | 1 for what the process is doing: `starting`, `snapshotting`, `running` or `stopping`. |
| `pg2iceberg_last_success_timestamp_seconds` | gauge | `stage` | When `flush`, `ack` (acking the slot), `materialize` or `watch` last completed. |

These come next to the standard `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_virtual_memory_bytes`, `process_open_fds`, `process_max_fds`, `process_threads` and `process_start_time_seconds`. On anything but Linux, only the start time is reported.

### Replication

Read from `pg_replication_slots` every watcher tick (30 s).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_replication_lag_bytes` | gauge | | WAL the source has written past the slot's confirmed position: what pg2iceberg has yet to stage and ack. |
| `pg2iceberg_slot_retained_wal_bytes` | gauge | | WAL the slot keeps the source from removing. |
| `pg2iceberg_slot_safe_wal_size_bytes` | gauge | | WAL the source can still write before the slot passes `max_slot_wal_keep_size`. Absent when that's unlimited. |
| `pg2iceberg_slot_wal_status` | gauge | `status` | 1 for the slot's `wal_status`: `reserved`, `extended`, `unreserved` (WAL is about to be removed) or `lost`. |
| `pg2iceberg_slot_ack_lag_bytes` | gauge | | WAL staged but not yet acked to the slot. |
| `pg2iceberg_replication_reconnects_total` | counter | `outcome` | Attempts to reopen a dropped replication stream: `ok` or `error`. |
| `pg2iceberg_replication_buffered_messages` | gauge | | Decoded messages waiting for the main loop. Near 1000, the main loop is the bottleneck, not Postgres. |

### Staging

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_pipeline_changes_total` | counter | `table`, `op` | Row changes received: `insert`, `update`, `delete` or `truncate`. A reconnect can deliver some again. |
| `pg2iceberg_pipeline_transactions_total` | counter | | Source transactions received. |
| `pg2iceberg_pipeline_flush_total` | counter | | Flushes recorded in the coordinator. |
| `pg2iceberg_pipeline_rows_staged_total` | counter | `table` | Rows staged as Parquet, snapshot rows included. |
| `pg2iceberg_pipeline_staged_bytes_total` | counter | `table` | Bytes of staged Parquet. |
| `pg2iceberg_pipeline_flushed_lsn` | gauge | | Highest source LSN staged and recorded. |

### Materializing

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_materializer_backlog_rows` | gauge | `table` | Rows staged but not yet materialized. Gauged by the watcher, so in distributed mode by `stream-only`. |
| `pg2iceberg_materializer_source_commit_timestamp_seconds` | gauge | `table` | Source commit time of the newest change materialized. |
| `pg2iceberg_materializer_rows_total` | counter | `table` | Rows written to Iceberg, deletes included: each key once per step it changed in. |
| `pg2iceberg_materializer_commits_total` | counter | `table` | Iceberg commits of materialized rows. |
| `pg2iceberg_materializer_files_written_total` | counter | `table`, `kind` | Files those commits added: `data` or `delete`. |
| `pg2iceberg_materializer_bytes_written_total` | counter | `table` | Bytes of those files. |
| `pg2iceberg_materializer_commit_duration_seconds` | histogram | | How long a commit took. |
| `pg2iceberg_materializer_cycle_duration_seconds` | histogram | | How long a cycle over every assigned table took. |
| `pg2iceberg_materializer_cycle_total` | counter | `table` | Times the materializer looked for staged rows for a table. |
| `pg2iceberg_materializer_cycle_failures_total` | counter | | Cycles that failed. In `run` that stops the process; `materializer-only` retries. |
| `pg2iceberg_unfilled_column_defaults_total` | counter | `table`, `column`, `reason` | Columns added with a default Postgres no longer had for existing rows, which read NULL in Iceberg. |
| `pg2iceberg_distributed_workers` | gauge | | `materializer-only`: workers in the consumer group. |
| `pg2iceberg_distributed_assigned_tables` | gauge | | `materializer-only`: tables assigned to this worker. |

### Compaction

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_compaction_runs_total` | counter | `table`, `outcome` | Compactions that `rewritten` files, or `failed`. |
| `pg2iceberg_compaction_files_rewritten_total` | counter | `table` | Data and delete files compaction replaced. |
| `pg2iceberg_compaction_duration_seconds` | histogram | | How long a compaction of one table took. |

### Requests

Every request to the Iceberg catalog, the object store and the coordinator.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_catalog_request_duration_seconds` | histogram | `op` | Catalog requests, by operation (`load_table`, `commit_snapshots`, ...). |
| `pg2iceberg_catalog_request_errors_total` | counter | `op`, `kind` | Failed catalog requests: `conflict`, `not_found` or `other`. |
| `pg2iceberg_blob_request_duration_seconds` | histogram | `op` | Object store requests: `put`, `get`, `list`, `delete`, `register_table`. |
| `pg2iceberg_blob_request_errors_total` | counter | `op` | Failed object store requests. |
| `pg2iceberg_blob_bytes_total` | counter | `op` | Bytes uploaded (`put`) and downloaded (`get`). |
| `pg2iceberg_coord_request_duration_seconds` | histogram | `op` | Coordinator requests (`claim_offsets`, `set_cursor`, ...). |
| `pg2iceberg_coord_request_errors_total` | counter | `op` | Failed coordinator requests. |

### Invariants

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pg2iceberg_invariant_violations_total` | counter | `invariant` | Runtime invariant violations the watcher found. Any is worth a look; `slot_ahead_of_record`, `slot_wal_lost` and `slot_conflicting` also stop the process. |

## What to alert on

[`example/single/monitoring/alerts.yml`](https://github.com/pg2iceberg/pg2iceberg/blob/main/example/single/monitoring/alerts.yml) has Prometheus rules for these. The essentials:

| Alert | Expression |
|---|---|
| Stuck | `time() - max by (instance) (pg2iceberg_last_success_timestamp_seconds) > 600` |
| Falling behind | `pg2iceberg_replication_lag_bytes > 1e9` for 15 minutes |
| About to lose WAL | `pg2iceberg_slot_wal_status{status=~"unreserved\|lost"} == 1` |
| A table is stale | `time() - pg2iceberg_materializer_source_commit_timestamp_seconds > 900 and on (table) pg2iceberg_materializer_backlog_rows > 0` |
| An invariant broke | `increase(pg2iceberg_invariant_violations_total[10m]) > 0` |

How stale a table is only means something while it has a backlog. A table nobody writes to keeps its last source commit time but is fully up to date, which is why the stale-table alert also checks the backlog.

The replication lag counts every write on the source, including writes to tables pg2iceberg doesn't replicate. pg2iceberg acks those writes along with its own.

## Try it

[`example/single`](https://github.com/pg2iceberg/pg2iceberg/tree/main/example/single) runs Prometheus with these rules, and Grafana with a pg2iceberg dashboard:

```sh
cd example/single
docker compose --profile monitoring up -d --wait
docker compose --profile workload up -d workload
```

Grafana is at <http://localhost:3000> (no login), Prometheus at <http://localhost:9090>, and pg2iceberg's own endpoint at <http://localhost:9091/metrics>.

## Logs

The binary logs through [`tracing`](https://docs.rs/tracing) to stdout, as plain text. Tune verbosity with `RUST_LOG`:

```sh
# Default
RUST_LOG=info,pg2iceberg=debug pg2iceberg run --config config.yaml

# Quiet
RUST_LOG=warn pg2iceberg run --config config.yaml

# Maximum verbosity for one crate
RUST_LOG=info,pg2iceberg_logical=trace pg2iceberg run --config config.yaml
```

### Useful log targets

| Target | What |
|---|---|
| `pg2iceberg::run` | Subcommand dispatch + lifecycle messages |
| `pg2iceberg_validate::runtime` | Startup invariants, slot health, snapshot phase progress, reconnects |
| `pg2iceberg_logical::materializer` | Per-cycle materializer events incl. compaction outcomes |
| `pg2iceberg_logical::pipeline` | Per-flush staging events |
| `pg2iceberg_pg::prod::stream` | pgoutput decode + slot interaction |
| `pg2iceberg_iceberg::prod::vended` | Vended-credential refresh events |
| `pg2iceberg_iceberg::compact` | Compaction runs |

## Iceberg control-plane meta tables

When `sink.meta_namespace` is set, pg2iceberg writes operational telemetry to Iceberg tables you can query from any Iceberg-compatible engine: one row per commit, compaction and maintenance run. See [metadata-tables.md](metadata-tables.md) for the schema and example queries.
