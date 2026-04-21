---
icon: lucide/activity
---

# Observability

pg2iceberg exposes Prometheus metrics and OpenTelemetry traces.

## Prometheus

Metrics are served at `http://<host>:9090/metrics` by default. The address is configurable with `metrics_addr` in the config or the `--metrics-addr` flag.

### Replication

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_pipeline_status` | Gauge | `pipeline` | Pipeline state: 0=stopped, 1=starting, 2=snapshotting, 3=running, 4=stopping, 5=error |
| `pg2iceberg_pipeline_uptime_seconds` | Gauge | `pipeline` | Seconds since pipeline started |
| `pg2iceberg_confirmed_lsn` | Gauge | `pipeline` | Confirmed flush LSN |
| `pg2iceberg_replication_lag_bytes` | Gauge | `pipeline` | Replication lag behind the primary in bytes |
| `pg2iceberg_wal_retained_bytes` | Gauge | `pipeline` | WAL bytes retained by the replication slot |

### Event processing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_rows_processed_total` | Counter | `pipeline`, `table`, `operation` | Rows processed, by table and operation (`I`/`U`/`D`) |
| `pg2iceberg_bytes_processed_total` | Counter | `pipeline` | Total bytes written to Iceberg |
| `pg2iceberg_events_buffered` | Gauge | `pipeline` | Events currently held in the flush buffer |
| `pg2iceberg_bytes_buffered` | Gauge | `pipeline` | Estimated bytes in the flush buffer |

### Flush

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_flush_total` | Counter | `pipeline` | Total flush operations |
| `pg2iceberg_flush_errors_total` | Counter | `pipeline` | Failed flushes |
| `pg2iceberg_flush_rows_total` | Counter | `pipeline` | Rows flushed to S3 |
| `pg2iceberg_flush_duration_seconds` | Histogram | `pipeline` | Flush latency |

### Materializer

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_materializer_runs_total` | Counter | `table`, `source` | Materialization cycles (`source`: `buffer` or `s3`) |
| `pg2iceberg_materializer_errors_total` | Counter | `table` | Failed materialization cycles |
| `pg2iceberg_materializer_events_total` | Counter | `table` | Events materialized |
| `pg2iceberg_materializer_data_files_written_total` | Counter | `table` | Data files written per cycle |
| `pg2iceberg_materializer_delete_files_written_total` | Counter | `table` | Equality delete files written per cycle |
| `pg2iceberg_materializer_delete_rows_total` | Counter | `table` | Delete rows written |
| `pg2iceberg_materializer_buffer_events` | Gauge | `table` | Events queued for materialization |
| `pg2iceberg_materializer_materialized_lsn` | Gauge | `table` | Last LSN materialized to Iceberg |
| `pg2iceberg_materializer_duration_seconds` | Histogram | `table` | Materialization latency |

### S3

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_s3_bytes_uploaded_total` | Counter | — | Total bytes uploaded |
| `pg2iceberg_s3_bytes_downloaded_total` | Counter | — | Total bytes downloaded |
| `pg2iceberg_s3_errors_total` | Counter | `operation` | S3 errors by operation |
| `pg2iceberg_s3_operation_duration_seconds` | Histogram | `operation` | S3 operation latency |

### Catalog

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_catalog_errors_total` | Counter | `operation` | Iceberg catalog errors |
| `pg2iceberg_catalog_operation_duration_seconds` | Histogram | `operation` | Catalog operation latency |

### Snapshot

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_snapshot_in_progress` | Gauge | `pipeline` | 1 while snapshot is running |
| `pg2iceberg_snapshot_rows_total` | Counter | `pipeline`, `table` | Rows written during snapshot |
| `pg2iceberg_snapshot_tables_completed_total` | Counter | `pipeline` | Tables that have finished snapshotting |
| `pg2iceberg_snapshot_chunks_completed_total` | Counter | `pipeline`, `table` | CTID chunks completed |
| `pg2iceberg_snapshot_chunk_duration_seconds` | Histogram | `pipeline`, `table` | Per-chunk snapshot latency |

### Compaction and maintenance

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_maintenance_runs_total` | Counter | `table` | Maintenance cycles |
| `pg2iceberg_maintenance_snapshots_expired_total` | Counter | `table` | Iceberg snapshots expired |
| `pg2iceberg_maintenance_orphans_deleted_total` | Counter | `table` | Orphan S3 files deleted |
| `pg2iceberg_maintenance_errors_total` | Counter | `table` | Maintenance failures |
| `pg2iceberg_maintenance_duration_seconds` | Histogram | `table` | Maintenance latency |

### Query mode

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pg2iceberg_query_poll_total` | Counter | `pipeline` | Poll operations |
| `pg2iceberg_query_poll_rows_total` | Counter | `pipeline`, `table` | Rows fetched per poll |
| `pg2iceberg_query_watermark_lag_seconds` | Gauge | `pipeline`, `table` | Lag between current time and the watermark column |
| `pg2iceberg_query_flush_total` | Counter | `pipeline` | Query-mode flush operations |
| `pg2iceberg_query_flush_errors_total` | Counter | `pipeline` | Query-mode flush failures |
| `pg2iceberg_query_data_files_written_total` | Counter | `pipeline`, `table` | Data files written |
| `pg2iceberg_query_delete_files_written_total` | Counter | `pipeline`, `table` | Delete files written |
| `pg2iceberg_query_poll_duration_seconds` | Histogram | `pipeline` | Poll latency |
| `pg2iceberg_query_flush_duration_seconds` | Histogram | `pipeline` | Query flush latency |

## OpenTelemetry

pg2iceberg emits OTLP traces. Tracing is a no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317
```

Traces are exported over gRPC. Standard `OTEL_*` environment variables are respected (e.g. `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`).

### Instrumented components

| Tracer | Covers |
|--------|--------|
| `pg2iceberg/pipeline` | Top-level flush cycles |
| `pg2iceberg/materializer` | Per-table materialization cycles, including TOAST resolution |
| `pg2iceberg/snapshot` | Snapshot chunks |
| `pg2iceberg/stream` | Leaderless log reads |
| `pg2iceberg/commit` | Iceberg commit builds |
| `pg2iceberg/catalog` | Iceberg catalog calls |
| `pg2iceberg/tablewriter` | Data and delete file writes |
| `pg2iceberg/s3` | S3 upload/download |
| `pg2iceberg/compact` | Compaction runs |
| `pg2iceberg/maintain` | Maintenance runs |
| `pg2iceberg/checkpoint` | Checkpoint saves |
| `pg2iceberg/coordinator` | Leaderless log coordinator (PG) |
| `pg2iceberg/sink` | Event sink |

### Key span names

| Span | Tracer | Attributes |
|------|--------|------------|
| `pg2iceberg.flush` | `pipeline` | `flush.rows` |
| `pg2iceberg.materialize` | `materializer` | — |
| `pg2iceberg.materialize.table` | `materializer` | `table` |
| `pg2iceberg.materialize.readEvents` | `materializer` | `stream.entry_count`, `stream.cache_hits`, `stream.cache_misses` |
