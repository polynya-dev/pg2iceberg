---
icon: lucide/activity
---

# Observability

The Rust port's observability surface is intentionally narrower than the Go reference's today. Three layers exist:

1. **Structured logs** via `tracing` + `tracing-subscriber` — wired and on by default.
2. **Iceberg control-plane meta tables** — wired (4 of 5; see [metadata-tables.md](metadata-tables.md)) when `sink.meta_namespace` is set.
3. **Prometheus metrics endpoint and OTLP trace export** — **not yet wired**. Tracked as remaining gaps vs the Go reference.

## Structured logs

The binary uses [`tracing`](https://docs.rs/tracing) for all events, and `tracing-subscriber` writes them to stdout. Tune verbosity with `RUST_LOG`:

```sh
# Default
RUST_LOG=info,pg2iceberg=debug pg2iceberg run --config config.yaml

# Quiet
RUST_LOG=warn pg2iceberg run --config config.yaml

# Maximum verbosity for one crate
RUST_LOG=info,pg2iceberg_logical=trace pg2iceberg run --config config.yaml
```

Each log line carries the standard `tracing` attributes (`level`, `target`, `module_path`, `file`, `line`, plus any structured fields the call site attached). Pipe through `jq` if you prefer JSON:

```sh
RUST_LOG=info pg2iceberg run --config config.yaml 2>&1 | jq -R 'fromjson? // .'
```

(Setting up the JSON layer is a follow-on; the default `fmt` layer is human-readable.)

### Useful log targets

| Target | What |
|---|---|
| `pg2iceberg::run` | Subcommand dispatch + lifecycle messages |
| `pg2iceberg_validate::runtime` | Startup invariants, slot health, snapshot phase progress |
| `pg2iceberg_logical::materializer` | Per-cycle materializer events incl. compaction outcomes |
| `pg2iceberg_logical::pipeline` | Per-flush staging events |
| `pg2iceberg_pg::prod::stream` | pgoutput decode + slot interaction |
| `pg2iceberg_iceberg::prod::vended` | Vended-credential refresh events |
| `pg2iceberg_iceberg::compact` | Compaction runs |

## Iceberg control-plane meta tables

When `sink.meta_namespace` is set, pg2iceberg writes operational telemetry to four Iceberg tables — queryable from any Iceberg-compatible engine. See [metadata-tables.md](metadata-tables.md) for the full schema and example queries.

This is the recommended mechanism for **historical observability** (per-commit audit trail, throughput by table, freshness lag) — same data path you already query, no new infrastructure.

## Prometheus metrics — not yet wired

The Go reference exposes a Prometheus endpoint on `:9090/metrics` and a configurable `metrics_addr`. The Rust port has a `Metrics` trait wired through every hot path (counters + histograms in `pg2iceberg-core::metrics`), but the operational HTTP endpoint that exports them is **not yet implemented**. The `metrics_addr` YAML field is parsed and ignored.

When wired, the planned metric set mirrors Go's surface:

| Domain | Examples |
|---|---|
| Pipeline state | `pg2iceberg_pipeline_status`, `pg2iceberg_confirmed_lsn`, `pg2iceberg_replication_lag_bytes` |
| Event processing | `pg2iceberg_rows_processed_total`, `pg2iceberg_events_buffered` |
| Flush + materializer | `pg2iceberg_flush_duration_seconds`, `pg2iceberg_materializer_duration_seconds` |
| S3 + catalog | `pg2iceberg_s3_bytes_uploaded_total`, `pg2iceberg_catalog_operation_duration_seconds` |

Until the endpoint lands, the Iceberg meta tables and structured logs cover the same questions for non-real-time use cases (what was the lag yesterday, how many rows did we replicate this hour, is the materializer falling behind).

## OpenTelemetry / OTLP — not yet wired

The Go reference exports distributed traces via OTLP/gRPC when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The Rust port instruments the same code paths via `tracing` spans but **does not export them**; output is stdout-only.

Wiring an OTLP exporter is straightforward (the `tracing-opentelemetry` crate bridges directly) but hasn't been done yet. If you need centralized trace collection today, parse the structured logs into your trace store, or wait for this to be wired.

## Health checks (k8s probes)

Not yet wired. The Go reference exposes `/healthz` and `/ready` endpoints on the metrics server; the Rust port has no equivalent. For k8s deployments today, use the `connect-pg` and `connect-iceberg` subcommands as `initContainer` probes:

```yaml
initContainers:
  - name: connect-pg
    image: ghcr.io/polynya-dev/pg2iceberg-rust:latest
    command: ["pg2iceberg", "connect-pg", "--config", "/etc/pg2iceberg/config.yaml"]
  - name: connect-iceberg
    image: ghcr.io/polynya-dev/pg2iceberg-rust:latest
    command: ["pg2iceberg", "connect-iceberg", "--config", "/etc/pg2iceberg/config.yaml"]
```

Each exits 0 on success, non-zero with a descriptive message on failure.
