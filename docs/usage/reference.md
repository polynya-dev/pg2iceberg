---
icon: lucide/book-open
---

# Reference

## CLI flags

| Flag | Subcommands | Description |
|------|-------------|-------------|
| `--config <path>` | all but `init` | Config file. Default: `PG2ICEBERG_CONFIG`, else `pg2iceberg.yaml` if present, else environment variables alone (see [Configuration](configuration.md)) |
| `--output <path>` | `init` | Where to write the config (default `pg2iceberg.yaml`; `-` prints it) |
| `--force` | `init` | Overwrite an existing file |
| `--retention <duration>` | `maintain` | Overrides `sink.maintenance_retention` |
| `--chunk-size <n>` | `verify` | Rows per Postgres read |
| `--worker-id <id>` | `materializer-only` | Process-unique worker identity |

Logging follows `RUST_LOG` (default `info,pg2iceberg=debug`), e.g. `RUST_LOG=warn,pg2iceberg=info`.

## Metrics

`run`, `stream-only`, `materializer-only` and `snapshot` serve Prometheus metrics on `:9090/metrics` (`metrics_addr`), with `/healthz` and `/readyz` beside it. A few to start with:

| Metric | Description |
|--------|-------------|
| `pg2iceberg_replication_lag_bytes` | WAL the source has written past the slot's confirmed position |
| `pg2iceberg_materializer_backlog_rows` | Rows staged but not yet in Iceberg, by table |
| `pg2iceberg_materializer_source_commit_timestamp_seconds` | Source commit time of the newest change in Iceberg, by table |
| `pg2iceberg_last_success_timestamp_seconds` | When each stage (flush, ack, materialize, watch) last completed |

[Observability](observability.md) lists every metric and what to alert on.

## Troubleshooting

??? question "pg2iceberg stops consuming WAL"

    Check the replication slot lag on the primary:

    ```sql
    SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
    FROM pg_replication_slots
    WHERE slot_name = 'pg2iceberg';
    ```

    A large lag typically means pg2iceberg is not running or is failing to flush. Check `pg2iceberg_last_success_timestamp_seconds`, the request errors and latencies on [`/metrics`](observability.md), and the logs.

??? question "Tables are missing from the Iceberg catalog"

    Ensure the table is included in the PostgreSQL publication:

    ```sql
    SELECT * FROM pg_publication_tables WHERE pubname = 'pg2iceberg_pub';
    ```

    With no tables configured, pg2iceberg logs each table it leaves out at startup (`not replicating: ...`) — one without a primary key, one the role can't read, one with a column Iceberg can't hold — and the tables it replicates (`replicating`). Tables created since pg2iceberg started are picked up at its next start. Otherwise, check the `tables` section of your config lists the table.
