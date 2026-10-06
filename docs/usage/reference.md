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

pg2iceberg exposes Prometheus metrics on `:9090/metrics`.

| Metric | Description |
|--------|-------------|
| `pg2iceberg_lsn_lag_bytes` | WAL bytes between current LSN and confirmed flush LSN |
| `pg2iceberg_rows_written_total` | Total rows written to Iceberg, by table |
| `pg2iceberg_flush_duration_seconds` | Histogram of Iceberg flush durations |
| `pg2iceberg_snapshot_rows_total` | Rows written during initial snapshot, by table |

## Troubleshooting

??? question "pg2iceberg stops consuming WAL"

    Check the replication slot lag on the primary:

    ```sql
    SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
    FROM pg_replication_slots
    WHERE slot_name = 'pg2iceberg';
    ```

    A large lag typically means pg2iceberg is not running or is failing to flush. Check `pg2iceberg_flush_duration_seconds` and application logs.

??? question "Tables are missing from the Iceberg catalog"

    Ensure the table is included in the PostgreSQL publication:

    ```sql
    SELECT * FROM pg_publication_tables WHERE pubname = 'pg2iceberg_pub';
    ```

    With no tables configured, pg2iceberg logs each table it leaves out at startup (`not replicating: ...`) — one without a primary key, one the role can't read, one with a column Iceberg can't hold — and the tables it replicates (`replicating`). Tables created since pg2iceberg started are picked up at its next start. Otherwise, check the `tables` section of your config lists the table.
