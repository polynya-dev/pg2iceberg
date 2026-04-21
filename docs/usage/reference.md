---
icon: lucide/book-open
---

# Reference

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config.yaml` | Path to configuration file |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--log-format` | `text` | Log format: `text`, `json` |

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
    SELECT * FROM pg_publication_tables WHERE pubname = 'pg2iceberg';
    ```

    Also verify the `tables` section of your config lists the table with the correct namespace.
