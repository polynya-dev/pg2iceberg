---
icon: lucide/table-properties
---

# Managing Tables

## Adding a table to an existing pipeline

To replicate a new PostgreSQL table, add it to the `tables` list in your config and restart pg2iceberg. No other manual steps are required.

```yaml
tables:
  - name: public.orders      # existing
  - name: public.products    # newly added
```

On restart, pg2iceberg will:

1. **Update the publication** — runs `ALTER PUBLICATION pg2iceberg_pub ADD TABLE public.products` automatically. The table must already exist in PostgreSQL before you restart.
2. **Snapshot only the new table** — tables already recorded in the checkpoint are skipped. The snapshot captures the state at restart time.
3. **Resume CDC for existing tables** — the replication slot is reused from its last confirmed LSN. No data is lost or re-read.

Once the snapshot completes, the new table enters streaming alongside the existing ones.

!!! note "Table must exist first"
    pg2iceberg does not create PostgreSQL tables. The table must exist before you restart, otherwise `ALTER PUBLICATION ... ADD TABLE` will fail.

## Removing a table

Remove the entry from `tables` and restart. pg2iceberg will stop processing events for that table.

The table is **not** removed from the PostgreSQL publication — it will continue to produce WAL events, which pg2iceberg silently ignores. The Iceberg table and its data are left intact.

If you want to clean up the publication entry manually:

```sql
ALTER PUBLICATION pg2iceberg_pub DROP TABLE public.products;
```

## Changing table configuration

All config changes — partition transforms, watermark column, primary key — take effect on restart. There is no hot-reload.

For partition changes specifically: the new partition spec is applied to the Iceberg table during the next materialization cycle after restart. Existing data files are not rewritten; only new commits use the updated spec.

## All config changes require a restart

pg2iceberg does not watch the config file for changes. Every change — including flush thresholds, materializer intervals, and sink settings — requires a full process restart.

Existing tables resume from the checkpoint on restart with no data loss.
