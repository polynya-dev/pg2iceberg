---
icon: lucide/layers
---

# Materializer

The materializer runs on a configurable interval (default: 30 seconds) and applies staged events to the Iceberg tables.

## Materialization cycle

For each table:

1. **Read cursor** — fetch the last committed offset from `mat_cursor`
2. **Read log entries** — fetch all `log_index` rows with offset > cursor; download or cache-hit events
3. **Fold** — deduplicate events by primary key, applying operation semantics:
    - `INSERT` / `UPDATE`: keep the latest row state
    - `DELETE`: mark the primary key for deletion
    - `UPDATE` with unresolved TOAST columns: download the affected files from the previous snapshot and backfill the missing columns
4. **Prepare** — serialize the folded rows into Iceberg data and delete files:
    - Inserts and updates → Parquet data files (partitioned by the table's partition spec)
    - Deletes → Parquet equality delete files (PK columns only)
5. **Commit** — atomically commit all tables in a single `POST /v1/transactions/commit` request to the Iceberg catalog, using an optimistic lock on each table's snapshot ID

After a successful commit, the materializer advances `mat_cursor` for each table and confirms the flush LSN back to PostgreSQL, allowing the replication slot to reclaim WAL.

## Merge-on-read

pg2iceberg uses Iceberg's **merge-on-read** write mode. Rather than rewriting existing data files on every update or delete, it appends:

- New data files for inserts and updates
- Equality delete files for deletes and updates-as-deletes (keyed by primary key)

This keeps the write path fast. Query engines apply the equality deletes at read time. Over time, accumulated delete files increase read amplification — which is why [compaction](compaction.md) exists.
