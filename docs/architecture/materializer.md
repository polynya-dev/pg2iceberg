---
icon: lucide/layers
---

# Materializer

The materializer runs on a configurable interval (default: 30 seconds) and applies staged events to the Iceberg tables.

## Materialization cycle

For each table:

1. **Read cursor** — fetch the last committed offset from `mat_cursor`
2. **Read log entries** — page through the `log_index` rows after the cursor and download their staged events
3. **Fold, in steps** — at most `materializer_batch_rows` events at a time, deduplicated by primary key:
    - `INSERT` / `UPDATE`: keep the latest row state
    - `DELETE`: mark the primary key for deletion
    - `UPDATE` with unresolved TOAST columns: read the row's previous version and backfill the missing columns
4. **Prepare** — serialize each step's folded rows into Iceberg data and delete files and upload them:
    - Inserts and updates → Parquet data files (partitioned by the table's partition spec)
    - Deletes, and the previous version of updated rows → Parquet equality delete files (PK columns only)
5. **Commit** — at a transaction boundary, commit every step prepared so far as **one atomic catalog update**: each step becomes its own snapshot, but the table's `main` branch moves once
6. **Advance cursor** — only after the commit succeeds; then continue with the next entries until the log is drained or the cycle has read its row budget

### Large transactions

Steps bound the materializer's memory, but a source transaction is never split across commits. When a transaction spans several steps, they're committed together: each step is a snapshot with the next sequence number, so a later step's equality deletes hide rows an earlier step wrote, and readers of the table see either none of the transaction or all of it.

Atomicity is per table. A transaction touching several tables is committed table by table, since Iceberg has no multi-table commit yet.

## Merge-on-read

pg2iceberg uses Iceberg's **merge-on-read** write mode. Rather than rewriting existing data files on every update or delete, it appends:

- New data files for inserts and updates
- Equality delete files for deletes and updates-as-deletes (keyed by primary key)

This keeps the write path fast. Query engines apply the equality deletes at read time. Over time, accumulated delete files increase read amplification — which is why [compaction](compaction.md) exists.
