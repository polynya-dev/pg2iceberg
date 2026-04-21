---
icon: lucide/camera
---

# Snapshot

Before streaming begins, pg2iceberg takes a consistent initial snapshot of every configured table and writes it directly to Iceberg. Ongoing replication only starts once the snapshot is complete.

## Consistency guarantee

On first start, pg2iceberg creates the replication slot and exports a PostgreSQL snapshot at the slot's creation LSN. Every table's snapshot transaction is pinned to this exported snapshot, so all tables are read at exactly the same point in time — regardless of how long the snapshot takes.

On crash recovery, the exported snapshot is no longer available, so transactions use plain `REPEATABLE READ` instead. Duplicate rows from a partial re-snapshot are safe because the materializer deduplicates by primary key.

## CTID chunking

Rather than reading each table in a single query, pg2iceberg divides it into CTID page ranges and reads them in parallel:

1. Query `pg_class.relpages` to estimate the table's page count
2. Divide into chunks of 2048 pages each (one chunk covers ~16 MB of heap at 8 KB/page)
3. Execute each chunk as a bounded `ctid` range query:

```sql
-- intermediate chunk
SELECT * FROM orders WHERE ctid >= '(0,0)'::tid AND ctid < '(2048,0)'::tid

-- last chunk — open-ended to catch pages beyond the stale relpages estimate
SELECT * FROM orders WHERE ctid >= '(2048,0)'::tid
```

Tables are processed up to the configured concurrency limit (default: 1–3 tables in parallel).

## Resumability

Chunk progress is tracked in the checkpoint after each chunk commits to Iceberg:

```
SnapshotChunks:  {"public.orders": 5, "public.customers": 2}
SnapshotedTables: {"public.orders": true}
SnapshotComplete: false
```

On restart, pg2iceberg skips chunks up to the last completed index and resumes from where it left off. The checkpoint is saved **after** the Iceberg commit for that chunk — a crash mid-chunk means the chunk is re-run, which is safe because the rows are idempotent with respect to primary key deduplication.

Once all tables are done, `SnapshotComplete` is set to `true` and the snapshot is never re-run.

## Handoff to streaming

After the snapshot, pg2iceberg transitions directly into logical replication mode, consuming WAL from the slot that was held open during the entire snapshot. No events are lost: WAL accumulated during the snapshot is replayed starting from the slot's creation LSN.
