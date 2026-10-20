---
icon: lucide/camera
---

# Snapshot

Before streaming begins, pg2iceberg takes a consistent initial snapshot of every configured table and writes it directly to Iceberg. Ongoing replication only starts once the snapshot is complete.

## Consistency guarantee

On first start, pg2iceberg creates the replication slot and exports a PostgreSQL snapshot at the slot's creation LSN. Every table's snapshot transaction is pinned to this exported snapshot, so all tables are read at exactly the same point in time — regardless of how long the snapshot takes.

On crash recovery, the exported snapshot is no longer available, so transactions use plain `REPEATABLE READ` instead. Duplicate rows from a partial re-snapshot are safe because the materializer deduplicates by primary key.

## Chunking

Each table is read in primary key order, `sink.flush_rows` rows at a time, inside one `REPEATABLE READ` transaction, so every chunk sees the same view:

```sql
-- after the first chunk: the rows past the last one read
SELECT * FROM orders WHERE (o_w_id, o_d_id, o_id) > (3, 7, 2511)
ORDER BY o_w_id, o_d_id, o_id LIMIT 10000
```

The primary key's index serves each chunk. A chunk is written to the table's data files, and the next one is read from Postgres meanwhile, so up to two chunks of a table are in memory. The table is committed once, when all of it is read: readers see it empty, then complete, never part of it.

Up to `source.logical.snapshot_concurrency` tables (default 4) are read at once, each on a task and a connection of its own. The first connection exports its snapshot (`pg_export_snapshot()`) and the others import it (`SET TRANSACTION SNAPSHOT`), so every table is read as of the same moment, whichever connection reads it. A table's rows are read in order by one connection, so the largest table bounds how long the snapshot takes.

## Resumability

Once a table is committed, it's marked complete. On restart, pg2iceberg skips the complete tables and reads the others again, from the start: Postgres still has their rows, and the slot the WAL since the snapshot began, so the changes made meanwhile are applied over them. The snapshot isn't staged: staging makes changes durable that Postgres can't serve again, which a snapshot's rows aren't. If no table was complete, pg2iceberg refuses to start on the slot the snapshot left; drop it, or run `pg2iceberg cleanup`, to start over.

A table added to the configuration later is backfilled while its changes stream, so its rows are staged with them, to be applied ahead of them. As each chunk is staged, the coordinator records its last row's primary key as the table's progress, and a restart resumes the backfill there.

## Handoff to streaming

After the snapshot, pg2iceberg transitions directly into logical replication mode, consuming WAL from the slot that was held open during the entire snapshot. No events are lost: WAL accumulated during the snapshot is replayed starting from the slot's creation LSN.
