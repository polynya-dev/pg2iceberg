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

The primary key's index serves each chunk. A chunk is staged like a replicated transaction, and the next one is read from Postgres while it stages, so up to two chunks are in memory. Tables are read one at a time.

## Resumability

Once a chunk is staged, the coordinator records its last row's primary key as the table's progress. Once a table's last chunk is staged, its progress is cleared and the table is marked complete.

On restart, pg2iceberg skips the complete tables and resumes the others after their recorded key. A crash mid-chunk means the chunk is staged again, which is safe because the materializer deduplicates by primary key.

## Handoff to streaming

After the snapshot, pg2iceberg transitions directly into logical replication mode, consuming WAL from the slot that was held open during the entire snapshot. No events are lost: WAL accumulated during the snapshot is replayed starting from the slot's creation LSN.
