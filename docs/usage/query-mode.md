---
icon: lucide/clock
---

# Query Mode

Query mode polls PostgreSQL with watermark-based `SELECT` queries and writes rows directly to Iceberg using merge-on-read semantics. It requires no replication slot and works on any PostgreSQL instance, including read replicas and managed services where logical replication is unavailable.

!!! warning "Logical replication is preferred"
    Query mode is provided for environments where logical replication cannot be enabled. For most use cases, **logical replication mode is strongly recommended** — it captures hard deletes, preserves transaction ordering, and delivers near real-time freshness without polling your database.

    Query mode has three significant drawbacks:

    - **Hard deletes are not detected.** Rows deleted from PostgreSQL are never removed from the Iceberg table.
    - **Freshness depends on poll interval.** Polling your database every few seconds adds load and still results in higher latency than WAL-based replication. Polling too infrequently means stale data; polling too frequently puts unnecessary pressure on PostgreSQL.
    - **Index tuning is your responsibility.** pg2iceberg issues `WHERE watermark_column > $1 ORDER BY watermark_column ASC` on every poll cycle. You must ensure the watermark column is indexed, or each poll becomes a full table scan.

    That said, query mode is good enough for most use cases if you can tolerate higher freshness and do not need hard-delete propagation.

## When to use query mode

| | Logical replication | Query mode |
|---|---|---|
| Hard deletes | Detected | Not detected |
| Transaction ordering | Preserved | Not preserved |
| Infrastructure | Requires replication slot | No slot needed |
| Latency | Near real-time | Bounded by `poll_interval` |
| Schema requirements | None beyond PK | Requires indexed watermark column |

Use query mode only when logical replication is unavailable — for example, on a managed service that does not expose replication slots, or on a read replica.

## Configuration

```yaml
source:
  mode: query
  postgres:
    host: ""
    port: 5432
    database: ""
    user: ""
    password: ""

  query:
    poll_interval: 5s      # how often to poll each table

tables:
  - name: public.orders
    primary_key: [id]            # required — used for deduplication
    watermark_column: updated_at # required — must be monotonically increasing
```

Each table requires:

- **`primary_key`** — one or more columns that uniquely identify a row. Used to deduplicate upserts in merge-on-read.
- **`watermark_column`** — a `timestamp` or `timestamptz` column that is set (or updated) whenever a row changes. pg2iceberg polls with `WHERE watermark_column > $last_watermark ORDER BY watermark_column ASC` and advances the watermark to the maximum value seen in each batch.

!!! warning "Watermark column requirements"
    The watermark column must be `NOT NULL` and must be a timestamp type. Rows with a `NULL` watermark value are skipped and will be re-polled on every cycle. Integer watermarks are not currently supported.

## Initial snapshot

On first run, pg2iceberg performs an initial snapshot before polling begins:

1. Records `MAX(watermark_column)` per table as a fence value.
2. Performs a full CTID-chunked bulk copy of each table (same mechanism as logical mode).
3. Sets the watermark to the fence value captured in step 1.

This ensures rows inserted or updated *during* the snapshot are not missed — polling starts from the fence, catching any changes that arrived while the snapshot was in progress.

## Limitations

- **No hard deletes** — query mode issues `SELECT` statements, so deleted rows are invisible. If you soft-delete by setting a column (e.g. `deleted_at`), those rows will be replicated as updates but the Iceberg table will not receive a delete record.
- **No transaction ordering** — each poll is an independent query. Cross-table or cross-row transaction boundaries are not preserved.
- **Minimum latency is `poll_interval`** — rows are not visible in Iceberg until the next poll cycle completes and flushes.
- **Watermark skew** — if your application sets `updated_at` in application code rather than a database trigger, clock skew or delayed writes can cause rows to be missed. Prefer `DEFAULT now()` with an `ON UPDATE` trigger.

## Flush thresholds

Query mode shares the same flush configuration as logical mode. A flush is triggered when any threshold is reached:

```yaml
sink:
  flush_rows: 1000        # flush after this many buffered rows
  flush_interval: 10s     # flush at least this often
  flush_bytes:            # flush after this many buffered bytes (default: 64MB)
```

After each flush, compaction runs automatically if the configured file-count thresholds are exceeded.

## Checkpointing

After each successful flush, the watermark for each table is saved to the checkpoint store. On restart, polling resumes from the saved watermark — no rows are re-polled.
