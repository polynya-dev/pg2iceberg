---
icon: lucide/table-2
---

# Metadata Tables

When `sink.meta_namespace` is set, pg2iceberg writes operational metadata into a set of Iceberg tables in that namespace. This gives you a queryable audit log of every commit, compaction, maintenance run, and (in Go) checkpoint save — directly accessible from any Iceberg-compatible query engine.

In the Rust port, four of the five tables are wired and auto-written; `checkpoints` is exposed via API but not yet auto-emitted (the Go reference writes one per CDC checkpoint save; the Rust port doesn't do periodic CDC checkpoint saves today, by design).

## Namespace

Metadata tables are off by default in the Rust port. Set the namespace to enable:

```yaml
sink:
  meta_namespace: _pg2iceberg_meta
```

The namespace is created if missing. Each table is auto-created on first write.

To disable: leave `meta_namespace` empty.

## Tables

### `commits`

One row per materialization or query-mode flush, per table. Partitioned by `day(ts)`.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Commit timestamp |
| `worker_id` | `text` | Worker ID (distributed mode) |
| `table_name` | `text` | Source table (e.g. `public.orders`) |
| `mode` | `text` | `materialize` or `query` |
| `snapshot_id` | `bigint` | Iceberg snapshot ID |
| `sequence_number` | `bigint` | Monotonic sequence per table |
| `lsn` | `bigint` | PostgreSQL WAL LSN at commit |
| `rows` | `bigint` | Rows written |
| `bytes` | `bigint` | Bytes flushed |
| `duration_ms` | `bigint` | Commit duration |
| `data_files` | `int` | Data files written |
| `delete_files` | `int` | Equality delete files written |
| `max_source_ts` | `timestamptz` | Latest source-side event timestamp |
| `schema_id` | `int` | Iceberg schema ID at commit time |
| `tx_count` | `int` | Distinct PostgreSQL transaction IDs (logical mode) |
| `pg2iceberg_commit_sha` | `text` | Git SHA of the pg2iceberg binary |

### `compactions`

One row per compaction run per table. Partitioned by `day(ts)`.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Compaction timestamp |
| `worker_id` | `text` | Worker ID |
| `table_name` | `text` | Table compacted |
| `snapshot_id` | `bigint` | Iceberg snapshot ID after compaction |
| `sequence_number` | `bigint` | Monotonic sequence per table |
| `input_data_files` | `int` | Data files before compaction |
| `input_delete_files` | `int` | Delete files before compaction |
| `output_data_files` | `int` | Data files after compaction |
| `rows_rewritten` | `bigint` | Rows rewritten |
| `rows_removed` | `bigint` | Rows removed (deletes applied) |
| `bytes_before` | `bigint` | Total file bytes before compaction |
| `bytes_after` | `bigint` | Total file bytes after compaction |
| `duration_ms` | `bigint` | Compaction duration |
| `pg2iceberg_commit_sha` | `text` | Git SHA of the pg2iceberg binary |

### `maintenance`

One row per maintenance operation (snapshot expiry or orphan cleanup) per table. Partitioned by `day(ts)`.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Maintenance timestamp |
| `worker_id` | `text` | Worker ID |
| `table_name` | `text` | Table maintained |
| `operation` | `text` | `expire_snapshots` or `clean_orphans` |
| `items_affected` | `int` | Snapshots expired or orphan files deleted |
| `bytes_freed` | `bigint` | S3 bytes freed (`clean_orphans` only) |
| `duration_ms` | `bigint` | Operation duration |
| `pg2iceberg_commit_sha` | `text` | Git SHA of the pg2iceberg binary |

### `markers`

One row per `(marker_uuid, table)` pair, written when blue-green marker mode is enabled. Partitioned by `day(ts)`.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Emission timestamp |
| `worker_id` | `text` | Worker ID |
| `marker_uuid` | `text` | Operator-supplied marker identity (PK component) |
| `table_name` | `text` | User table this marker covers (PK component) |
| `snapshot_id` | `bigint` | Iceberg snapshot id at the marker's WAL alignment point |

The blue-green tooling joins this table across two pg2iceberg replicas on `marker_uuid` to verify replica equivalence at known WAL points. See [`example/blue-green/`](https://github.com/polynya-dev/pg2iceberg/tree/main/pg2iceberg-rust/example/blue-green) for the full setup.

### `checkpoints` (not auto-written in Rust)

The Go reference writes one row per CDC checkpoint save. The Rust port doesn't do periodic CDC checkpoint saves (recovery doesn't depend on them — the slot's `confirmed_flush_lsn` plus per-table state is sufficient), so this table doesn't get auto-emitted today. The `Materializer::record_checkpoint` API is exposed for callers who want to emit one manually.

When wired, the schema matches Go:

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Checkpoint timestamp |
| `worker_id` | `text` | Worker ID |
| `lsn` | `bigint` | Confirmed flush LSN |
| `last_flush_at` | `timestamptz` | Timestamp of the last flush |
| `pg2iceberg_commit_sha` | `text` | Git SHA of the pg2iceberg binary |

## Useful queries

### Data freshness per table

How stale is each table? `max_source_ts` is the latest PostgreSQL event timestamp in a commit, so `now() - max_source_ts` is the true end-to-end lag — from when a row changed in PostgreSQL to when it landed in Iceberg.

```sql
SELECT
    table_name,
    max(max_source_ts)                                    AS last_event_at,
    dateDiff('second', max(max_source_ts), now())         AS lag_seconds
FROM _pg2iceberg.commits
WHERE ts >= now() - INTERVAL 1 HOUR
  AND max_source_ts IS NOT NULL
GROUP BY table_name
ORDER BY lag_seconds DESC
```

### Is pg2iceberg running?

The Rust port doesn't auto-write `checkpoints` rows yet (see above), so the Go-style "checkpoint heartbeat" query doesn't apply. Use the `commits` table as a heartbeat instead:

```sql
SELECT
    max(ts)                                       AS last_commit_at,
    dateDiff('second', max(ts), now())            AS seconds_since_last_commit
FROM _pg2iceberg.commits
```

A `seconds_since_last_commit` significantly above your write rate warrants investigation. (For idle databases this can be misleading — a long-quiet table won't produce commits even when pg2iceberg is healthy. Pair with the slot's `confirmed_flush_lsn` advancement when in doubt.)

### Replication lag trend (last 24 hours)

Average and maximum end-to-end lag per table per hour.

```sql
SELECT
    toStartOfHour(ts)                                                         AS hour,
    table_name,
    avg(dateDiff('millisecond', max_source_ts, ts)) / 1000                   AS avg_lag_seconds,
    max(dateDiff('millisecond', max_source_ts, ts)) / 1000                   AS max_lag_seconds
FROM _pg2iceberg.commits
WHERE ts >= now() - INTERVAL 24 HOUR
  AND max_source_ts IS NOT NULL
GROUP BY hour, table_name
ORDER BY hour DESC, table_name
```

### Throughput per table (rows per hour)

```sql
SELECT
    toStartOfHour(ts)   AS hour,
    table_name,
    sum(rows)           AS rows_written,
    count()             AS commits
FROM _pg2iceberg.commits
WHERE ts >= now() - INTERVAL 24 HOUR
GROUP BY hour, table_name
ORDER BY hour DESC, table_name
```

### Delete file accumulation since last compaction

Tables with many delete files need compaction. This query shows how many delete files have accumulated since the last compaction run for each table.

```sql
WITH last_compaction AS (
    SELECT
        table_name,
        max(ts) AS last_compacted_at
    FROM _pg2iceberg.compactions
    GROUP BY table_name
)
SELECT
    c.table_name,
    coalesce(lc.last_compacted_at, toDateTime(0))   AS last_compacted_at,
    sum(c.delete_files)                              AS delete_files_accumulated,
    sum(c.data_files)                                AS data_files_written
FROM _pg2iceberg.commits AS c
LEFT JOIN last_compaction AS lc USING (table_name)
WHERE c.ts > coalesce(lc.last_compacted_at, toDateTime(0))
GROUP BY c.table_name, lc.last_compacted_at
ORDER BY delete_files_accumulated DESC
```

### Compaction effectiveness

Bytes saved and delete files removed per compaction run, last 7 days.

```sql
SELECT
    ts,
    table_name,
    input_delete_files,
    input_data_files - output_data_files    AS files_reduced,
    rows_removed,
    bytes_before - bytes_after              AS bytes_saved,
    duration_ms
FROM _pg2iceberg.compactions
WHERE ts >= now() - INTERVAL 7 DAY
ORDER BY ts DESC
```

## Schema evolution

Metadata table schemas are evolved additively — new columns are appended and all additions are nullable. Existing column field IDs are never changed. This means the tables are safe to query across pg2iceberg version upgrades without schema conflicts.
