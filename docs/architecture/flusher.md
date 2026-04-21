---
icon: lucide/send
---

# Flusher

The flusher connects to PostgreSQL via a logical replication slot (using the `pgoutput` plugin) and consumes WAL messages as they are produced.

## WAL capture

For each transaction, the flusher:

1. Buffers all row change events (`INSERT`, `UPDATE`, `DELETE`) until the transaction `COMMIT`
2. Serializes each table's buffered events into a Parquet file with a fixed staging schema:

    | Column | Type | Description |
    |--------|------|-------------|
    | `_op` | `string` | Operation: `I`, `U`, or `D` |
    | `_lsn` | `int64` | Source WAL LSN |
    | `_ts` | `timestamptz` | PostgreSQL commit timestamp |
    | `_xid` | `int64` | PostgreSQL transaction ID |
    | `_unchanged_cols` | `string` | Comma-separated unresolved TOAST columns |
    | `_data` | `string` | JSON-encoded row columns |

    The staging schema is fixed regardless of the source table structure. Schema evolution only affects the materialized Iceberg table, not the staged files.

3. Uploads the Parquet files to S3 and atomically registers them in the leaderless log

The replication slot's confirmed flush LSN is advanced immediately after staging — once events are durably registered in S3 + `log_index`, PostgreSQL can recycle WAL up to that point. On a crash between staging and materialization, pg2iceberg recovers by re-reading the staged Parquet files from the log; the WAL is no longer needed.

## Leaderless log protocol

The leaderless log implements the [leaderless log protocol](https://github.com/lakestream-io/leaderless-log-protocol), providing offset-ordered, durable delivery of staged events to one or more materializer workers, without a dedicated leader process.

The protocol's coordinator role is filled by the **source PostgreSQL database** itself. This is a deliberate choice: the coordinator is technically a single point of failure, but if the source database goes down, replication cannot happen regardless — so there is no additional availability cost to colocating coordination there. It also means pg2iceberg needs no extra infrastructure beyond the database you are already replicating from.

**Coordination tables** (in the `_pg2iceberg` schema):

| Table | Purpose |
|-------|---------|
| `log_seq` | Per-table atomic offset counter |
| `log_index` | Sparse index: maps offset ranges to S3 file paths |
| `mat_cursor` | Per-worker cursor tracking the last materialized offset |
| `lock` | Heartbeat-based table locks for distributed workers |
| `checkpoints` | Replication LSN and snapshot progress |

**Append** (flusher → log):

1. Upload staged Parquet files to S3 in parallel
2. In a single PostgreSQL transaction: increment `log_seq` by the batch size and insert a row into `log_index` with the assigned offset range and S3 path

Step 2 is atomic — a file is either fully registered or not registered at all. Orphaned S3 files from failed step 2s are collected by [table maintenance](maintenance.md).

**Read** (materializer ← log):

The materializer reads `log_index` entries with offsets greater than its cursor, downloads the referenced files, and processes events in offset order. In combined mode, events are served from an in-memory cache populated during the flush, bypassing S3 entirely.

## Checkpointing

The checkpoint is pg2iceberg's durable restart record. It is stored in the `_pg2iceberg.checkpoints` table by default, and tracks:

- **Confirmed flush LSN** — the WAL position confirmed staged to S3. PostgreSQL will not recycle WAL before this point. On restart, pg2iceberg resumes replication from this LSN; any staged-but-not-yet-materialized events are recovered from the leaderless log.
- **Snapshot state** — whether the initial table snapshot has completed, and per-table chunk progress so an interrupted snapshot can be resumed rather than restarted.

On startup, pg2iceberg loads the checkpoint and resumes replication from the confirmed flush LSN. The replication slot ensures no WAL is lost between that LSN and the present.

!!! note "Separate checkpoint storage"
    By default checkpoints are stored in the source PostgreSQL database. If you want to keep them separate — for example, to avoid writes on a read replica or to share state across pipelines — point `state.postgres_url` at a different database, or use a local file with `state.path`.
