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

## Staged log

The staged-log primitive provides offset-ordered, durable delivery of WAL events from the flusher to one or more materializer workers without a dedicated leader process.

The coordinator role is filled by the **source PostgreSQL database** itself. This is a deliberate choice: the coordinator is technically a single point of failure, but if the source database goes down, replication cannot happen regardless — so there is no additional availability cost to colocating coordination there. It also means pg2iceberg needs no extra infrastructure beyond the database you are already replicating from. (Operators who want isolation can point `state.postgres_url` at a separate Postgres.)

**Coordination tables** (in `_pg2iceberg`):

| Table | Purpose |
|-------|---------|
| `log_seq` | Per-table atomic offset counter |
| `log_index` | Sparse index: maps offset ranges to staged-Parquet S3 paths + LSNs |
| `mat_cursor` | Per-`(group, table)` cursor tracking the last materialized offset |
| `consumer` | Heartbeat registry for distributed materializer workers |
| `pipeline_meta` | Singleton: source-cluster `system_identifier` |
| `flushed_lsn` | Singleton: highest LSN we've acked the slot to |
| `tables` | Per-table snapshot status + `pg_class.oid` |
| `snapshot_progress` | Per-table mid-snapshot resume cursor |
| `query_watermarks` | Per-table query-mode cursor |
| `pending_markers` / `marker_emissions` | Blue-green marker bookkeeping |

**Append** (flusher → log):

1. Upload staged Parquet files to S3 in parallel
2. In a single PostgreSQL transaction: increment `log_seq` by the batch size, insert per-claim rows into `log_index` with the assigned offset range, S3 path, and `flushable_lsn`, and persist any blue-green markers

Step 2 is atomic — a flush is either fully registered or not registered at all. Orphaned S3 files from failed step 2s are collected by [table maintenance](maintenance.md).

**Read** (materializer ← log):

The materializer reads `log_index` entries with offsets greater than its cursor, downloads the referenced files, decodes them via the staging codec, and processes events in offset order. There is no in-memory cache optimization for the single-process case; profiling showed the S3-fetch overhead is amortized well below the materializer cycle interval.

## Restart record

Replication state is split across several narrow tables — no single "checkpoint" blob. Each concern that needs durability gets its own row, so updates from different code paths don't contend on a shared OCC token:

- **Confirmed flush LSN** — recorded in `_pg2iceberg.flushed_lsn` *before* every standby ack to the slot. This durable record is compared to the slot's `confirmed_flush_lsn` at startup to catch external slot tampering (`pg_replication_slot_advance`, drop+recreate, stray `pg_recvlogical`).
- **Per-table snapshot status** — `_pg2iceberg.tables` rows store `snapshot_complete`, `pg_oid`, and `snapshot_lsn` per table. The `pg_oid` field drives the `TableIdentityChanged` invariant (DROP+recreate detection).
- **Per-table mid-snapshot resume** — `_pg2iceberg.snapshot_progress` stores the canonical-PK cursor of the last successfully staged row. Cleared on completion.
- **Cluster fingerprint** — singleton row in `_pg2iceberg.pipeline_meta` stores `IDENTIFY_SYSTEM`'s `system_identifier`. Stamped at first run; subsequent runs against a different cluster fail with `SystemIdMismatch`.

On startup, pg2iceberg cross-references all four against PG and the slot, and refuses to start on any mismatch. See [validate_startup](https://github.com/polynya-dev/pg2iceberg/blob/main/pg2iceberg-rust/crates/pg2iceberg-validate/src/lib.rs) for the full invariant list.

!!! note "Separate coord storage"
    By default coord state is stored in the source PostgreSQL database. If you want to keep them separate — for example, to avoid writes on a read replica or to share state across pipelines — point `state.postgres_url` at a different database. Coord state is always Postgres-backed; there is no file-based store option.
