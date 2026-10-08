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
5. **Commit** — at a transaction boundary, commit every step prepared so far as **one atomic catalog update**: each step becomes its own snapshot, but the table's `main` branch moves once. The last snapshot's summary records the log range the commit applied (`pg2iceberg.log-group`, `pg2iceberg.log-start`, `pg2iceberg.log-end`), and how far each cursor group has applied the log (`pg2iceberg.log-ends`)
6. **Advance cursor** — only after the commit succeeds; then continue with the next entries until the log is drained or the cycle has read its row budget

### Applying each change once

The commit and the cursor update are two writes to two systems, so one can happen without the other: the process dies in between, or the catalog applies a commit but its response is lost. Folding the same log entries again isn't harmless — a primary-key change that left a TOASTed column unchanged resolves the value from the row's old key, which the first commit moved or reused.

So the materializer never trusts the cursor alone. Before folding a unit, it compares the unit's start with the log end the table's snapshots record for its cursor group; if a commit already applied past it, it moves the cursor there and goes on from the end. A commit that reports failure but did land is recognized the same way and treated as committed.

Snapshot expiry doesn't lose that record: every snapshot pg2iceberg commits — compactions included, inline or by `pg2iceberg compact` — carries the ends forward, and the current snapshot is never expired. Only a snapshot another engine writes (a Spark rewrite, say) doesn't carry them; its predecessors still do until they expire.

### Large transactions

Steps bound the materializer's memory, but a source transaction is never split across commits. When a transaction spans several steps, they're committed together: each step is a snapshot with the next sequence number, so a later step's equality deletes hide rows an earlier step wrote, and readers of the table see either none of the transaction or all of it.

Atomicity is per table. A transaction touching several tables is committed table by table, since Iceberg has no multi-table commit yet.

### Catalog cache

Each catalog call is a round trip, often to a catalog and object store across the internet, and a cycle makes several per table: loading its metadata, reading its snapshot history for the compaction check. pg2iceberg is its tables' only writer, so the materializer keeps what it read until a pg2iceberg process writes the table — an idle cycle asks the catalog nothing.

Every pg2iceberg process that writes a table's Iceberg metadata — `run`, the workers of a distributed deployment, `compact` and `maintain` jobs — bumps the table's epoch in the coordinator's `table_epoch`. At the start of each cycle the materializer reads the epochs and forgets the tables another process wrote; its own commits update what it holds, since a commit answers with the table's new metadata. A write that fails may have landed, so its table is read again.

A writer that dies between its commit and the bump leaves the epoch behind: long-running processes keep nothing longer than a minute, well inside `maintenance_grace`. Orphan cleanup, which deletes what the table doesn't reference, always reads the catalog.

A catalog that maintains the tables itself (`sink.maintenance: managed`) commits to them without bumping any epoch. Then the materializer forgets what it read at the start of every cycle, and rereads the tables it works on.

## Merge-on-read

pg2iceberg uses Iceberg's **merge-on-read** write mode. Rather than rewriting existing data files on every update or delete, it appends:

- New data files for inserts and updates
- Equality delete files for deletes and updates-as-deletes (keyed by primary key)

This keeps the write path fast. Query engines apply the equality deletes at read time. Over time, accumulated delete files increase read amplification — which is why [compaction](compaction.md) exists.
