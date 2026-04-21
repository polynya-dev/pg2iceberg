---
icon: lucide/sandwich
---

# TOAST Handling

PostgreSQL uses [TOAST](https://www.postgresql.org/docs/current/storage-toast.html) (The Oversized-Attribute Storage Technique) to store large column values out-of-line. During logical replication, unchanged TOAST columns in `UPDATE` events are omitted from the WAL stream — PostgreSQL sends a marker indicating the value was not modified rather than retransmitting the full value. pg2iceberg detects these markers, preserves them through the staging layer, and resolves the missing values during materialization.

## Detection

When the flusher decodes a WAL `UPDATE` message, each column carries a type marker:

| Marker | Meaning |
|--------|---------|
| `t` | Text value — column data follows |
| `b` | Binary value — column data follows |
| `n` | Null |
| `u` | Unchanged — value omitted (TOAST) |

Columns marked `u` have their names recorded in an `unchangedCols` list. Their values are set to `nil` in the row map.

!!! note "REPLICA IDENTITY"
    Unchanged TOAST column markers only appear with the default replica identity (primary-key-based). With `REPLICA IDENTITY FULL`, PostgreSQL always sends the full row, so no TOAST resolution is needed.

## Staging

The flusher serializes the `unchangedCols` list into the `_unchanged_cols` column of the staged Parquet file as a comma-separated string:

```
_op  │ _lsn │ _ts │ _unchanged_cols    │ _data
──────┼──────┼─────┼────────────────────┼────────────────────────
 U   │ 1234 │ ... │ body,attachment     │ {"id":42,"subject":"…"}
```

The staging schema is fixed regardless of how many columns are unresolved — the materialized Iceberg table is unaffected at this stage.

## Resolution

During materialization, after events are folded by primary key, the materializer identifies all `UPDATE` rows that still have unresolved TOAST columns. It then:

1. **Looks up affected files** — uses the FileIndex (a PK → data file map built from the table's current Iceberg snapshot) to find which existing data files contain each affected primary key.
2. **Downloads and scans** — downloads only the affected Parquet files from S3 and reads the relevant column values.
3. **Backfills** — copies each missing column value from the scanned file into the pending row, then clears `unchangedCols`.

If the FileIndex has no entry for a primary key — for example on the very first materialization after a cold start — the unresolved columns remain `nil` in the output.

## FileIndex

The FileIndex is a PK → S3 file path mapping maintained in the MetadataCache. It is seeded once from the table's live Iceberg snapshot and updated incrementally after each successful commit, so full rebuilds are rare. TOAST resolution uses `AffectedFiles(pks)` to minimise the number of files downloaded — only files that actually contain one of the unresolved primary keys are fetched.

## Observability

TOAST resolution time is reported separately in the materializer log line:

```
[materializer] materialized 1000 events: drain=2ms fold=1ms toast=15ms
```

The `toast=` component covers FileIndex lookups, S3 downloads, and backfill. A large value indicates many cross-flush updates to wide TOAST columns.

TOAST resolution runs inside the `pg2iceberg.materialize.table` span (part of the `pg2iceberg/materializer` tracer). S3 downloads triggered by resolution are traced under `pg2iceberg/s3`. Errors at any step — URI parsing, S3 download, Parquet read — are recorded on the span via `RecordError` and will appear in your trace backend.
