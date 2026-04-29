---
icon: lucide/git-branch
---

# Schema Evolution

pg2iceberg detects `ALTER TABLE` changes in the WAL stream and evolves the Iceberg table schema automatically, with no manual intervention and no replication downtime.

## How it works

When the WAL decoder encounters a schema change event, the pipeline:

1. **Flushes in-flight rows first** — any buffered rows with the old schema are staged and registered in the log before the schema changes. This prevents mixed-schema rows from landing in the same Iceberg write.

2. **Updates the in-memory source schema** — column additions, drops, and type changes are applied to the tracked `TableSchema`.

3. **Evolves the Iceberg table** — if the change maps to a different Iceberg type, the catalog is updated via a schema-version increment with an optimistic lock on the current schema ID:

    ```json
    {
      "requirements": [{ "type": "assert-current-schema-id", "current-schema-id": 3 }],
      "updates": [
        { "action": "add-schema", "schema": { ..., "schema-id": 4 } },
        { "action": "set-current-schema", "schema-id": -1 }
      ]
    }
    ```

4. **Syncs the materializer's table writer** — the writer is updated to use the new Iceberg schema ID for all subsequent commits.

## Staging schema is unaffected

The staged Parquet files use a fixed schema regardless of the source table structure — user columns are always stored as JSON in the `_data` field. Schema evolution only affects how the materializer reads and writes to the Iceberg table, not how events are staged.

## Column field IDs

Each column is assigned a monotonically increasing field ID that is never reused, even after a column is dropped. This is the Iceberg convention for safe schema evolution: dropped columns can be re-added later without ambiguity, and query engines can distinguish a missing column from a newly added one.

## Type changes

Not every PostgreSQL type change produces an Iceberg schema change. Some aliases (`integer` → `int4`) normalise to the same Iceberg type and are transparent. Only changes that produce a different Iceberg type (e.g. `varchar(100)` → `text`, or `numeric(10,2)` → `numeric(20,4)`) result in a catalog update.
