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

## Dropped columns

A column dropped in PostgreSQL stays in the Iceberg table — older data files hold its values — renamed to `<name>__dropped_<field id>` and made nullable. Its name is then free: a column the source adds with that name later is a new column, with a new field ID and no values, just as PostgreSQL gives a re-added column none. (Kept under its old name, the re-added column would read the dropped one's values.)

The rename is what records the drop, in the table's schema, so every process that loads the table — a restarted materializer, another distributed worker — knows which columns are dropped.

pgoutput reports a schema change only with the table's next change. A drop shows then. A column dropped and re-added with no change in between shows only by order: PostgreSQL appends a re-added column after every other, so a column that comes before one it used to follow was re-added. One re-added while it was already the last column, with no change between, can't be told from one never dropped.

## Columns added with a default

`ALTER TABLE … ADD COLUMN … DEFAULT <value>` writes nothing to the WAL for the rows already in the table. PostgreSQL stores the value once, in its catalog (`pg_attribute.attmissingval`), and rows that predate the column read it from there.

pg2iceberg reads it from there too. When it stages the schema change, it reads the table's column defaults from the catalog. The materializer then fills the stored value into the rows the Iceberg table holds, rewriting them as PostgreSQL rewrites a table for a volatile default. The fill commits together with the transaction that carried the change, so readers see the new column filled in all at once.

The table property `pg2iceberg.fill.<field id>` makes the fill happen exactly once, across crashes and restarts. It's set together with the new column, and the fill's commit removes it.

Limitations:

- **Volatile defaults** (`clock_timestamp()`, `random()`, `gen_random_uuid()`) make PostgreSQL rewrite the table, giving each row its own value, again with no WAL. Nothing records those values, so rows already in Iceberg keep NULL. pg2iceberg logs a warning and increments `pg2iceberg_unfilled_column_defaults_total{table, column}`.
- **Table rewrites** have the same effect if they happen before pg2iceberg reads the default: `VACUUM FULL`, `CLUSTER` and column type changes all clear the stored value.
- **False warnings:** a default set after the column was added (`ALTER COLUMN … SET DEFAULT`) also triggers the warning, but there the rows really are NULL in PostgreSQL too.
- **Workaround:** to bring the values over in any of these cases, touch the rows in PostgreSQL (`UPDATE t SET c = c`). That writes them to the WAL.
- **Timing:** the default is read when pg2iceberg stages the change, which happens with the table's next change (pgoutput reports schema changes no sooner), not at the `ALTER`. A column dropped by then has lost its stored value, so the rows that predate it keep NULL under the dropped column's name.
- **Cost:** a fill rewrites every row of the table, as equality deletes plus new data files, so on a large table it is one large commit. Compaction folds it back down.

## Type changes

Not every PostgreSQL type change produces an Iceberg schema change. Some aliases (`integer` → `int4`) normalise to the same Iceberg type and are transparent. Only changes that produce a different Iceberg type (e.g. `varchar(100)` → `text`, or `numeric(10,2)` → `numeric(20,4)`) result in a catalog update.
