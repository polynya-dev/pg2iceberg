---
icon: lucide/layers
---

# Operational Modes

pg2iceberg's CLI is organized as subcommands. Each one is a separate entry point that takes the same `--config` YAML.

```sh
pg2iceberg <SUBCOMMAND> --config /etc/pg2iceberg/config.yaml [flags...]
```

## `run` — single-process default

Both the WAL writer and materializer run in the same process. This is the right choice for most deployments.

```bash
pg2iceberg run --config config.yaml
```

```
┌─────────────────────────────────────────┐
│             pg2iceberg run              │
│                                         │
│  WAL writer ──► stream ──► materializer │
└─────────────────────────────────────────┘
```

The WAL writer captures row-level changes via PostgreSQL logical replication and stages them to S3 + the `_pg2iceberg.log_index` table. The materializer reads from that staging layer and writes Iceberg data files via the catalog.

`run` dispatches on `source.mode` in the YAML — `logical` (default) drives the CDC pipeline; `query` drives the watermark-poll pipeline.

## `stream-only` + `materializer-only` — distributed

One process owns the replication slot; N worker processes claim a deterministic round-robin slice of tables.

**WAL writer** (one instance only — Postgres allows one consumer per slot):

```bash
pg2iceberg stream-only --config config.yaml
```

**Materializer workers** (one or more, each with a process-unique id):

```bash
pg2iceberg materializer-only --config config.yaml --worker-id worker-a
pg2iceberg materializer-only --config config.yaml --worker-id worker-b
```

```
┌──────────────────┐     ┌──────────────────────┐
│  stream-only     │     │ materializer-only A  │
│                  │──►  ├──────────────────────┤
│                  │     │ materializer-only B  │
└──────────────────┘     └──────────────────────┘
         │                          │
   `_pg2iceberg`              Iceberg catalog
   coordinator
```

Each worker registers a heartbeat in `_pg2iceberg.consumer` keyed by `state.group` and `--worker-id`. On every cycle, every worker reads the active-worker list and computes the same deterministic assignment (sorted tables → sorted workers → `[i % N]`). Adding or removing a worker rebalances on the next cycle automatically; no leader election.

!!! note
    All workers must share the same `state.group` (default `default`) and the same configured table list. Different `--worker-id` per process is mandatory — duplicates trample each other's heartbeat row and produce undefined assignment.

## `snapshot` — one-shot initial snapshot

Runs the initial snapshot phase for every configured table and exits. Auto-creates the replication slot first so a later `run` doesn't lose WAL between snapshot completion and CDC start.

```bash
pg2iceberg snapshot --config config.yaml
```

If every configured table is already marked complete in `_pg2iceberg.tables`, the command prints `OK: snapshot already complete` and exits without touching PG or the catalog.

## `compact` — one-shot compaction pass

Runs a single compaction pass across every configured table, then exits. Designed for cron / Kubernetes `CronJob` deployments that want compaction on a slower cadence than the materializer.

```bash
pg2iceberg compact --config config.yaml
```

Skipped for tables below the configured `compaction_data_files` / `compaction_delete_files` thresholds. Set `target_file_size: 0` in YAML to disable compaction entirely.

## `maintain` — one-shot maintenance pass

Runs snapshot expiry first, then orphan-file cleanup, across every configured table. Both phases can be turned off independently by leaving their config field blank.

```bash
pg2iceberg maintain --config config.yaml
pg2iceberg maintain --config config.yaml --retention 168h   # CLI override
```

| Phase | Driven by | Behavior |
|---|---|---|
| Snapshot expiry | `sink.maintenance_retention` | Drops snapshots older than retention. Never drops the current snapshot. |
| Orphan-file cleanup | `sink.maintenance_grace` | Deletes S3 files older than grace that no live snapshot references. |

## `verify` — one-shot diff against PG

Reads PG ground truth (REPEATABLE READ snapshot) and Iceberg materialized state for every configured table, compares row-by-row by primary key, prints per-table diff counts. Exits non-zero if any diff is non-empty.

```bash
pg2iceberg verify --config config.yaml
pg2iceberg verify --config config.yaml --chunk-size 4096
```

## `cleanup` — drop PG-side state

Drops the replication slot, drops the publication, and `DROP SCHEMA … CASCADE` on the coordinator. Does **not** delete the materialized Iceberg tables — drop those out-of-band via the catalog if you want a full reset.

```bash
pg2iceberg cleanup --config config.yaml
```

!!! danger
    This is irreversible. After cleanup, the next `pg2iceberg run` will create a fresh slot and re-snapshot every table.

## `migrate-coord` — idempotent schema migration

Runs the coordinator schema migration. Every statement is `CREATE TABLE IF NOT EXISTS` (or `ALTER TABLE … ADD COLUMN IF NOT EXISTS`), so it's safe to run repeatedly.

```bash
pg2iceberg migrate-coord --config config.yaml
```

`run` calls this automatically at startup, so explicit invocation is only needed if you want to inspect the schema before the long-running pipeline starts.

## `connect-pg` / `connect-iceberg` — connectivity smoke tests

Open a connection in the same prod path the lifecycle uses, run a trivial probe, and exit. Useful for k8s `initContainer` hooks and on-call sanity checks.

```bash
pg2iceberg connect-pg --config config.yaml
pg2iceberg connect-iceberg --config config.yaml
```
