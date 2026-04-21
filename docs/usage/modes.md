---
icon: lucide/layers
---

# Operational Modes

pg2iceberg can run in several modes depending on your scale and operational needs.

## Single mode (default)

Both the WAL writer and materializer run in the same process. This is the right choice for the vast majority of deployments.

```bash
pg2iceberg --config config.yaml
```

```
┌─────────────────────────────────────────┐
│              pg2iceberg                 │
│                                         │
│  WAL writer ──► stream ──► materializer │
└─────────────────────────────────────────┘
```

The WAL writer captures row-level changes from PostgreSQL via logical replication and stages them to the stream (S3 + PostgreSQL coordinator). The materializer reads from the stream and writes Iceberg data files.

## Multi-worker mode

One pg2iceberg process acts as the WAL writer; N separate processes run materializers. This is useful for databases with very large tables or high write throughput, where materializer work can be parallelised across multiple workers.

**WAL writer** (one instance):

```bash
pg2iceberg --config config.yaml --stream-only
```

**Materializer workers** (one or more instances, each with a unique ID):

```bash
pg2iceberg --config config.yaml --materializer-only --materializer-worker-id worker-1
pg2iceberg --config config.yaml --materializer-only --materializer-worker-id worker-2
```

```
┌──────────────────┐     ┌──────────────────────┐
│  WAL writer      │     │  materializer worker-1│
│  (stream-only)   │──►  ├──────────────────────┤
│                  │     │  materializer worker-2│
└──────────────────┘     └──────────────────────┘
         │                         │
      stream                    Iceberg
```

Each materializer worker independently reads from the shared stream and writes to Iceberg. Workers are differentiated by their `--materializer-worker-id` value, which is also used as the consumer group cursor in the coordinator.

!!! note
    All materializer workers must share the same configuration (catalog, namespace, tables). The WAL writer does not need `--materializer-worker-id`.

## Snapshot-only mode

Performs a one-shot initial snapshot of all configured tables and exits. The replication pipeline is not started — no slot, no WAL streaming.

```bash
pg2iceberg --config config.yaml --snapshot-only
```

pg2iceberg writes the snapshot to Iceberg and records completion in the checkpoint store. If the snapshot was already completed on a previous run, it exits immediately without doing any work.

## Compact mode

Runs one compaction cycle across all configured tables and exits. Compaction merges small data and delete files into larger files, improving query performance.

```bash
pg2iceberg --config config.yaml --compact
```

Compaction is skipped for any table that is below the configured `data_file_threshold` and `delete_file_threshold`.

!!! warning "Known limitation"
    Compaction currently writes unpartitioned output for partitioned tables. This is a known issue and will be addressed in a future release.

## Maintain mode

Runs one maintenance cycle and exits. Maintenance covers two operations:

- **Snapshot expiry** — removes old Iceberg snapshots beyond the configured retention period
- **Orphan file cleanup** — deletes S3 files that are no longer referenced by any snapshot

```bash
pg2iceberg --config config.yaml --maintain
```

!!! note
    Maintenance requires a `MetadataStore` catalog (Iceberg REST). It is not supported with plain S3 catalog backends.

## Cleanup mode

Drops all pg2iceberg state from PostgreSQL and exits. This removes the replication slot, the publication, and the `_pg2iceberg` schema.

```bash
pg2iceberg --config config.yaml --cleanup
```

!!! danger
    This is irreversible. After cleanup, pg2iceberg must perform a fresh snapshot before streaming can resume. The Iceberg tables themselves are not modified.
