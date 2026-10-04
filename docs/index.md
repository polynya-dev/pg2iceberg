---
icon: lucide/home
---

# pg2iceberg

Replicate PostgreSQL tables to Apache Iceberg in real time using logical replication.

## What is pg2iceberg?

pg2iceberg captures row-level changes from PostgreSQL via logical replication — full CDC through PostgreSQL's `pgoutput` plugin, including deletes and schema changes — and writes them to Apache Iceberg tables. An initial snapshot seeds your Iceberg tables before streaming begins.

## Key features

- **Apache Iceberg** — write directly to Iceberg REST, Polaris, or S3-compatible catalogs
- **Iceberg partitioning** — materialize into partitioned Iceberg tables using time bucketing, identity, or custom partition transforms
- **PostgreSQL partitioned tables** — native support for partitioned source tables via `publish_via_partition_root`
- **At-least-once delivery** — checkpointed LSN ensures no data loss on restart; duplicates are deduplicated by primary key during materialization
- **Schema evolution** — `ALTER TABLE` changes are detected and applied to the Iceberg schema automatically

## Quick links

- [Installation](getting-started/installation.md)
- [Quickstart](getting-started/quickstart.md)
- [Configuration reference](usage/configuration.md)
