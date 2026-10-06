---
icon: lucide/zap
---

# Quickstart

## Try it locally

The `example/single` stack runs PostgreSQL with a rideshare workload, MinIO, an Iceberg REST catalog, pg2iceberg and ClickHouse:

```bash
cd example/single
docker compose up -d --wait
docker compose --profile workload up -d workload
```

Open `http://localhost:8123/play` and query the replicated tables:

```sql
SELECT * FROM rideshare.`rideshare.rides`
```

## Run it against your database

The source needs `wal_level = logical`, and a role that can replicate. `pg2iceberg init` checks both.

### 1. Point pg2iceberg at Postgres and the catalog

```bash
export POSTGRES_URL=postgres://user:password@db.example.com:5432/app
export ICEBERG_CATALOG_URL=https://catalog.example.com
export ICEBERG_WAREHOUSE=s3://my-bucket/warehouse/
```

S3 credentials come from the AWS default chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, a profile, or an instance or task role. For MinIO or other S3-compatible storage, also set `AWS_ENDPOINT_URL_S3`. Catalogs that vend credentials (Polaris, R2, Lakekeeper) need no S3 settings — see [Catalogs](../catalogs/overview.md).

### 2. Check the database, and write a config (optional)

```bash
pg2iceberg init
```

This checks what replication needs, and writes `pg2iceberg.yaml` with every table pg2iceberg can replicate — edit it to choose tables, or add partitioning. Without it, pg2iceberg replicates every table with a primary key.

### 3. Run

```bash
pg2iceberg run
```

pg2iceberg takes an initial snapshot of each table, then streams changes as they commit. Each table lands in an Iceberg namespace named after its Postgres schema (`ICEBERG_NAMESPACE` puts them all in one), queryable from any engine that reads the catalog.

## Next steps

- [Configuration](../usage/configuration.md) — every setting, and how pg2iceberg infers the ones you leave out
- [Modes](../usage/modes.md) — distributed mode, one-shot snapshot, compaction and maintenance
