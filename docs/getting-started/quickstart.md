---
icon: lucide/zap
---

# Quickstart

This guide replicates a PostgreSQL table to a local Iceberg REST catalog using Docker Compose.

## 1. Start the stack

```bash
docker compose up -d
```

This starts PostgreSQL, MinIO, and an Iceberg REST catalog.

## 2. Configure pg2iceberg

Create a `config.yaml` pointing at your Postgres instance and Iceberg catalog:

```yaml
source:
  dsn: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
  publication: pg2iceberg
  slot: pg2iceberg

catalog:
  type: rest
  uri: http://localhost:8181

storage:
  type: s3
  endpoint: http://localhost:9000
  bucket: warehouse
  access_key_id: minioadmin
  secret_access_key: minioadmin

tables:
  - name: public.orders
    namespace: default
```

## 3. Run

```bash
pg2iceberg --config config.yaml
```

pg2iceberg will take an initial snapshot of `public.orders` and then stream ongoing changes. You can query the results immediately with any Iceberg-compatible engine:

```sql
-- DuckDB
SELECT * FROM iceberg_scan('s3://warehouse/default/orders');
```

## Next steps

- [Configuration reference](../usage/configuration.md) — tune snapshot batch size, parallelism, and more
- [Reference](../usage/reference.md) — CLI flags, metrics, and troubleshooting
