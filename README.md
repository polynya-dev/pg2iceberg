# pg2iceberg

pg2iceberg replicates data from Postgres directly to Iceberg.

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  OLAP[Snowflake<br />ClickHouse<br />etc.] -- Query --> ICE
```

## Quickstart

```sh
cd example/single
docker compose up -d --wait
```

Then go to http://localhost:8123/play and run:

```sql
select * from rideshare.`rideshare.rides`
```

You should see new rows added over time.

## Single vs Multitenant mode

pg2iceberg has two modes of operation:

### Single mode

Runs one pipeline from a config file. This is the simplest way to replicate a single Postgres database into Iceberg.

```sh
docker run -v ./config.yaml:/etc/pg2iceberg/config.yaml \
  ghcr.io/pg2iceberg/pg2iceberg --config /etc/pg2iceberg/config.yaml
```

See [`example/single`](example/single) for a full working example.

### Multitenant (server) mode

Runs an HTTP API server that manages multiple pipelines. Pipelines are created, listed, and deleted via REST API.

```sh
docker run -p 8080:8080 ghcr.io/pg2iceberg/pg2iceberg \
  --server \
  --listen=:8080 \
  --store-url="postgresql://postgres:postgres@mydb:5432/pg2iceberg?sslmode=disable"
```

The web UI is a separate container that proxies API requests to the server:

```sh
docker run -p 3000:80 ghcr.io/pg2iceberg/pg2iceberg-ui
```

Then open http://localhost:3000 to manage pipelines.

See [`example/multitenant`](example/multitenant) for a full working example with multiple Postgres sources.

## Checkpoint storage

pg2iceberg tracks replication progress (LSN for logical replication, watermark for query mode) in a checkpoint. By default, checkpoints are stored in the source Postgres database under the `_pg2iceberg` schema:

```sql
_pg2iceberg.checkpoints
```

This means no extra infrastructure or persistent volumes are needed. If the container restarts, it resumes from where it left off.

To use a separate Postgres instead of the source database, set `state.postgres_url` in the pipeline config:

```yaml
state:
  postgres_url: postgresql://user:pass@host:5432/db?sslmode=disable
```

For local development, a file-based store is also available:

```yaml
state:
  path: ./pg2iceberg-state.json
```

## Running tests

Start dependencies:

```sh
docker compose up -d --wait
```

To run all tests:

```sh
./tests/run.sh
```

To run specific test:

```sh
./tests/run.sh 00001_basic_insert
```

### Writing tests

Test cases live in `tests/cases/` with three files per test:

| File | Purpose |
|------|---------|
| `<name>__input.sql` | SQL executed against PostgreSQL |
| `<name>__query.sql` | Query run on ClickHouse to verify results |
| `<name>__reference.tsv` | Expected tab-separated output from ClickHouse |

Input SQL is split into steps using markers:

```sql
-- SETUP --     DDL phase: runs before pg2iceberg starts
-- DATA --      DML phase: runs after pg2iceberg connects to replication
-- SLEEP <N> -- pause for N seconds (useful between DDL and DML batches)
```

The table name, publication, and replication slot are auto-derived from the SQL.

## FAQ

### Will it support other sources and sinks in the future?

No. As its name suggests, it's specifically designed to replicate data from Postgres to Iceberg.
