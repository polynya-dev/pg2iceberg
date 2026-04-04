# pg2iceberg

pg2iceberg replicates data from Postgres directly to Iceberg, no Kafka needed. It has opinionated design:
- It's specifically designed to replicate data from Postgres, to Iceberg, nothing else.
- It assumes pg2iceberg is the sole writer of the Iceberg tables, which includes compaction.

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  OLAP[Snowflake<br />ClickHouse<br />etc.] -- Query --> ICE
```

## How it works

pg2iceberg can operate on query mode or logical replication mode.

### Logical replication mode

```mermaid
graph LR
  subgraph Postgres
      TableA["Table A"]
      TableB["Table B"]
  end

  subgraph Iceberg
      WAL["Change Events (WAL)"] -->|Materializer| TargetA[Table A]
      WAL["Change Events (WAL)"] -->|Materializer| TargetB[Table B]
  end

  TableA -->|Logical Replication| WAL
  TableB -->|Logical Replication| WAL
```

On logical replication mode (the recommended mode), it replicates change events to an append-only Iceberg table, which acts as a WAL. Once change events are written to this table, the replication slot LSN can be safely advanced. Since append-only write to Iceberg is fast, this minimizes the likelihood of the source database retaining too much WAL.

A materializer, which runs at a separate interval, will then take these change events and merge them into the target tables, which will have the same schema as the source tables. If you don't need near real-time replication, just set the materializer interval to something high (e.g. 1 hour), which will essentially make pg2iceberg behave like a batch replication tool.

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

## Usage

Runs one pipeline from a config file:

```sh
docker run -v ./config.yaml:/etc/pg2iceberg/config.yaml \
  ghcr.io/pg2iceberg/pg2iceberg --config /etc/pg2iceberg/config.yaml
```

See [`example/single`](example/single) for a full working example.

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
