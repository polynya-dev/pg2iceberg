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
      WALA["Change Events"] -->|Materializer| TargetA[Table A]
      WALB["Change Events"] -->|Materializer| TargetB[Table B]
  end

  TableA -->|Logical Replication| WALA
  TableB -->|Logical Replication| WALB
```

On logical replication mode (the recommended mode), it replicates change events to an append-only Iceberg table, which acts as a WAL. Once change events are written to this table, the replication slot LSN can be safely advanced. Since append-only write to Iceberg is fast, this minimizes the likelihood of the source database retaining too much WAL.

A materializer, which runs at a separate interval, will then take these change events and merge them into its corresponding target tables, which will have the same schema as the source tables. If you don't need near real-time replication, just set the materializer interval to something high (e.g. 1 hour), which will essentially make pg2iceberg behave like a batch replication tool.
∏
### Query mode

```mermaid
graph LR
  subgraph Postgres
      TableA["Table A"]
      TableB["Table B"]
  end

  subgraph Iceberg
      TargetA[Table A]
      TargetB[Table B]
  end

  TableA -->|"SELECT WHERE watermark > $1"| TargetA
  TableB -->|"SELECT WHERE watermark > $1"| TargetB
```

On query mode, pg2iceberg polls Postgres using watermark-based SELECT queries and writes directly to the materialized Iceberg tables. Each row is an upsert (equality delete + insert) keyed by primary key.

Query mode is simpler but cannot detect hard deletes and has no transaction semantics. Use logical mode when you need full CDC fidelity.

## Code structure

```
pg2iceberg/
├── cmd/pg2iceberg/  # entry point, mode dispatch
├── config/          # YAML config parsing & validation
├── iceberg/         # shared Iceberg primitives (catalog, S3, Parquet, manifest, TableWriter)
├── logical/         # logical replication mode (WAL capture, events table, materializer)
├── pipeline/        # shared infrastructure (Pipeline interface, checkpoint, metrics)
├── postgres/        # shared PG types (TableSchema, ChangeEvent, Op)
├── query/           # query polling mode (watermark poller, PK buffer, pipeline)
└── utils/           # retry helper, task pool
```

Both modes share `iceberg.TableWriter` for the final write path (partition bucketing, Parquet serialization, S3 upload, manifest assembly, catalog commit). Logical mode adds a two-tier architecture (events table + materializer) on top, query mode calls the TableWriter directly.

## Type mapping

pg2iceberg maps PostgreSQL column types to Iceberg types automatically during schema discovery. Aliases (e.g. `integer`, `serial`) are normalized to their canonical form.

| PostgreSQL type | Iceberg type | Notes |
|---|---|---|
| `smallint` | `int` | |
| `integer`, `serial`, `oid` | `int` | |
| `bigint`, `bigserial` | `long` | |
| `real` | `float` | |
| `double precision` | `double` | |
| `numeric(p,s)` where p ≤ 38 | `decimal(p,s)` | Precision preserved exactly |
| `numeric(p,s)` where p > 38 | — | **Pipeline refuses to start** (see below) |
| `numeric` (unconstrained) | `decimal(38,18)` | Warning logged; values that overflow will error |
| `boolean` | `boolean` | |
| `text`, `varchar`, `char`, `name` | `string` | |
| `bytea` | `binary` | |
| `date` | `date` | |
| `time`, `timetz` | `time` | Microsecond precision |
| `timestamp` | `timestamp` | Microsecond precision |
| `timestamptz` | `timestamptz` | Microsecond precision |
| `uuid` | `uuid` | |
| `json`, `jsonb` | `string` | |
| Other (`inet`, `interval`, `xml`, ...) | `string` | Stored as text |

Support for `geometry` and `geography` types will be added soon!

### Decimal precision limit

Iceberg supports a maximum decimal precision of 38. If a PostgreSQL table has a `numeric(p,s)` column where `p > 38`, pg2iceberg will fail on start, and also fail on schema evolution. This is intentional to avoid data corruption.

Unconstrained `numeric` columns (no precision specified) use `decimal(38,18)` as default.

## Supported Iceberg Catalogs

Any catalogs that follows the Iceberg [REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/) should be supported by pg2iceberg. The following catalogs have been verified to work.

| Catalog | Authentication | Vended Credentials? |
|---|---|---|
| Cloudflare R2 Data Catalog | Bearer | Yes |
| AWS Glue | SigV4 with IAM | No |

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

### Environment variables

| Env var | CLI flag | Description |
|---|---|---|
| `POSTGRES_URL` | `--postgres-url` | PostgreSQL connection URL |
| `POSTGRES_HOST` | | PostgreSQL host (overrides URL) |
| `POSTGRES_PORT` | | PostgreSQL port (overrides URL) |
| `POSTGRES_DATABASE` | | PostgreSQL database (overrides URL) |
| `POSTGRES_USER` | | PostgreSQL user (overrides URL) |
| `POSTGRES_PASSWORD` | | PostgreSQL password (overrides URL) |
| `TABLES` | `--tables` | Comma-separated list of tables |
| `MODE` | `--mode` | `logical` (default) or `query` |
| `SLOT_NAME` | `--slot-name` | Replication slot (default: `pg2iceberg_slot`) |
| `PUBLICATION_NAME` | `--publication-name` | Publication (default: `pg2iceberg_pub`) |
| `ICEBERG_CATALOG_URL` | `--iceberg-catalog-url` | Iceberg REST catalog URL |
| `WAREHOUSE` | `--warehouse` | Iceberg warehouse path |
| `NAMESPACE` | `--namespace` | Iceberg namespace |
| `S3_ENDPOINT` | `--s3-endpoint` | S3 endpoint URL |
| `S3_ACCESS_KEY` | `--s3-access-key` | S3 access key |
| `S3_SECRET_KEY` | `--s3-secret-key` | S3 secret key |
| `S3_REGION` | `--s3-region` | S3 region (default: `us-east-1`) |
| `SNAPSHOT_ONLY` | `--snapshot-only` | Exit after initial snapshot (default: `false`) |
| `STATE_POSTGRES_URL` | `--state-postgres-url` | Separate Postgres for checkpoint storage |
| `METRICS_ADDR` | `--metrics-addr` | Metrics server address (default: `:9090`) |

See config.example.yaml for the full config YAML.

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

## Observability

### OpenTelemetry Tracing

pg2iceberg exports distributed traces via OTLP/gRPC. Set the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable to enable:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 pg2iceberg
```

When unset, tracing is a no-op with zero overhead.

**Trace hierarchy:**

```
pg2iceberg.flush                           # events table flush cycle
  pg2iceberg.flush.table {table}           # per events table (parallel across tables)
    catalog.LoadTable {table} (cached)     # metadata cache hit (0ms)
    pg2iceberg.upload                      # data + manifests + manifest list (all parallel)
      s3.Upload {table} data
      s3.Upload {table} metadata
      s3.Upload {table} metadata
  catalog.CommitTransaction                # atomic catalog commit
    http POST /v1/transactions/commit      # actual HTTP round-trip (peer.service=iceberg-catalog)

pg2iceberg.materialize                     # materialization cycle
  pg2iceberg.materialize.table {table}     # per materialized table (parallel across tables)
    catalog.LoadTable {table} (cached)
    pg2iceberg.serialize                   # parquet encoding
    pg2iceberg.upload                      # data + manifests + manifest list (all parallel)
      s3.Upload {table} data
      s3.Upload {table} metadata
  catalog.CommitTransaction

pg2iceberg.compact                         # only when thresholds exceeded
  pg2iceberg.upload
  catalog.CommitTransaction
```

**External services** identified via `peer.service` attribute:

| Service | Spans |
|---------|-------|
| `iceberg-catalog` | `http GET/POST /v1/...` (REST catalog API calls) |
| `s3` | `s3.Upload`, `s3.Download`, `s3.DownloadRange`, `s3.StatObject` |
| `postgres` | `checkpoint.pg SELECT/UPDATE/UPSERT` |

**SigNoz setup:** The `example/single/` directory includes a complete docker-compose with SigNoz, an OTel collector, and ClickHouse as the trace backend. Run `docker compose up` and open `http://localhost:3301` to view traces.

### Prometheus Metrics

pg2iceberg exposes Prometheus metrics on `:9090/metrics` (configurable via `metrics_addr` in config). Key metrics include:

- `pg2iceberg_flush_duration_seconds` - events table flush latency
- `pg2iceberg_materializer_duration_seconds` - materialization cycle latency
- `pg2iceberg_catalog_operation_duration_seconds` - catalog API latency by operation
- `pg2iceberg_replication_lag_bytes` - WAL lag from Postgres
- `pg2iceberg_rows_processed_total` - rows replicated by table and operation
