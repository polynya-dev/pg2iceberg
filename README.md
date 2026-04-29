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
      Coord["_pg2iceberg schema<br />(coordination)"]
  end

  subgraph S3
      StagedA["Staged WAL<br />(Parquet)"]
      StagedB["Staged WAL<br />(Parquet)"]
  end

  subgraph Iceberg
      TargetA[Table A]
      TargetB[Table B]
  end

  TableA -->|Logical Replication| StagedA
  TableB -->|Logical Replication| StagedB
  StagedA -->|Materializer| TargetA
  StagedB -->|Materializer| TargetB
  StagedA -.->|offset index| Coord
  StagedB -.->|offset index| Coord
```

On logical replication mode (the recommended mode), it captures WAL change events and stages them as Parquet files in S3. A lightweight coordination layer in the source Postgres database (`_pg2iceberg` schema) tracks offsets and materializer progress. Since the write path only involves S3 uploads + a small PG transaction (no Iceberg catalog on the hot path), the replication slot LSN can be advanced quickly, minimizing WAL retention on the source database.

A materializer, which runs at a separate interval, reads the staged Parquet files and merges them into the corresponding Iceberg tables using merge-on-read (equality deletes for updates/deletes, data files for inserts/updates). In combined mode (single process), the materializer reads staged events directly from memory to avoid S3 download and Parquet parsing overhead.

Staged files use a fixed Parquet schema regardless of source table changes: metadata columns (`_op`, `_lsn`, `_ts`, `_unchanged_cols`) plus a JSON `_data` column containing user data. Schema evolution (ALTER TABLE) only affects the Iceberg materialized table, not the staging layer.

#### Deployment modes

**Combined mode** (default): A single process runs the WAL writer and materializer together. Staged events are served from an in-memory cache, avoiding S3 round-trips. Best for simplicity.

```
+------------------------------+
|  pg2iceberg                  |
|  +----------+  +------------+|
|  |WAL Writer|->|Materializer||
|  +----------+  +------------+|
|  (in-memory cache + S3)      |
+------------------------------+
```

**Horizontal scaling**: One process runs the WAL writer (single replication slot), and N materializer workers each claim tables via heartbeat locks. Workers are added or removed dynamically — tables rebalance automatically.

```
+------------------+   +--------------+   +--------------+
|  pg2iceberg      |   | materializer |   | materializer |
|  +----------+    |   |  worker A    |   |  worker B    |
|  |WAL Writer|    |   | (tables 1,3) |   | (tables 2,4) |
|  +----------+    |   +--------------+   +--------------+
|  (S3 only)       |         ^                   ^
+------------------+         +-- heartbeat locks -+
```

#### Coordination

All coordination state lives in the source Postgres database under the `_pg2iceberg` schema:

| Table | Purpose |
|-------|---------|
| `log_seq` | Per-table offset counter (atomic increment) |
| `log_index` | Sparse index of staged Parquet files with offset ranges |
| `mat_cursor` | Materializer progress (last committed offset per table) |
| `lock` | Heartbeat-based locks for distributed materializer |
| `checkpoints` | Replication checkpoint (LSN, snapshot state) |

Coordinator write amplification is negligible: ~3 PG writes per flush regardless of batch size. At the default `flush_rows=1000`, overhead is <0.3% of source write throughput per table.

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
├── logical/         # logical replication mode (WAL capture, staging, materializer)
├── pipeline/        # shared infrastructure (Pipeline interface, checkpoint, metrics)
├── postgres/        # shared PG types (TableSchema, ChangeEvent, Op)
├── query/           # query polling mode (watermark poller, PK buffer, pipeline)
├── stream/          # leaderless log (Coordinator, Stream, CachedStream)
├── snapshot/        # initial table snapshot (CTID-based chunked COPY)
└── utils/           # retry helper, task pool
```

The `stream` package implements the [leaderless log protocol](https://github.com/lakestream-io/leaderless-log-protocol):
- **Coordinator** interface — distributed coordination primitives (offset claiming, cursors, heartbeat locks) backed by PostgreSQL
- **BaseStream** — S3 upload + PG coordination (for multi-process mode)
- **CachedStream** — same, plus in-memory event cache (for combined mode — zero-copy from WAL writer to materializer)

Both modes share `iceberg.TableWriter` for the final write path (partition bucketing, Parquet serialization, S3 upload, manifest assembly, catalog commit).

## Type mapping

pg2iceberg maps PostgreSQL column types to Iceberg types automatically during schema discovery. Aliases (e.g. `integer`, `serial`) are normalized to their canonical form.

| PostgreSQL type | Iceberg type | Notes |
|---|---|---|
| `smallint` | `int` | |
| `integer`, `serial`, `oid` | `int` | |
| `bigint`, `bigserial` | `long` | |
| `real` | `float` | |
| `double precision` | `double` | |
| `numeric(p,s)` where p <= 38 | `decimal(p,s)` | Precision preserved exactly |
| `numeric(p,s)` where p > 38 | -- | **Pipeline refuses to start** (see below) |
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
| `MAINTENANCE_RETENTION` | | Snapshot retention duration (default: `168h` / 7 days) |
| `MAINTENANCE_INTERVAL` | | Background maintenance interval (default: `1h`) |
| `MAINTENANCE_GRACE` | | Orphan file grace period (default: `30m`) |

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

To run tests in parallel:

```sh
PARALLEL=4 ./tests/run.sh
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

**Trace hierarchy (write path):**

```
pg2iceberg.flush {flush.rows=1000}
  stream.Append {batch_count=1, cached=true}
    s3.Upload staged/orders/xxx.parquet
    coordinator.ClaimOffsets {batch_size=1}         # PG: atomic offset claim
  checkpoint.Save
    checkpoint.pg UPDATE                            # PG: checkpoint persistence
```

The write hot path involves one S3 upload + one small PG transaction (ClaimOffsets). No Iceberg catalog operations.

**Trace hierarchy (read path):**

```
pg2iceberg.materialize
  coordinator.GetCursor {table=public.orders}       # PG: read cursor
  pg2iceberg.materialize.readEvents {entry_count=1, cache_hits=1, cache_misses=0}
  pg2iceberg.materialize.table {table=public.orders}
    s3.Upload orders data                           # data files
    s3.Upload orders metadata                       # manifests
  catalog.CommitTransaction                         # Iceberg REST catalog
    http POST /v1/transactions/commit
  coordinator.SetCursor {table, offset=1000}        # PG: advance cursor
```

In combined mode, `cache_hits > 0` confirms the materializer reads staged events from memory (zero S3 download, zero Parquet/JSON parsing). On recovery (cold start), `cache_misses > 0` indicates the S3 fallback path.

**Trace hierarchy (distributed mode):**

```
pg2iceberg.materialize
  coordinator.TryLock {table=public.orders, worker_id=worker-a}   # acquired
  coordinator.TryLock {table=public.payments, worker_id=worker-a}  # blocked (held by worker-b)
  coordinator.GetCursor {table=public.orders}
  ...
  coordinator.SetCursor {table=public.orders, offset=5000}
```

**External services** identified via `peer.service` attribute:

| Service | Spans |
|---------|-------|
| `iceberg-catalog` | `http GET/POST /v1/...` (REST catalog API calls) |
| `s3` | `s3.Upload`, `s3.Download`, `s3.DownloadRange`, `s3.StatObject` |
| `postgres` | `coordinator.*`, `checkpoint.pg SELECT/UPDATE/UPSERT` |

**Measuring coordinator write amplification** from span metrics:

```
amplification = rate(coordinator_ClaimOffsets_count) / rate(flush_rows_sum)
```

At default settings (`flush_rows=1000`), expect <0.3% per table.

**SigNoz setup:** The `example/single/` directory includes a complete docker-compose with SigNoz, an OTel collector, and ClickHouse as the trace backend. Run `docker compose up` and open `http://localhost:3301` to view traces.

### Prometheus Metrics

pg2iceberg exposes Prometheus metrics on `:9090/metrics` (configurable via `metrics_addr` in config). Key metrics include:

- `pg2iceberg_flush_duration_seconds` - staged WAL flush latency
- `pg2iceberg_materializer_duration_seconds` - materialization cycle latency
- `pg2iceberg_catalog_operation_duration_seconds` - catalog API latency by operation
- `pg2iceberg_replication_lag_bytes` - WAL lag from Postgres
- `pg2iceberg_rows_processed_total` - rows replicated by table and operation
- `pg2iceberg_maintenance_snapshots_expired_total` - snapshots expired by maintenance
- `pg2iceberg_maintenance_orphans_deleted_total` - orphan files deleted by maintenance

### HTTP Endpoints

The metrics server (`:9090` by default) exposes the following endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | Pipeline health check (503 if status is `error`) |
| `GET /ready` | Readiness probe (503 if pipeline is not `running`) |
| `GET /tables` | Per-table metadata, schema, and statistics |

#### `GET /tables`

Returns metadata and runtime statistics for every replicated table. Includes source and Iceberg schema information, partition specs, and per-table counters.

```bash
curl http://localhost:9090/tables | jq .
```

```json
[
  {
    "source_table": "public.orders",
    "namespace": "analytics",
    "iceberg_table": "orders",
    "columns": [
      {"name": "id", "pg_type": "int4", "iceberg_type": "int", "nullable": false},
      {"name": "amount", "pg_type": "numeric", "iceberg_type": "decimal(10,2)", "nullable": true},
      {"name": "created_at", "pg_type": "timestamptz", "iceberg_type": "timestamptz", "nullable": false}
    ],
    "primary_key": ["id"],
    "partition_spec": ["day(created_at)"],
    "stats": {
      "rows_processed": 152340,
      "buffered_rows": 47,
      "buffered_bytes": 6016
    }
  }
]
```

| Field | Description |
|-------|-------------|
| `source_table` | Fully qualified PostgreSQL table name |
| `namespace` | Iceberg namespace |
| `iceberg_table` | Iceberg table name |
| `columns` | Column list with PostgreSQL type, Iceberg type, and nullability |
| `primary_key` | Primary key column names |
| `partition_spec` | Partition expressions from config (e.g. `day(ts)`, `bucket[16](id)`) |
| `stats.rows_processed` | Cumulative rows replicated for this table |
| `stats.buffered_rows` | Rows currently buffered awaiting flush |
| `stats.buffered_bytes` | Estimated bytes currently buffered |
