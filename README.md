# pg2iceberg

pg2iceberg replicates data from Postgres directly to Iceberg, no Kafka needed. Opinionated by design:
- Specifically replicates Postgres → Iceberg, nothing else.
- Assumes pg2iceberg is the sole writer of the Iceberg tables it manages, including compaction.

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  OLAP[Snowflake<br />ClickHouse<br />etc.] -- Query --> ICE
```

## How it works

pg2iceberg can operate in **logical replication** mode (recommended, full CDC) or **query** mode (watermark-based polling for Postgres replicas without `wal_level=logical`).

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

pg2iceberg captures WAL change events via PostgreSQL logical replication and stages them as Parquet files in S3. A lightweight coordination layer in the source Postgres database (`_pg2iceberg` schema) tracks offsets and materializer progress. Since the write path only involves S3 uploads + a small PG transaction (no Iceberg catalog on the hot path), the replication slot LSN can be advanced quickly, minimizing WAL retention on the source.

A materializer, which runs at a separate interval, reads the staged Parquet files and merges them into the corresponding Iceberg tables using merge-on-read (equality deletes for updates/deletes, data files for inserts).

Staged files use a fixed Parquet schema regardless of source table changes: metadata columns (`_op`, `_lsn`, `_ts`, `_unchanged_cols`) plus a JSON `_data` column containing user data. Schema evolution (`ALTER TABLE`) only affects the Iceberg materialized table, not the staging layer.

#### Deployment modes

**Single-process** (default `pg2iceberg run`): one process runs the WAL writer and materializer together. Simplest to deploy.

```
+------------------------------+
|  pg2iceberg run              |
|  +----------+  +------------+|
|  |WAL Writer|->|Materializer||
|  +----------+  +------------+|
+------------------------------+
```

**Distributed**: one `pg2iceberg stream-only` process owns the replication slot; N `pg2iceberg materializer-only --worker-id <id>` workers each claim a deterministic slice of tables via heartbeat-based coordination. Workers can be added or removed dynamically — tables rebalance on the next cycle.

```
+------------------+   +--------------------------+   +--------------------------+
| stream-only      |   | materializer-only        |   | materializer-only        |
| +------------+   |   |  --worker-id worker-a    |   |  --worker-id worker-b    |
| | WAL Writer |   |   |  (tables 1, 3)           |   |  (tables 2, 4)           |
| +------------+   |   +--------------------------+   +--------------------------+
+------------------+              ^                                ^
                                  +-- _pg2iceberg.consumer ---------+
                                       (heartbeat registry)
```

#### Coordination

All coordination state lives under the `_pg2iceberg` schema in the source (or a dedicated state) Postgres:

| Table | Purpose |
|-------|---------|
| `log_seq` | Per-table offset counter (atomic increment) |
| `log_index` | Sparse index of staged Parquet files with offset ranges + LSN |
| `mat_cursor` | Materializer progress (last committed offset per table) |
| `consumer` | Heartbeat registry for distributed materializer workers |
| `lock` | Per-table locks (legacy; not load-bearing in current design) |
| `pipeline_meta` | Singleton: source-cluster `system_identifier` (DSN-swap detection) |
| `flushed_lsn` | Singleton: highest LSN we've acked the slot to (slot-tamper detection) |
| `tables` | Per-table snapshot status + `pg_class.oid` (drop-recreate detection) |
| `snapshot_progress` | Per-table mid-snapshot resume cursor |
| `query_watermarks` | Per-table watermark for query mode |
| `pending_markers` | Pending blue-green replica-alignment markers |
| `marker_emissions` | Per-(uuid, table) marker emission record (idempotent dedup) |

Coordinator write amplification is negligible: a few small PG writes per flush regardless of batch size.

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

Query mode polls Postgres using watermark-based `SELECT` queries and writes directly to the materialized Iceberg tables. Each row is an upsert (equality delete + insert) keyed by primary key.

Query mode is simpler but cannot detect hard deletes and has no transaction semantics. Use logical mode when you need full CDC fidelity.

## CLI subcommands

```sh
pg2iceberg <SUBCOMMAND> --config /etc/pg2iceberg/config.yaml [flags...]
```

| Subcommand | Purpose |
|---|---|
| `run` | Long-running pipeline. Logical or query mode depending on `source.mode`. |
| `snapshot` | One-shot: run the initial snapshot phase per configured table, then exit. Auto-creates the slot first so a later `run` doesn't lose WAL. |
| `cleanup` | Drop the replication slot, drop the publication, and `DROP SCHEMA … CASCADE` on the coordinator. Resets PG-side state ahead of a re-bootstrap. **Doesn't drop Iceberg tables** — do that out-of-band. |
| `compact` | One-shot: run a single compaction pass over every configured table, then exit. For cron / k8s `CronJob`. |
| `maintain` | One-shot: snapshot expiry + orphan-file cleanup over every configured table. Reads `sink.maintenance_retention` / `sink.maintenance_grace`. |
| `verify` | Diff PG ground truth against Iceberg materialized state for every configured table. Exits non-zero on any diff. Day-2 confidence check. |
| `stream-only` | Distributed mode: WAL writer only; pair with one or more `materializer-only` workers. |
| `materializer-only --worker-id <id>` | Distributed mode: materializer worker only. Joins the heartbeat group keyed by `state.group`; tables auto-rebalance on join/leave. |
| `migrate-coord` | Run the coordinator's idempotent schema migration (every statement is `CREATE … IF NOT EXISTS`). |
| `connect-pg` / `connect-iceberg` | Connectivity smoke tests for the PG / Iceberg-catalog prod paths. |

Run `pg2iceberg --help` for the full list and per-subcommand flags.

## Code structure

```
pg2iceberg/
├── Cargo.toml                 # workspace root; pins polynya-dev/iceberg-rust fork
├── crates/
│   ├── pg2iceberg/            # binary: CLI dispatch, run.rs, setup.rs
│   ├── pg2iceberg-core/       # types only (no IO): Lsn, ChangeEvent, TableSchema, Mode, …
│   ├── pg2iceberg-pg/         # PG client: pgoutput stream, slot health, replication trait
│   ├── pg2iceberg-coord/      # Coordinator trait + SQL + Postgres impl
│   ├── pg2iceberg-stream/     # BlobStore trait + object_store-backed prod impl + codec
│   ├── pg2iceberg-iceberg/    # Catalog trait, TableWriter, MoR fold, vended-S3 router, meta tables
│   ├── pg2iceberg-logical/    # Pipeline + Materializer + ticker schedule
│   ├── pg2iceberg-snapshot/   # Resumable snapshot phase
│   ├── pg2iceberg-query/      # Query-mode pipeline
│   ├── pg2iceberg-validate/   # Startup invariants + lifecycle helper + verify subcommand
│   ├── pg2iceberg-sim/        # Memory-backed implementations of every prod trait (DST harness)
│   └── pg2iceberg-tests/      # DST scenario tests + testcontainers integration tests
├── docs/                      # mdbook-style reference (architecture, catalogs, usage, …)
├── example/
│   ├── single/                # Docker Compose stack: PG + iceberg-rest + MinIO + Grafana
│   └── blue-green/            # Two-side replica-alignment example with marker UUIDs
└── Dockerfile                 # multi-stage release build
```

Every IO-touching crate is gated behind a `prod` feature; the default build is sim-only and feeds the deterministic-simulation testing harness in `pg2iceberg-tests`.

## Type mapping

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

### Decimal precision limit

Iceberg supports a maximum decimal precision of 38. If a PostgreSQL table has a `numeric(p,s)` column where `p > 38`, pg2iceberg fails on start, and also fails on schema evolution. This is intentional to avoid silent data corruption.

Unconstrained `numeric` columns (no precision specified) default to `decimal(38,18)`.

### Schema evolution

| Change | Iceberg behavior |
|---|---|
| `ADD COLUMN` (nullable) | Appends a column with the next field id |
| `DROP COLUMN` | Soft-drop: column stays in schema, becomes nullable; older data files keep resolving |
| `ALTER COLUMN TYPE` (legal promotion: `int → long`, `float → double`, decimal precision increase) | Type-promote in place, field id preserved |
| `ALTER COLUMN TYPE` (illegal: narrowing, cross-family) | Refuses with an actionable error; operator must re-snapshot |
| `RENAME COLUMN` | Treated as drop + add (pgoutput doesn't carry attribute OIDs to detect renames) |
| `SET / DROP NOT NULL` | Invisible: pgoutput Relation messages don't carry nullability |

## Supported Iceberg catalogs

Any catalog implementing the [Iceberg REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/) should work. The following have been verified end-to-end:

| Catalog | Authentication | Vended Credentials? |
|---|---|---|
| Apache Polaris | OAuth2 / Bearer | Yes (`credential_mode: vended`) |
| Apache REST reference (testcontainers) | None | No |
| Cloudflare R2 Data Catalog | Bearer | Yes (not yet re-verified end-to-end) |
| AWS Glue | SigV4 with IAM | No (not yet re-verified end-to-end) |

## Quickstart

```sh
cd example/single
docker compose up -d --wait
```

Open `http://localhost:8123/play` and run:

```sql
SELECT * FROM rideshare.`rideshare.rides`
```

You should see new rows appearing as the simulator drives PG.

## Configuration

Configuration is YAML-first; see [`config.example.yaml`](config.example.yaml) for the full surface. CLI flags and env vars override individual fields.

| Env var | YAML field | Description |
|---|---|---|
| `POSTGRES_URL` | `source.postgres.dsn` | PostgreSQL connection URL |
| `TABLES` | `tables` | List of source tables to replicate |
| `MODE` | `source.mode` | `logical` (default) or `query` |
| `SLOT_NAME` | `source.logical.slot_name` | Replication slot (default: `pg2iceberg_slot`) |
| `PUBLICATION_NAME` | `source.logical.publication_name` | Publication (default: `pg2iceberg_pub`) |
| `ICEBERG_CATALOG_URL` | `sink.catalog_uri` | Iceberg REST catalog URL |
| `WAREHOUSE` | `sink.warehouse` | Iceberg warehouse path (`s3://bucket/prefix/`) |
| `NAMESPACE` | `sink.namespace` | Iceberg namespace |
| `S3_ENDPOINT` | `sink.s3_endpoint` | S3 endpoint URL |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` | `sink.s3_*` | S3 credentials and region |
| `STATE_POSTGRES_URL` | `state.postgres_url` | Optional separate Postgres for coord state |

### Credential modes (`sink.credential_mode`)

| Mode | Description |
|---|---|
| `static` (default) | Operator-supplied S3 keys in YAML / env |
| `iam` | AWS SDK env / IMDS / EC2-metadata credential chain |
| `vended` | Per-table credentials from the catalog (Polaris / Snowflake / Tabular). Requires `header.x-iceberg-access-delegation: vended-credentials` in the REST request, which pg2iceberg sets automatically when `credential_mode: vended`. |

## Coordinator state and recovery

State persisted in `_pg2iceberg`:

- **Cluster fingerprint** (`pipeline_meta.system_identifier`): stamped at first startup. A different `IDENTIFY_SYSTEM` value on subsequent runs (e.g. accidental DSN swap, blue-green cutover) returns `SystemIdMismatch` and refuses to start.
- **Slot-tamper baseline** (`flushed_lsn`): the highest LSN we've ever acked. Compared against `slot.confirmed_flush_lsn` at startup to catch external advancement (`pg_replication_slot_advance`, drop-recreate, stray `pg_recvlogical`).
- **Per-table snapshot state** (`tables`, `snapshot_progress`): `snapshot_complete` + `pg_oid` per table; mid-snapshot resume cursor cleared on completion.

To resume from where the previous process left off, just restart with the same config — coord cursors + slot LSN are sufficient.

To reset everything (cleanup PG-side state) without manually dropping tables:

```sh
pg2iceberg cleanup --config <path>
# Iceberg tables remain — drop them via the catalog separately if you want a full reset.
```

To use a separate Postgres for coord state (instead of the source DB):

```yaml
state:
  postgres_url: postgresql://user:pass@host:5432/state-db
  coordinator_schema: _pg2iceberg
```

## Running tests

The default test target is sim-based DST + unit tests; no Docker needed.

```sh
cargo test --workspace
```

Integration tests against testcontainers (PG 16-alpine + MinIO + Apache iceberg-rest) are gated behind the `integration` feature:

```sh
# Colima users: see docs/getting-started/installation.md for the env vars
cargo test --workspace --features integration -- --test-threads=1
```

The integration suite covers the prod Coordinator (PG-replication path), the prod PgClient (pgoutput decoding against real PG), and a full end-to-end LogicalLifecycle against the docker stack. Roughly 17 tests.

## Observability

### Prometheus metrics

A future maintenance pass will expose Prometheus metrics on a configurable address. The `Metrics` trait is wired through every hot path; the operational HTTP endpoint that exports them is **not yet wired**.

### Structured logs

The binary uses [`tracing`](https://docs.rs/tracing) with `tracing-subscriber` for stdout output. Set `RUST_LOG=info,pg2iceberg=debug` (or finer) to control verbosity.

### Distributed tracing

Distributed-tracing export via OTLP is **not yet wired**. The `tracing` instrumentation that exists is stdout-only today.

### Control-plane meta tables

When `sink.meta_namespace` is set, pg2iceberg writes operational telemetry to four Iceberg tables under that namespace:

| Table | Row written by |
|---|---|
| `<meta_ns>.commits` | Each successful materializer cycle (per-table) |
| `<meta_ns>.compactions` | Each successful compaction commit |
| `<meta_ns>.maintenance` | Each `expire_snapshots` / `clean_orphans` op (per-table) |
| `<meta_ns>.markers` | Blue-green marker alignment (when marker mode is enabled) |

See [`docs/usage/metadata-tables.md`](docs/usage/metadata-tables.md) for the full schemas.

A fifth, `<meta_ns>.checkpoints`, is exposed via the `record_checkpoint` API for callers who want to emit one manually but is not auto-written — recovery doesn't depend on periodic checkpoint saves (the slot's `confirmed_flush_lsn` plus per-table state is sufficient).
