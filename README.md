# pg2iceberg

pg2iceberg mirrors data from Postgres directly to Iceberg, no Kafka needed. Opinionated by design:
- Only supports Postgres as source and Iceberg as destination, nothing else.
- Mirrors data, i.e. source and target contain the same data. So no such thing as skipping snapshot.
- Stateless, all state lives in Postgres and S3. This makes operation simple.
- Assumes pg2iceberg is the sole writer of the rows in the Iceberg tables it manages. It compacts and maintains them too, unless their catalog does (`sink.maintenance: managed`).

```mermaid
graph LR
  App[Application] <-->|Read/Write| PG

  subgraph pg2iceberg
      PG[Postgres] -->|Replicate| ICE[Iceberg]
  end

  OLAP[Snowflake<br />ClickHouse<br />etc.] -- Query --> ICE
```

## How it works

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

pg2iceberg captures WAL change events via PostgreSQL logical replication and stages them as Parquet files in S3 via [leaderless log protocol](https://github.com/lakestream-io/leaderless-log-protocol/). A lightweight coordination layer in the source Postgres database (`_pg2iceberg` schema) tracks offsets and materializer progress. Since the write path only involves S3 uploads + a small PG transaction (no Iceberg catalog on the hot path), the replication slot LSN can be advanced quickly, minimizing WAL retention on the source.

A materializer, which runs at a separate interval, reads the staged Parquet files and merges them into the corresponding Iceberg tables using merge-on-read (equality deletes for updates/deletes, data files for inserts).

Staged files use a fixed Parquet schema regardless of source table changes: metadata columns (`_op`, `_lsn`, `_ts`, `_unchanged_cols`) plus a JSON `_data` column containing user data. Schema evolution (`ALTER TABLE`) only affects the Iceberg materialized table, not the staging layer.

### Deployment modes

**Single-process** (default `pg2iceberg run`): one process runs the WAL writer and materializer together. Simplest to deploy.

```
┌──────────┐
│ Postgres │
└────┬─────┘
     │ logical replication
     ▼
┌─ pg2iceberg run ────────────────┐
│ WAL writer                      │
│   │ staged Parquet + log_index  │
│   ▼                             │
│ Materializer                    │
└────┬────────────────────────────┘
     ▼
┌─────────┐
│ Iceberg │
└─────────┘
```

**Distributed**: one `pg2iceberg stream-only` process owns the replication slot; N `pg2iceberg materializer-only --worker-id <id>` workers each claim a deterministic slice of tables via heartbeat-based coordination. Workers can be added or removed dynamically — tables rebalance on the next cycle.

```
┌──────────┐
│ Postgres │
└────┬─────┘
     │ logical replication (one slot)
     ▼
┌─ pg2iceberg stream-only ────────┐
│ WAL writer                      │
└────┬────────────────────────────┘
     │ staged Parquet + log_index
     ├────────────────────────────────────┐
     ▼                                    ▼
┌─ materializer-only ─────────────┐  ┌─ materializer-only ─────────────┐
│ --worker-id worker-a            │  │ --worker-id worker-b            │
│ tables 1, 3                     │  │ tables 2, 4                     │
└────┬────────────────────────────┘  └────┬────────────────────────────┘
     └─────────────────┬──────────────────┘
                       ▼
                  ┌─────────┐
                  │ Iceberg │
                  └─────────┘
```

### Coordination

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
| `pending_markers` | Pending blue-green replica-alignment markers |
| `marker_emissions` | Per-(uuid, table) marker emission record (idempotent dedup) |
| `table_epoch` | Per-table write epoch: tells processes caching a table's Iceberg metadata that another one wrote it |

Coordinator write amplification is negligible: a few small PG writes per flush regardless of batch size.

## CLI subcommands

```sh
pg2iceberg <SUBCOMMAND> [--config pg2iceberg.yaml] [flags...]
```

Every subcommand takes its settings from environment variables and an optional config file (see [Configuration](#configuration)).

| Subcommand | Purpose |
|---|---|
| `init` | Inspect the source database and write `pg2iceberg.yaml`: every table pg2iceberg can replicate, the settings the environment gives, secrets as `${VAR}` references. Checks what replication needs of the database too (`wal_level`, the replication privilege, a free slot, table ownership for the publication). |
| `run` | Long-running pipeline: initial snapshot, then CDC via logical replication. |
| `snapshot` | One-shot: run the initial snapshot phase per configured table, then exit. Auto-creates the slot first so a later `run` doesn't lose WAL. |
| `cleanup` | Drop the replication slot, drop the publication, and `DROP SCHEMA … CASCADE` on the coordinator. Resets PG-side state ahead of a re-bootstrap. **Doesn't drop Iceberg tables** — do that out-of-band. |
| `compact` | One-shot: run a single compaction pass over every configured table, then exit. For cron / k8s `CronJob`. With `sink.maintenance: managed`, only retires delete files. |
| `maintain` | One-shot: snapshot expiry + orphan-file cleanup over every configured table. Reads `sink.maintenance_retention` / `sink.maintenance_grace`. Refuses with `sink.maintenance: managed`. |
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
│   ├── pg2iceberg-core/       # types only (no IO): Lsn, ChangeEvent, TableSchema, …
│   ├── pg2iceberg-pg/         # PG client: pgoutput stream, slot health, replication trait
│   ├── pg2iceberg-coord/      # Coordinator trait + SQL + Postgres impl
│   ├── pg2iceberg-stream/     # BlobStore trait + object_store-backed prod impl + codec
│   ├── pg2iceberg-iceberg/    # Catalog trait, TableWriter, MoR fold, vended-S3 router, meta tables
│   ├── pg2iceberg-logical/    # Pipeline + Materializer + ticker schedule
│   ├── pg2iceberg-snapshot/   # Resumable snapshot phase
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
| `ADD COLUMN … DEFAULT` | As above, and the rows already in the table get the default, which PostgreSQL stores instead of writing it to the WAL. If PostgreSQL no longer has the value when pg2iceberg reads it (a volatile default, or a rewrite, drop or rename in between), they keep NULL, with a warning; see [schema evolution](docs/architecture/schema-evolution.md#columns-added-with-a-default) |
| `DROP COLUMN` | Renamed to `<name>__dropped_<field id>` and made nullable: its values stay readable, and a column later added with its name is a new one |
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
| Cloudflare R2 Data Catalog | Bearer | Yes ([setup](docs/catalogs/r2.md)) |
| AWS Glue | SigV4 with IAM | No (not yet re-verified end-to-end) |

## Quickstart

```sh
cd example/single
docker compose up -d --wait
docker compose --profile workload up -d workload
```

Open `http://localhost:8123/play` and run:

```sql
SELECT * FROM rideshare.`rideshare.rides`
```

You should see new rows appearing as the workload drives PG.

Against your own database and catalog, environment variables are enough:

```sh
export POSTGRES_URL=postgres://user:password@db.example.com:5432/app
export ICEBERG_CATALOG_URL=https://catalog.example.com
export ICEBERG_WAREHOUSE=s3://my-bucket/warehouse/
pg2iceberg init   # optional: check the database, and write pg2iceberg.yaml to edit
pg2iceberg run
```

With no tables configured, pg2iceberg replicates every table with a primary key. S3 credentials come from the AWS default chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, a profile, an instance or task role).

`./run.sh` in the same directory runs it as a smoke test: the mixed workload — deletes, large and rolled-back transactions, key changes, TOAST, schema changes — with a crash, external compactions and snapshot expiry alongside, then a row-by-row comparison of Postgres with ClickHouse. See [`example/single`](example/single).

## Configuration

Settings come from environment variables and an optional YAML file: `--config`, else `PG2ICEBERG_CONFIG`, else `pg2iceberg.yaml` in the current directory if it exists. Environment variables override the file, and the file can read them: `${VAR}` in a string value is replaced by the variable. See [`config.example.yaml`](config.example.yaml) for the full surface, and [Configuration](docs/usage/configuration.md) for the details.

| Env var | Config field | Description |
|---|---|---|
| `POSTGRES_URL` | `source.postgres_url` | Source database: a URL or `key=value` connection string (required) |
| `PG2ICEBERG_TABLES` | `tables` | Comma-separated `schema.table` or `schema.*`. Default: every table with a primary key |
| `PG2ICEBERG_SLOT` / `PG2ICEBERG_PUBLICATION` | `source.logical.slot_name` / `publication_name` | Default `pg2iceberg_slot` / `pg2iceberg_pub` |
| `PG2ICEBERG_STATE_URL` | `state.postgres_url` | A separate Postgres for pg2iceberg's state. Default: the source |
| `PG2ICEBERG_CONFIG` | — | The config file |
| `ICEBERG_CATALOG_URL` | `sink.catalog_uri` | Iceberg REST catalog (required) |
| `ICEBERG_CATALOG_TOKEN` | `sink.catalog_token` | Bearer token |
| `ICEBERG_CATALOG_CLIENT_ID` / `_SECRET` | `sink.catalog_client_id` / `_secret` | OAuth2 client credentials |
| `ICEBERG_CATALOG_AUTH` | `sink.catalog_auth` | `none` / `bearer` / `oauth2` / `sigv4`. Default: inferred |
| `ICEBERG_WAREHOUSE` | `sink.warehouse` | `s3://bucket/prefix/` |
| `ICEBERG_NAMESPACE` | `sink.namespace` | One namespace for every table. Default: each table's Postgres schema |
| `ICEBERG_CREDENTIAL_MODE` | `sink.credential_mode` | `static` / `iam` / `vended`. Default: inferred |
| `PG2ICEBERG_METRICS_ADDR` | `metrics_addr` | Where to serve `/metrics`, `/healthz`, `/readyz`. Default `:9090`; `off` serves nothing |
| `PG2ICEBERG_LIVENESS_TIMEOUT` | `liveness_timeout` | `/healthz` fails once nothing completes for this long. Default `5m` |
| `PG2ICEBERG_MAINTENANCE` | `sink.maintenance` | `pg2iceberg` / `managed` (the catalog compacts, expires and cleans the tables). Default: `pg2iceberg` |
| `AWS_REGION`, `AWS_ENDPOINT_URL_S3`, `AWS_ACCESS_KEY_ID`, ... | `sink.s3_*` | Read as AWS tools read them; the file's `sink.s3_*` take precedence |

### Inferred settings

| Setting | Inferred |
|---|---|
| Catalog auth | `bearer` with a token, `oauth2` with client credentials, `sigv4` for an AWS endpoint (S3 Tables, Glue), else none |
| Credential mode | `static` with `sink.s3_access_key` set; `iam` with an `s3://` warehouse: the AWS default credential chain — `AWS_*` variables, a profile, an instance or task role; otherwise (no warehouse, or a catalog-side name like Polaris's) `vended`: temporary credentials from the catalog |
| Region | `sink.s3_region` / `AWS_REGION`, else an AWS catalog endpoint's, else `us-east-1` |
| Tables | Every table with a primary key, outside pg2iceberg's own schema; those without one are skipped with a warning |

## Coordinator state and recovery

State persisted in `_pg2iceberg`:

- **Cluster fingerprint** (`pipeline_meta.system_identifier`): stamped at first startup. A different `IDENTIFY_SYSTEM` value on subsequent runs (e.g. accidental DSN swap, blue-green cutover) returns `SystemIdMismatch` and refuses to start.
- **Slot-tamper baseline** (`flushed_lsn`): the highest LSN we've ever acked — every ack is recorded first, and never past the record. Compared against `slot.confirmed_flush_lsn` at startup and by the runtime watcher to catch external advancement (`pg_replication_slot_advance`, drop-recreate, stray `pg_recvlogical`).
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

### Prometheus metrics and health checks

`run`, `stream-only`, `materializer-only` and `snapshot` serve `/metrics` (Prometheus), `/healthz` and `/readyz` on `metrics_addr` (default `:9090`): replication lag and slot WAL, per-table backlog and freshness, throughput, the latency and errors of every catalog, object store and coordinator request, and more. `/healthz` fails once nothing has completed for `liveness_timeout` (default `5m`) — stuck, not slow. See [Observability](docs/usage/observability.md) for every metric and what to alert on, and `example/single` (`--profile monitoring`) for Prometheus, alert rules and a Grafana dashboard.

### Structured logs

The binary uses [`tracing`](https://docs.rs/tracing) with `tracing-subscriber` for stdout output. Set `RUST_LOG=info,pg2iceberg=debug` (or finer) to control verbosity.

### Distributed tracing

Distributed-tracing export via OTLP is **not yet wired**. Logs are stdout-only today.

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
