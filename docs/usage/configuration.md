---
icon: lucide/settings
---

# Configuration

pg2iceberg takes its settings from environment variables and an optional YAML file. Two are required — the source database and the Iceberg catalog — and the rest have defaults or are inferred:

```bash
export POSTGRES_URL=postgres://user:password@db.example.com:5432/app
export ICEBERG_CATALOG_URL=https://catalog.example.com
export ICEBERG_WAREHOUSE=s3://my-bucket/warehouse/
pg2iceberg run
```

`pg2iceberg init` inspects the database and writes a config file to start from — see [below](#generating-a-config).

## Where settings come from

```
defaults < config file < environment variables < flags
```

The config file is `--config <path>`, else `PG2ICEBERG_CONFIG`, else `pg2iceberg.yaml` in the current directory if it exists. Every section of it is optional.

In the file's string values, `${VAR}` is replaced by the environment variable `VAR` (an error if it isn't set), and `$${` is a literal `${`. Comments aren't read, so a commented-out line may name a variable that isn't set. This keeps secrets out of the file:

```yaml
sink:
  catalog_uri: https://catalog.example.com
  catalog_token: ${CATALOG_TOKEN}
```

### Environment variables

| Variable | Config field |
|---|---|
| `POSTGRES_URL` | `source.postgres_url`: a URL (`postgres://user:password@host:5432/db?sslmode=require`) or libpq `key=value` pairs |
| `PG2ICEBERG_TABLES` | `tables`: comma-separated `schema.table`, or `schema.*` for every table with a primary key in a schema |
| `PG2ICEBERG_SLOT` | `source.logical.slot_name` |
| `PG2ICEBERG_PUBLICATION` | `source.logical.publication_name` |
| `PG2ICEBERG_STATE_URL` | `state.postgres_url` |
| `PG2ICEBERG_CONFIG` | the config file |
| `ICEBERG_CATALOG_URL` | `sink.catalog_uri` |
| `ICEBERG_CATALOG_AUTH` | `sink.catalog_auth` |
| `ICEBERG_CATALOG_TOKEN` | `sink.catalog_token` |
| `ICEBERG_CATALOG_CLIENT_ID` / `ICEBERG_CATALOG_CLIENT_SECRET` | `sink.catalog_client_id` / `sink.catalog_client_secret` |
| `ICEBERG_WAREHOUSE` | `sink.warehouse` |
| `ICEBERG_NAMESPACE` | `sink.namespace` |
| `ICEBERG_CREDENTIAL_MODE` | `sink.credential_mode` |
| `PG2ICEBERG_METRICS_ADDR` | `metrics_addr` |
| `PG2ICEBERG_LIVENESS_TIMEOUT` | `liveness_timeout` |

AWS's own variables are read as AWS tools read them, and give way to the config file: `AWS_REGION` / `AWS_DEFAULT_REGION` for `sink.s3_region`, `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` for `sink.s3_endpoint`. Credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) are read by the AWS default credential chain, with `credential_mode: iam`.

### Inferred settings

| Setting | When not set |
|---|---|
| `tables` | Every table with a primary key, when pg2iceberg starts — including ones created since the last start. Partitioned tables count as one table (their partitions replicate as it); views, unlogged tables, extensions' tables and pg2iceberg's own schema don't count. Tables without a primary key, or that the role can't read, or with a column Iceberg can't hold, are skipped with a warning. In distributed mode each process discovers the tables when it starts: after the WAL writer (`stream-only`) picks up a new table, restart the `materializer-only` workers, or list the tables. |
| `sink.catalog_auth` | `bearer` with a `catalog_token`; `oauth2` with a `catalog_client_id`; `sigv4` when the catalog is an AWS endpoint (`*.amazonaws.com`: S3 Tables, Glue), signed for its service and region; else none. |
| `sink.credential_mode` | `static` when `s3_access_key` is set; `iam`, the AWS default credential chain, with an `s3://` warehouse; otherwise — no `warehouse`, or a catalog-side name for one, as Polaris, Lakekeeper and R2 take — `vended`: temporary credentials from the catalog. |
| `sink.s3_region` | The AWS catalog endpoint's region, else `us-east-1`. |
| `sink.namespace` | Each table's Postgres schema. |
| `sink.s3_endpoint` | AWS S3 itself. Set it for MinIO, R2 and other S3-compatible storage; pg2iceberg then uses path-style requests. |

## Generating a config

```bash
POSTGRES_URL=postgres://... pg2iceberg init
```

`init` reads the environment (not an existing config file), and writes `pg2iceberg.yaml` (`--output <path>`, or `--output -` to print; `--force` to overwrite):

- every table it would replicate, with its estimated size; the tables it would skip are listed, commented out, with the reason;
- the settings the environment gives; secrets (`POSTGRES_URL`, catalog tokens and client secrets) as `${VAR}` references, so they stay in the environment.

It also checks what replication needs of the database, and says how to fix what's missing: `wal_level = logical`, the replication privilege, a free replication slot, a place for pg2iceberg's state schema, and ownership of the tables (creating the publication takes it).

Listing the tables pins them: a table created later isn't replicated until it's added. Delete `tables:` to replicate every table with a primary key again.

## Full reference

```yaml
# Replication source
source:
  mode: logical                  # the only mode (default); may be omitted
  postgres_url: ""               # or a connection string, in place of `postgres`
  postgres:
    host: ""                     # required without postgres_url
    port: 5432
    database: ""                 # required without postgres_url
    user: ""                     # required without postgres_url
    password: ""
    sslmode: disable             # "disable" | "require" | "verify-ca" | "verify-full"
                                 # — currently anything non-disable enables
                                 #   webpki-roots verification; finer-grained
                                 #   modes (mTLS, custom CA) are follow-ons.

  # Logical replication settings
  logical:
    publication_name: pg2iceberg_pub
    slot_name: pg2iceberg_slot
    standby_interval: 10s        # how often the standby_status ack is sent

# Tables to replicate. Leave out for every table with a primary key.
tables:
  - name: public.orders          # fully-qualified PostgreSQL table name, or schema.*
    iceberg:
      partition:                 # partition transforms — six are supported:
        - "day(created_at)"      #   year/month/day/hour
        - "bucket[16](id)"       #   murmur3_x86_32 % N
        - "region"               #   identity
        - "truncate[4](name)"    #   string truncation / int floor

    # Optional — overrides discovered PK
    # primary_key: [id]

    # Optional — overrides discovery from information_schema/pg_index
    # columns:
    #   - { name: id, pg_type: int4 }
    #   - { name: qty, pg_type: int8, nullable: true }

    # Skip the initial snapshot for this table (already populated by some
    # other process). Default: false.
    # skip_snapshot: false

# Iceberg sink
sink:
  catalog_uri: ""                # Iceberg REST catalog URL (required)
  catalog_auth: ""               # "none" | "bearer" | "oauth2" | "sigv4"; default: inferred
  catalog_token: ""              # bearer token
  catalog_client_id: ""          # OAuth2 client ID
  catalog_client_secret: ""      # OAuth2 client secret

  credential_mode: ""            # "static" | "iam" | "vended"; default: inferred
  warehouse: ""                  # s3://bucket/prefix (required for static/iam)
  namespace: ""                  # one Iceberg namespace for every table; default: each table's PG schema
  s3_endpoint: ""                # S3-compatible storage (MinIO, R2); default: AWS
  s3_access_key: ""              # static credentials
  s3_secret_key: ""
  s3_region: ""                  # default: inferred

  # Flush thresholds — flush when any threshold is reached
  flush_rows: 10000              # also caps change events held in memory
  materializer_batch_rows: 50000 # change events per materializer step
  flush_interval: 10s            # how often changes are staged

  # How often the materializer commits staged changes to Iceberg
  # (`run` and `materializer-only`)
  materializer_interval: 10s

  # Compaction. Runs as part of every materializer cycle, gated by these
  # thresholds. `target_file_size: 0` disables compaction entirely.
  compaction_data_files: 8
  compaction_delete_files: 4
  target_file_size: 134217728    # 128 MiB

  # `pg2iceberg maintain` (snapshot expiry + orphan cleanup)
  maintenance_retention: 168h    # 7 days. Snapshots older than this are dropped.
  maintenance_grace: 30m         # Orphan files younger than this are protected.
                                 # Cleanup only scans each table's own directory,
                                 # <warehouse>/materialized/<namespace>.<table>/.

  # Free-form REST catalog props passthrough. Layered on top of the
  # built-in props (uri, warehouse, auth, S3, access-delegation header).
  # catalog_props:
  #   "header.X-Custom-Header": "value"

  # Control-plane meta tables. When set, pg2iceberg writes operational
  # telemetry (commits, compactions, maintenance ops, blue-green markers)
  # to Iceberg tables under this namespace.
  # meta_namespace: _pg2iceberg_meta

# Coordinator state
state:
  # Optional dedicated PG for the _pg2iceberg coordinator schema.
  # When empty, the source PG hosts it.
  # postgres_url: postgres://coord_user:secret@coord-db.example.com/coord?sslmode=require
  coordinator_schema: _pg2iceberg
  group: default                 # consumer group name (distributed mode)

metrics_addr: ":9090"            # /metrics, /healthz, /readyz; `off` serves nothing
liveness_timeout: 5m             # /healthz fails once nothing completes for this long
snapshot_only: false             # legacy field; prefer the `snapshot` subcommand
```

## Notes on individual fields

- **`source.logical.snapshot_concurrency` / `snapshot_chunk_pages` / `snapshot_target_file_size`** — not yet exposed; snapshot uses fixed defaults.
- **`sink.flush_rows`** — the most change events pg2iceberg holds in memory before staging them. A transaction bigger than this is staged in chunks of this size as it streams in, and claimed in one step when it commits, so readers never see part of it. Larger values mean fewer, larger staged files.
- **`sink.materializer_batch_rows`** — the most change events the materializer folds into one Iceberg snapshot, which bounds its memory. A transaction larger than this is written as several snapshots committed in one atomic catalog update, so readers of the table never see part of it.
- **`sink.flush_interval` / `sink.materializer_interval`** — the micro-batch cadence: changes are staged every `flush_interval` (or sooner, once `flush_rows` are buffered) and committed to Iceberg every `materializer_interval`. A change typically reaches Iceberg within about the two added together. Shorter intervals mean fresher tables, and more, smaller Iceberg snapshots and files for compaction to merge.
- **`sink.flush_bytes`** — not yet exposed; flush threshold is `flush_rows` + `flush_interval` only.
- **`sink.materializer_target_file_size` / `materializer_concurrency`** — not yet exposed; uses `target_file_size` + a fixed concurrency.
- **`sink.materializer_worker_id`** — replaced by the `--worker-id` flag on `pg2iceberg materializer-only`.
- **`sink.meta_enabled`** — inferred from `meta_namespace`: setting it enables meta-table writes.
- **`metrics_addr`** / **`liveness_timeout`** — where `run`, `stream-only`, `materializer-only` and `snapshot` serve `/metrics`, `/healthz` and `/readyz`, and how long nothing may complete before `/healthz` fails. See [Observability](observability.md).
- **`state.path`** — file-based checkpoint store. Not implemented; coord is always Postgres-backed.
- **`snapshot_only`** — use the `pg2iceberg snapshot` subcommand instead. The field still parses for backward compat.

!!! note "OAuth2 catalog auth"
    Setting `catalog_auth: oauth2` plus `catalog_client_id` / `catalog_client_secret` causes pg2iceberg to forward OAuth2 props to iceberg-rust's REST client (`oauth2-server-uri` derived from `catalog_uri`). Verified end-to-end against the Iceberg REST reference; production deployments against Snowflake / Tabular / Polaris should also work but haven't been integration-tested in CI.

!!! note "Vended credentials"
    `credential_mode: vended` requires the catalog to return per-table S3 credentials in the `loadTable` response config. pg2iceberg automatically sets the `header.x-iceberg-access-delegation: vended-credentials` REST header. See [Polaris](../catalogs/polaris.md) for setup.
