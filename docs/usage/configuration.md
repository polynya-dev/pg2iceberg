---
icon: lucide/settings
---

# Configuration

pg2iceberg is configured via a YAML file passed with `--config`. Environment variables override individual fields. Subcommands accept additional flags — see [`pg2iceberg --help`](reference.md) for the full surface.

## Full reference

```yaml
# Replication source
source:
  mode: logical                  # "logical" (default) or "query"
  postgres:
    host: ""                     # required
    port: 5432
    database: ""                 # required
    user: ""                     # required
    password: ""
    sslmode: disable             # "disable" | "require" | "verify-ca" | "verify-full"
                                 # — currently anything non-disable enables
                                 #   webpki-roots verification; finer-grained
                                 #   modes (mTLS, custom CA) are follow-ons.

  # Logical replication mode settings
  logical:
    publication_name: pg2iceberg_pub
    slot_name: pg2iceberg_slot
    standby_interval: 10s        # how often the standby_status ack is sent

  # Query mode settings
  query:
    poll_interval: 30s

# Tables to replicate
tables:
  - name: public.orders          # fully-qualified PostgreSQL table name (required)
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

    # Query mode only
    # watermark_column: updated_at

# Iceberg sink
sink:
  catalog_uri: ""                # Iceberg REST catalog URL (required)
  catalog_auth: ""               # "" | "bearer" | "oauth2" | "sigv4"
  catalog_token: ""              # required when catalog_auth = "bearer"
  catalog_client_id: ""          # OAuth2 client ID
  catalog_client_secret: ""      # OAuth2 client secret

  credential_mode: static        # "static" (default) | "iam" | "vended"
  warehouse: ""                  # s3://bucket/prefix (required for static/iam)
  namespace: ""                  # Iceberg namespace (required)
  s3_endpoint: ""                # required for credential_mode=static
  s3_access_key: ""              # required for credential_mode=static
  s3_secret_key: ""              # required for credential_mode=static
  s3_region: us-east-1

  # Flush thresholds (logical mode) — flush when any threshold is reached
  flush_rows: 1000
  flush_interval: 10s            # default standby cadence

  # Materializer cycle (only consulted by `pg2iceberg materializer-only`)
  materializer_interval: 10s

  # Compaction. Runs as part of every materializer cycle, gated by these
  # thresholds. `target_file_size: 0` disables compaction entirely.
  compaction_data_files: 8
  compaction_delete_files: 4
  target_file_size: 134217728    # 128 MiB

  # `pg2iceberg maintain` (snapshot expiry + orphan cleanup)
  maintenance_retention: 168h    # 7 days. Snapshots older than this are dropped.
  maintenance_grace: 30m         # Orphan files younger than this are protected.
  materialized_prefix: materialized/   # blob path prefix the materializer writes to;
                                       # orphan-cleanup scans here

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

metrics_addr: ":9090"            # parsed but not yet wired (Prometheus endpoint TODO)
snapshot_only: false             # legacy field; prefer the `snapshot` subcommand
```

## Notes on individual fields

- **`source.logical.snapshot_concurrency` / `snapshot_chunk_pages` / `snapshot_target_file_size`** — not yet exposed; snapshot uses fixed defaults.
- **`sink.flush_bytes`** — not yet exposed; flush threshold is `flush_rows` + `flush_interval` only.
- **`sink.materializer_target_file_size` / `materializer_concurrency`** — not yet exposed; uses `target_file_size` + a fixed concurrency.
- **`sink.materializer_worker_id`** — replaced by the `--worker-id` flag on `pg2iceberg materializer-only`.
- **`sink.meta_enabled`** — inferred from `meta_namespace`: setting it enables meta-table writes.
- **`metrics_addr`** — parsed but not yet wired (Prometheus endpoint TODO).
- **`state.path`** — file-based checkpoint store. Not implemented; coord is always Postgres-backed.
- **`snapshot_only`** — use the `pg2iceberg snapshot` subcommand instead. The field still parses for backward compat.

## Override precedence

```
defaults < YAML < environment variables < CLI flags
```

Common environment variables (full list in [reference.md](reference.md)):

| Env var | YAML field |
|---|---|
| `POSTGRES_URL` | `source.postgres.dsn` (parsed into host/port/database/user/password) |
| `TABLES` | `tables` (comma-separated, qualified `schema.table`) |
| `MODE` | `source.mode` |
| `SLOT_NAME` | `source.logical.slot_name` |
| `PUBLICATION_NAME` | `source.logical.publication_name` |
| `ICEBERG_CATALOG_URL` | `sink.catalog_uri` |
| `WAREHOUSE` | `sink.warehouse` |
| `NAMESPACE` | `sink.namespace` |
| `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` | `sink.s3_*` |
| `STATE_POSTGRES_URL` | `state.postgres_url` |

!!! note "OAuth2 catalog auth"
    Setting `catalog_auth: oauth2` plus `catalog_client_id` / `catalog_client_secret` causes pg2iceberg to forward OAuth2 props to iceberg-rust's REST client (`oauth2-server-uri` derived from `catalog_uri`). Verified end-to-end against the Iceberg REST reference; production deployments against Snowflake / Tabular / Polaris should also work but haven't been integration-tested in CI.

!!! note "Vended credentials"
    `credential_mode: vended` requires the catalog to return per-table S3 credentials in the `loadTable` response config. pg2iceberg automatically sets the `header.x-iceberg-access-delegation: vended-credentials` REST header. See [Polaris](../catalogs/polaris.md) for setup.
