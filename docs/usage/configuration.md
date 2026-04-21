---
icon: lucide/settings
---

# Configuration

pg2iceberg is configured via a YAML file passed with `--config`. Environment variables and CLI flags can override any field — see the [CLI Reference](reference.md) for the full list.

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
    sslmode: disable             # disable | require | verify-ca | verify-full

  # Logical replication mode settings
  logical:
    publication_name: pg2iceberg_pub
    slot_name: pg2iceberg_slot
    snapshot_concurrency: 4      # parallel tables during initial snapshot (default: GOMAXPROCS)
    snapshot_chunk_pages: 2048   # CTID chunk size in pages (~16MB at 8KB/page)
    snapshot_target_file_size:   # target Parquet file size during snapshot (default: 128MB)
    standby_interval: 10s        # how often standby status is sent to PostgreSQL

  # Query mode settings
  query:
    poll_interval: 5s

# Tables to replicate
tables:
  - name: public.orders          # fully-qualified PostgreSQL table name (required)
    iceberg:
      partition:                 # partition transforms, e.g. "day(created_at)", "bucket[16](id)"
        - "day(created_at)"

    # Query mode only
    primary_key: [id]            # required for query mode
    watermark_column: updated_at # required for query mode

# Iceberg sink
sink:
  catalog_uri: ""                # Iceberg REST catalog URL (required)
  catalog_auth: ""               # "" | "sigv4" | "bearer"
  catalog_token: ""              # required when catalog_auth is "bearer"
  catalog_client_id: ""          # OAuth2 client ID
  catalog_client_secret: ""      # OAuth2 client secret

  credential_mode: static        # "static" (default) | "vended" | "iam"
  warehouse: ""                  # required for static credential mode
  namespace: ""                  # Iceberg namespace (required)
  s3_endpoint: ""                # required for static credential mode
  s3_access_key: ""
  s3_secret_key: ""
  s3_region: us-east-1

  # Flush thresholds (logical mode) — flush when any threshold is reached
  flush_rows: 1000
  flush_interval: 10s
  flush_bytes:                   # default: 64MB

  # Materializer
  materializer_interval: 30s
  materializer_worker_id: ""     # set to enable distributed mode
  materializer_target_file_size: # default: 8MB
  materializer_concurrency: 16   # S3 I/O concurrency

  # Compaction thresholds — compact after materialization when exceeded
  compaction_data_files: 20
  compaction_delete_files: 10

  # Target data file size for snapshot and streaming writes
  target_file_size:              # default: 128MB

  # Maintenance (snapshot expiry + orphan cleanup)
  maintenance_retention: 168h    # snapshot retention period (default: 7 days)
  maintenance_interval: 1h
  maintenance_grace: 30m         # orphan file grace period

  # Events table (logical mode only)
  events_partition: day(_ts)     # partition transform for the internal events table

  # Control-plane metadata tables
  meta_enabled: true
  meta_namespace: _pg2iceberg

# Checkpoint / coordinator storage
state:
  postgres_url: ""               # separate PostgreSQL for checkpoint storage (optional)
  path: ""                       # file-based checkpoint store (for local dev)
  coordinator_schema: _pg2iceberg

metrics_addr: :9090
snapshot_only: false             # exit after initial snapshot completes
```

!!! note "`oauth2` catalog auth"
    The config struct has `catalog_client_id` and `catalog_client_secret` fields but the current validation only accepts `""`, `"sigv4"`, and `"bearer"` for `catalog_auth`. If you are using a catalog that requires OAuth2 client credentials (e.g. Polaris), check the [Polaris page](../catalogs/polaris.md) for the current recommended approach.
