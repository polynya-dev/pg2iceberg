---
icon: lucide/package
---

# Installation

## Docker

The repository ships a multi-stage Dockerfile that builds the binary and publishes a slim runtime image. To build locally:

```bash
docker build -t pg2iceberg-rust:dev .
```

Pre-built images are not yet published. Once a release pipeline is in place, the image will be available as:

```bash
docker pull ghcr.io/polynya-dev/pg2iceberg:latest
```

The image's entrypoint is the `pg2iceberg` binary; pass a subcommand:

```bash
docker run --rm -v $(pwd)/config.yaml:/etc/pg2iceberg/config.yaml \
  pg2iceberg-rust:dev run --config /etc/pg2iceberg/config.yaml
```

## Build from source

Requires Rust 1.85+ (matching `rust-toolchain.toml`).

```bash
git clone https://github.com/polynya-dev/pg2iceberg.git
cd pg2iceberg/pg2iceberg-rust
cargo build --release --bin pg2iceberg --features prod
./target/release/pg2iceberg --help
```

The default build is sim-only (no S3, no real PG client). The `prod` feature wires the real backends (`tokio-postgres-rustls`, `iceberg-rust` REST catalog, `object_store` for S3, `iceberg-storage-opendal` for catalog-side IO).

## Helm

Not yet published.

## Prerequisites

### PostgreSQL

Logical replication must be enabled. Set the following in `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

pg2iceberg auto-creates the replication slot and publication on first run; you don't need to create them manually. If you do want to pre-create:

```sql
SELECT pg_create_logical_replication_slot('pg2iceberg_slot', 'pgoutput');
CREATE PUBLICATION pg2iceberg_pub FOR TABLE my_schema.my_table;
```

Each replicated table needs a primary key. Tables without one fail at startup with a clear error pointing at the missing column.

!!! note
    The replication slot retains WAL until pg2iceberg consumes it. Monitor slot lag to avoid excessive disk usage on the primary. The startup validation refuses to start if `slot.wal_status = lost` (PG 13+) — at that point the slot is unrecoverable and you must `pg2iceberg cleanup` and re-snapshot.

### Local development on macOS (Colima)

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) and need a Docker daemon. On macOS we recommend [Colima](https://github.com/abiosoft/colima):

```bash
brew install colima
colima start

export DOCKER_HOST=unix://${HOME}/.colima/default/docker.sock
export TESTCONTAINERS_RYUK_DISABLED=true   # Colima doesn't run Ryuk reliably
```

Then:

```bash
cargo test --features integration -- --test-threads=1
```

Single-threaded execution is required because the integration tests share a single PG container per test binary; parallelism races on slot/publication names.
