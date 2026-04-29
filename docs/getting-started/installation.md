---
icon: lucide/package
---

# Installation

## Docker

The quickest way to run pg2iceberg is with Docker:

```bash
docker pull ghcr.io/pg2iceberg/pg2iceberg:latest
```

## Helm

Deploy to Kubernetes using the official Helm chart:

```bash
helm install pg2iceberg oci://ghcr.io/pg2iceberg/charts/pg2iceberg \
  --values values.yaml
```

## Binary

Pre-built binaries are available on the [GitHub releases page](https://github.com/pg2iceberg/pg2iceberg/releases).

=== "Linux (amd64)"

    ```bash
    curl -L https://github.com/pg2iceberg/pg2iceberg/releases/latest/download/pg2iceberg_linux_amd64.tar.gz | tar xz
    sudo mv pg2iceberg /usr/local/bin/
    ```

=== "Linux (arm64)"

    ```bash
    curl -L https://github.com/pg2iceberg/pg2iceberg/releases/latest/download/pg2iceberg_linux_arm64.tar.gz | tar xz
    sudo mv pg2iceberg /usr/local/bin/
    ```

=== "macOS"

    ```bash
    curl -L https://github.com/pg2iceberg/pg2iceberg/releases/latest/download/pg2iceberg_darwin_arm64.tar.gz | tar xz
    sudo mv pg2iceberg /usr/local/bin/
    ```

## Prerequisites

### PostgreSQL

Logical replication must be enabled. Set the following in `postgresql.conf`:

```ini
wal_level = logical
```

Then create a replication slot and publication for the tables you want to replicate:

```sql
SELECT pg_create_logical_replication_slot('pg2iceberg', 'pgoutput');
CREATE PUBLICATION pg2iceberg FOR TABLE my_table;
```

!!! note
    The replication slot retains WAL until pg2iceberg consumes it. Monitor slot lag to avoid excessive disk usage on the primary.
