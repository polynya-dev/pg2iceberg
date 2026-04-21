---
icon: simple/snowflake
---

# Polaris

[Polaris](https://github.com/apache/polaris) is an open-source Iceberg catalog from Snowflake. It uses OAuth2 client credentials for authentication and supports vended S3 credentials per table.

## Prerequisites

- A Polaris catalog with a principal and principal role configured
- Client ID and secret from the Polaris service connection

## Configuration

```yaml
source:
  postgres_url: "postgres://user:pass@host:5432/db?sslmode=disable"
  publication: pg2iceberg
  slot: pg2iceberg

sink:
  catalog_uri: "https://polaris.example.com"
  catalog_auth: oauth2
  catalog_client_id: "${POLARIS_CLIENT_ID}"
  catalog_client_secret: "${POLARIS_CLIENT_SECRET}"
  credential_mode: vended
  namespace: default

tables:
  - name: public.orders
```

| Field | Value |
|-------|-------|
| `catalog_uri` | Your Polaris instance URL |
| `catalog_auth` | `oauth2` |
| `catalog_client_id` | Principal client ID from Polaris |
| `catalog_client_secret` | Principal client secret from Polaris |
| `credential_mode` | `vended` |

## How OAuth2 works

pg2iceberg fetches a token from `{catalog_uri}/v1/oauth/tokens` using the `client_credentials` grant. The token is cached and refreshed automatically 5 minutes before expiry.

## Vended credentials

With `credential_mode: vended`, pg2iceberg requests temporary S3 credentials scoped to each table on `LoadTable`. The `warehouse`, `s3_endpoint`, `s3_access_key`, and `s3_secret_key` fields are not needed.

!!! note "Snowflake-managed Polaris"
    If you are using Snowflake's managed Polaris (Open Catalog), the `catalog_uri` is your Snowflake account URL and the client credentials come from a service connection in the Snowflake console.
