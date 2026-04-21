---
icon: lucide/library
---

# Catalogs

pg2iceberg writes to any catalog that implements the [Iceberg REST Catalog specification](https://iceberg.apache.org/rest-catalog-spec/). Configuration varies mainly across two dimensions: how the catalog authenticates requests and how S3 credentials are obtained.

## Authentication modes

| `catalog_auth` | Description |
|----------------|-------------|
| `""` / `none` | No authentication — suitable for local development or internal networks |
| `bearer` | Static API token passed as a Bearer header |
| `sigv4` | AWS SigV4 request signing — credentials come from the IAM role attached to the process |
| `oauth2` | OAuth2 client credentials flow — pg2iceberg fetches and refreshes tokens automatically |

## Credential modes

S3 credentials can be supplied in three ways:

| `credential_mode` | Description |
|-------------------|-------------|
| `static` | Access key and secret supplied directly in config (`s3_access_key`, `s3_secret_key`) |
| `iam` | No keys in config — credentials come from the IAM role attached to the process |
| `vended` | Catalog returns temporary, per-table S3 credentials on `LoadTable` — no long-lived keys needed |

## Supported catalogs

| Catalog | Auth | Credentials |
|---------|------|-------------|
| [AWS Glue](glue.md) | `sigv4` | `iam` |
| [S3 Tables](s3-tables.md) | `sigv4` | `iam` |
| [R2 Catalog](r2.md) | `bearer` | `vended` |
| [Lakekeeper](lakekeeper.md) | `bearer` | `vended` or `static` |
| [Polaris](polaris.md) | `oauth2` | `vended` |
| [Iceberg REST](rest.md) | any | any |
