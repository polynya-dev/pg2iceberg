---
icon: lucide/layers
---

# Lakekeeper

[Lakekeeper](https://lakekeeper.io) is an open-source Iceberg REST catalog. It supports vended credentials and can be self-hosted or used as a managed service.

## Configuration

```yaml
source:
  postgres_url: "postgres://user:pass@host:5432/db?sslmode=disable"
  publication: pg2iceberg
  slot: pg2iceberg

sink:
  catalog_uri: "https://lakekeeper.example.com"
  catalog_auth: bearer
  catalog_token: "${LAKEKEEPER_TOKEN}"
  credential_mode: vended
  namespace: default

tables:
  - name: public.orders
```

| Field | Value |
|-------|-------|
| `catalog_uri` | Your Lakekeeper instance URL |
| `catalog_auth` | `bearer` |
| `catalog_token` | Lakekeeper access token |
| `credential_mode` | `vended` (if your Lakekeeper is configured with vended credentials) or `static` |

## Vended vs static credentials

If your Lakekeeper instance is configured to vend S3 credentials (e.g. via an AWS IAM role or assumed role), use `credential_mode: vended` and omit S3 config.

If Lakekeeper is not configured for vended credentials, use `credential_mode: static` and provide S3 config directly:

```yaml
sink:
  catalog_uri: "https://lakekeeper.example.com"
  catalog_auth: bearer
  catalog_token: "${LAKEKEEPER_TOKEN}"
  credential_mode: static
  warehouse: "s3://my-bucket/warehouse/"
  namespace: default
  s3_endpoint: "https://s3.us-east-1.amazonaws.com"
  s3_access_key: "${S3_ACCESS_KEY}"
  s3_secret_key: "${S3_SECRET_KEY}"
  s3_region: us-east-1
```
