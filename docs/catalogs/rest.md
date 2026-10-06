---
icon: lucide/server
---

# Iceberg REST (Generic)

Any catalog that implements the [Iceberg REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/) works with pg2iceberg. This page covers the generic setup and is also the configuration used for local development.

## Local development

The quickstart docker-compose includes an Iceberg REST catalog backed by MinIO:

```yaml
source:
  postgres_url: "postgres://postgres:postgres@localhost:5432/postgres"

sink:
  catalog_uri: "http://localhost:8181"
  catalog_auth: ""
  credential_mode: static
  warehouse: "s3://warehouse/"
  namespace: default
  s3_endpoint: "http://localhost:9000"
  s3_access_key: minioadmin
  s3_secret_key: minioadmin
  s3_region: us-east-1

tables:
  - name: public.orders
```

Or with environment variables alone — every table with a primary key, the S3 keys read by the AWS default credential chain:

```bash
export POSTGRES_URL=postgres://postgres:postgres@localhost:5432/postgres
export ICEBERG_CATALOG_URL=http://localhost:8181
export ICEBERG_WAREHOUSE=s3://warehouse/
export AWS_ENDPOINT_URL_S3=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
pg2iceberg run
```

## Generic configuration reference

Both settings are inferred when left out — see [Configuration](../usage/configuration.md#inferred-settings).

| `catalog_auth` | Use when |
|----------------|----------|
| `""` / `none` | No authentication (local, internal network) |
| `bearer` | Catalog accepts a static API token |
| `sigv4` | Catalog requires AWS SigV4 request signing |
| `oauth2` | Catalog uses OAuth2 client credentials flow |

| `credential_mode` | Use when |
|-------------------|----------|
| `static` | You supply S3 access key + secret directly |
| `iam` | Running on AWS with an IAM role (no keys in config) |
| `vended` | Catalog returns temporary S3 credentials per table |
