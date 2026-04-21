---
icon: lucide/table
---

# AWS S3 Tables

S3 Tables is AWS's managed Iceberg service — tables are stored directly in a dedicated S3 table bucket with the Iceberg REST catalog API built in. Authentication is SigV4, same as Glue.

## Prerequisites

- An S3 table bucket created in your AWS account
- IAM permissions for the role running pg2iceberg:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3tables:GetTableBucket",
    "s3tables:GetNamespace",
    "s3tables:CreateNamespace",
    "s3tables:GetTable",
    "s3tables:CreateTable",
    "s3tables:UpdateTableMetadataLocation",
    "s3tables:GetTableMetadataLocation"
  ],
  "Resource": "*"
}
```

## Configuration

```yaml
source:
  postgres_url: "postgres://user:pass@host:5432/db?sslmode=disable"
  publication: pg2iceberg
  slot: pg2iceberg

sink:
  catalog_uri: "https://s3tables.us-east-1.amazonaws.com/iceberg"
  catalog_auth: sigv4
  credential_mode: iam
  warehouse: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"
  namespace: default
  s3_region: us-east-1

tables:
  - name: public.orders
```

| Field | Value |
|-------|-------|
| `catalog_uri` | `https://s3tables.{region}.amazonaws.com/iceberg` |
| `catalog_auth` | `sigv4` |
| `credential_mode` | `iam` |
| `warehouse` | Table bucket ARN: `arn:aws:s3tables:{region}:{account-id}:bucket/{bucket-name}` |
| `s3_region` | Must match the table bucket region |

## Credential chain

With `credential_mode: iam`, pg2iceberg uses the AWS SDK default credential chain in order:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. IAM role (EC2 instance profile, ECS task role, IRSA for Kubernetes)
3. `~/.aws/credentials` file

!!! note
    S3 Tables manages data storage internally — you do not supply an S3 endpoint or access keys.
