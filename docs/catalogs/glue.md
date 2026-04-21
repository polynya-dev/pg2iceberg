---
icon: lucide/cloud
---

# AWS Glue

AWS Glue acts as an Iceberg REST catalog via its native Iceberg endpoint. Authentication uses SigV4 — no API keys in config; credentials come from the IAM role attached to the process.

## Prerequisites

- An AWS Glue catalog in the target region
- IAM permissions for the role running pg2iceberg:

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetDatabase",
    "glue:CreateDatabase",
    "glue:GetTable",
    "glue:CreateTable",
    "glue:UpdateTable",
    "glue:GetTableVersions",
    "glue:DeleteTableVersion",
    "glue:BatchDeleteTableVersion"
  ],
  "Resource": "*"
}
```

- S3 read/write access on the bucket used for Iceberg data files

## Configuration

```yaml
source:
  postgres_url: "postgres://user:pass@host:5432/db?sslmode=disable"
  publication: pg2iceberg
  slot: pg2iceberg

sink:
  catalog_uri: "https://glue.us-east-1.amazonaws.com/iceberg"
  catalog_auth: sigv4
  credential_mode: iam
  warehouse: "s3://my-bucket/warehouse/"
  namespace: default
  s3_region: us-east-1

tables:
  - name: public.orders
```

| Field | Value |
|-------|-------|
| `catalog_uri` | `https://glue.{region}.amazonaws.com/iceberg` |
| `catalog_auth` | `sigv4` |
| `credential_mode` | `iam` |
| `s3_region` | Must match the Glue catalog region |

## Credential chain

With `credential_mode: iam`, pg2iceberg uses the AWS SDK default credential chain in order:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. IAM role (EC2 instance profile, ECS task role, IRSA for Kubernetes)
3. `~/.aws/credentials` file

For production, attach an IAM role to the compute instance rather than using environment variables.

!!! warning "No vended credentials"
    Glue does not support vended credentials. The IAM role must have direct S3 access to the data bucket.
