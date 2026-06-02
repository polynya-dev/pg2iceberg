# AWS Glue Iceberg REST catalog + a plain S3 warehouse bucket. pg2iceberg
# authenticates to Glue with SigV4 (credential_mode=iam) and writes data
# files straight to S3 using the task role — no credential vending.

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.project}-wh-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = local.tags
}

# Glue database backing the Iceberg namespace (Glue REST maps namespace
# -> database). Pre-created so pg2iceberg doesn't depend on CreateDatabase
# behavior at startup.
resource "aws_glue_catalog_database" "this" {
  name = var.namespace
  # Base S3 location for Iceberg table data in this namespace; Glue
  # assigns each table a location under it (same bucket pg2iceberg's
  # blob store writes to).
  location_uri = "s3://${aws_s3_bucket.warehouse.bucket}/warehouse/"
}
