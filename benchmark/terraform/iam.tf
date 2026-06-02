data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: pull from ECR + ship logs to CloudWatch. Shared by all
# task definitions.
resource "aws_iam_role" "execution" {
  name_prefix        = "${local.name}-exec-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# pg2iceberg task role: Glue Iceberg REST catalog (SigV4) + read/write on
# the S3 warehouse bucket. credential_mode=iam means the role writes data
# files to S3 directly (no vended credentials).
data "aws_iam_policy_document" "pg2iceberg" {
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetCatalog",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion",
    ]
    resources = ["*"]
  }
  statement {
    sid = "WarehouseBucket"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.warehouse.arn,
      "${aws_s3_bucket.warehouse.arn}/*",
    ]
  }
}

resource "aws_iam_role" "pg2iceberg" {
  name_prefix        = "${local.name}-pg2i-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "pg2iceberg" {
  name_prefix = "${local.name}-pg2i-"
  role        = aws_iam_role.pg2iceberg.id
  policy      = data.aws_iam_policy_document.pg2iceberg.json
}

# Shim task role: no AWS API access needed (only talks to RDS). Logs go
# through the execution role.
resource "aws_iam_role" "shim" {
  name_prefix        = "${local.name}-shim-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = local.tags
}
