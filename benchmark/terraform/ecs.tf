# ECR repositories.
resource "aws_ecr_repository" "pg2iceberg" {
  name                 = "${var.project}/pg2iceberg"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "bench_writer" {
  name                 = "${var.project}/bench-writer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "bench_verify" {
  name                 = "${var.project}/bench-verify"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

# ECS cluster.
resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM role for ECS task execution (pulling images, logging).
resource "aws_iam_role" "ecs_execution" {
  name = "${var.project}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM role for ECS tasks (S3 + Glue access).
resource "aws_iam_role" "ecs_task" {
  name = "${var.project}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_s3" {
  name = "${var.project}-s3-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
      ]
      Resource = [
        aws_s3_bucket.warehouse.arn,
        "${aws_s3_bucket.warehouse.arn}/*",
      ]
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_glue" {
  name = "${var.project}-glue-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
      ]
      Resource = ["*"]
    }]
  })
}

# CloudWatch log group.
resource "aws_cloudwatch_log_group" "main" {
  name              = "/ecs/${var.project}"
  retention_in_days = 7
}

# --- Task Definitions ---

locals {
  glue_catalog_uri = "https://glue.${var.region}.amazonaws.com/iceberg"
}

resource "aws_ecs_task_definition" "pg2iceberg" {
  family                   = "${var.project}-pg2iceberg"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.pg2iceberg_cpu
  memory                   = var.pg2iceberg_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "pg2iceberg"
    image     = "${aws_ecr_repository.pg2iceberg.repository_url}:latest"
    essential = true

    entryPoint = ["sh", "-c"]
    command = [join("\n", [
      "cat > /tmp/config.yaml << 'CFGEOF'",
      "tables:",
      "  - name: public.bench_events",
      "    iceberg:",
      "      partition:",
      "        - truncate[100000](seq)",
      "source:",
      "  mode: logical",
      "  postgres:",
      "    host: ${aws_db_instance.source.address}",
      "    port: 5432",
      "    database: bench",
      "    user: postgres",
      "    password: ${var.source_db_password}",
      "  logical:",
      "    publication_name: pg2iceberg_bench",
      "    slot_name: pg2iceberg_bench_slot",
      "sink:",
      "  catalog_uri: ${local.glue_catalog_uri}",
      "  catalog_auth: sigv4",
      "  warehouse: s3://${aws_s3_bucket.warehouse.id}/",
      "  namespace: benchmark",
      "  s3_region: ${var.region}",
      "  flush_interval: 10s",
      "  flush_rows: 50000",
      "  flush_bytes: 67108864",
      "state: {}",
      "metrics_addr: \":9090\"",
      "CFGEOF",
      "exec pg2iceberg --config /tmp/config.yaml",
    ])]

    portMappings = [{
      containerPort = 9090
      protocol      = "tcp"
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "pg2iceberg"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "bench_writer" {
  family                   = "${var.project}-bench-writer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.bench_writer_cpu
  memory                   = var.bench_writer_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "bench-writer"
    image     = "${aws_ecr_repository.bench_writer.repository_url}:latest"
    essential = true

    environment = [
      { name = "DATABASE_URL", value = "postgres://postgres:${var.source_db_password}@${aws_db_instance.source.address}:5432/bench" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "bench-writer"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "bench_verify" {
  family                   = "${var.project}-bench-verify"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.bench_verify_cpu
  memory                   = var.bench_verify_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "bench-verify"
    image     = "${aws_ecr_repository.bench_verify.repository_url}:latest"
    essential = true

    environment = [
      { name = "DATABASE_URL", value = "postgres://postgres:${var.source_db_password}@${aws_db_instance.source.address}:5432/bench" },
      { name = "S3_REGION", value = var.region },
      { name = "CATALOG_URI", value = local.glue_catalog_uri },
      { name = "CATALOG_AUTH", value = "sigv4" },
      { name = "WAREHOUSE", value = "s3://${aws_s3_bucket.warehouse.id}/" },
      { name = "NAMESPACE", value = "benchmark" },
      { name = "TABLE", value = "bench_events" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "bench-verify"
      }
    }
  }])
}
