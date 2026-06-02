locals {
  pg2iceberg_image = "${aws_ecr_repository.pg2iceberg.repository_url}:${var.image_tag}"
  shim_image       = "${aws_ecr_repository.shim.repository_url}:${var.image_tag}"

  # Env for the pg2iceberg config template (rendered by the image's
  # entrypoint via envsubst).
  pg2iceberg_env = [
    { name = "PG_HOST", value = aws_db_instance.this.address },
    { name = "PG_PORT", value = tostring(aws_db_instance.this.port) },
    { name = "PG_DATABASE", value = var.db_name },
    { name = "PG_USER", value = var.db_username },
    { name = "PG_PASSWORD", value = random_password.db.result },
    { name = "REGION", value = var.region },
    { name = "WAREHOUSE", value = "s3://${aws_s3_bucket.warehouse.bucket}/warehouse/" },
    { name = "GLUE_CATALOG_ID", value = data.aws_caller_identity.current.account_id },
    { name = "NAMESPACE", value = var.namespace },
    { name = "FLUSH_INTERVAL", value = var.flush_interval },
    { name = "FLUSH_ROWS", value = tostring(var.flush_rows) },
    { name = "RUST_LOG", value = var.rust_log },
  ]

  postgres_url = "postgres://${var.db_username}:${random_password.db.result}@${aws_db_instance.this.address}:${aws_db_instance.this.port}/${var.db_name}?sslmode=require"
}

resource "aws_ecs_cluster" "this" {
  name = local.name
  tags = local.tags
}

resource "aws_cloudwatch_log_group" "pg2iceberg" {
  name              = "/ecs/${local.name}/pg2iceberg"
  retention_in_days = 7
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "shim" {
  name              = "/ecs/${local.name}/shim"
  retention_in_days = 7
  tags              = local.tags
}

# ── pg2iceberg ────────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "pg2iceberg" {
  family                   = "${local.name}-pg2iceberg"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.pg2iceberg_cpu
  memory                   = var.pg2iceberg_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.pg2iceberg.arn

  # ARM64 (Graviton) so images build natively on Apple Silicon — no
  # QEMU emulation, no cross-compile, and cheaper than x86.
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name        = "pg2iceberg"
    image       = local.pg2iceberg_image
    essential   = true
    command     = ["run", "--config", "/tmp/config.yaml"]
    environment = local.pg2iceberg_env
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.pg2iceberg.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "pg2iceberg"
      }
    }
  }])

  tags = local.tags
}

resource "aws_ecs_service" "pg2iceberg" {
  name            = "pg2iceberg"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.pg2iceberg.arn
  desired_count   = var.pg2iceberg_desired_count
  launch_type     = "FARGATE"

  # pg2iceberg holds a single Postgres replication slot, so two instances
  # can never run at once. Stop the old task before starting the new one
  # (no rolling overlap) — otherwise the new task fails with
  # "replication slot is active".
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.pg2iceberg.id]
    assign_public_ip = false
  }

  # `make seed` scales this to 1 out-of-band; don't let TF revert it.
  lifecycle {
    ignore_changes = [desired_count]
  }

  tags = local.tags
}

# ── shim ──────────────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "shim" {
  family                   = "${local.name}-shim"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.shim_cpu
  memory                   = var.shim_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.shim.arn

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name         = "shim"
    image        = local.shim_image
    essential    = true
    portMappings = [{ containerPort = 8080, protocol = "tcp" }]
    environment = [
      { name = "POSTGRES_URL", value = local.postgres_url },
      { name = "PORT", value = "8080" },
      { name = "COORD_SCHEMA", value = "_pg2iceberg" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.shim.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "shim"
      }
    }
  }])

  tags = local.tags
}

resource "aws_ecs_service" "shim" {
  name            = "shim"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.shim.arn
  desired_count   = var.shim_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.shim.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.shim.arn
    container_name   = "shim"
    container_port   = 8080
  }

  depends_on = [aws_lb_listener.http]
  tags       = local.tags
}

# ── ALB ───────────────────────────────────────────────────────────────
resource "aws_lb" "shim" {
  name               = local.name
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]
  tags               = local.tags
}

resource "aws_lb_target_group" "shim" {
  name        = local.name
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = aws_vpc.this.id
  target_type = "ip"

  health_check {
    path                = "/healthz"
    matcher             = "200"
    interval            = 15
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.shim.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.shim.arn
  }
  tags = local.tags
}
