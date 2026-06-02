resource "aws_db_subnet_group" "this" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
  tags       = local.tags
}

# rds.logical_replication = 1 sets wal_level=logical (the RDS-managed way
# to enable logical decoding). The slot/sender counts give pg2iceberg
# headroom. Static params require a reboot, which the instance does on
# first create.
resource "aws_db_parameter_group" "this" {
  name_prefix = "${local.name}-"
  family      = "postgres${var.db_engine_version}"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }
  # Allow non-SSL connections. pg2iceberg's Rust TLS verifies against
  # webpki public roots and can't validate the Amazon RDS CA, so it
  # connects with sslmode=disable; traffic stays inside the private VPC.
  parameter {
    name         = "rds.force_ssl"
    value        = "0"
    apply_method = "immediate"
  }

  tags = local.tags
  lifecycle { create_before_destroy = true }
}

resource "aws_db_instance" "this" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = var.db_engine_version
  instance_class = var.db_instance_class

  allocated_storage = var.db_allocated_storage
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.this.name
  publicly_accessible    = false

  multi_az            = false
  skip_final_snapshot = true
  deletion_protection = false
  apply_immediately   = true

  performance_insights_enabled = true

  tags = local.tags
}
