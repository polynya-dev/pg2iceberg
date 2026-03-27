resource "aws_db_subnet_group" "main" {
  name       = "${var.project}-db-subnet"
  subnet_ids = [aws_subnet.private.id, aws_subnet.private_b.id]

  tags = { Name = "${var.project}-db-subnet" }
}

# Source PostgreSQL — the database pg2iceberg replicates from.
resource "aws_db_parameter_group" "source" {
  name_prefix = "${var.project}-source-"
  family      = "postgres16"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name  = "max_replication_slots"
    value = "4"
  }

  parameter {
    name  = "max_wal_senders"
    value = "4"
  }

  parameter {
    name  = "shared_buffers"
    value = "{DBInstanceClassMemory/4}"
  }

  parameter {
    name  = "max_wal_size"
    value = "4096"
  }

  parameter {
    name  = "checkpoint_timeout"
    value = "1800"
  }

  tags = { Name = "${var.project}-source-params" }
}

resource "aws_db_instance" "source" {
  identifier     = "${var.project}-source"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.source_db_instance_class

  allocated_storage     = var.source_db_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true
  max_allocated_storage = var.source_db_allocated_storage * 2

  db_name  = "bench"
  username = "postgres"
  password = var.source_db_password

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.source.name

  multi_az            = false
  availability_zone   = var.az
  publicly_accessible = false
  skip_final_snapshot = true

  # Performance insights (free tier).
  performance_insights_enabled = true

  tags = { Name = "${var.project}-source" }
}
