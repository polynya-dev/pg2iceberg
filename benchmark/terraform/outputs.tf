output "alb_url" {
  description = "Base URL the local k6 + scripts target."
  value       = "http://${aws_lb.shim.dns_name}"
}

output "ecr_pg2iceberg" {
  value = aws_ecr_repository.pg2iceberg.repository_url
}

output "ecr_shim" {
  value = aws_ecr_repository.shim.repository_url
}

output "cluster" {
  value = aws_ecs_cluster.this.name
}

output "pg2iceberg_service" {
  value = aws_ecs_service.pg2iceberg.name
}

output "pg2iceberg_task_definition" {
  value = aws_ecs_task_definition.pg2iceberg.family
}

output "warehouse_bucket" {
  value = aws_s3_bucket.warehouse.bucket
}

output "namespace" {
  value = var.namespace
}

output "rds_endpoint" {
  description = "RDS address (private; for debugging from inside the VPC)."
  value       = aws_db_instance.this.address
}

output "region" {
  value = var.region
}

output "private_subnets" {
  value = aws_subnet.private[*].id
}

output "pg2iceberg_security_group" {
  value = aws_security_group.pg2iceberg.id
}

output "db_password" {
  sensitive = true
  value     = random_password.db.result
}
