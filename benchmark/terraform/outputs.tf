output "region" {
  value = var.region
}

output "ecr_pg2iceberg" {
  value = aws_ecr_repository.pg2iceberg.repository_url
}

output "ecr_bench_writer" {
  value = aws_ecr_repository.bench_writer.repository_url
}

output "ecr_bench_verify" {
  value = aws_ecr_repository.bench_verify.repository_url
}

output "ecs_cluster" {
  value = aws_ecs_cluster.main.name
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "security_group_id" {
  value = aws_security_group.ecs.id
}

output "source_db_endpoint" {
  value = aws_db_instance.source.endpoint
}

output "s3_bucket" {
  value = aws_s3_bucket.warehouse.id
}

output "glue_catalog_uri" {
  value = local.glue_catalog_uri
}

output "pg2iceberg_task_def" {
  value = aws_ecs_task_definition.pg2iceberg.arn
}

output "bench_writer_task_def" {
  value = aws_ecs_task_definition.bench_writer.arn
}

output "bench_verify_task_def" {
  value = aws_ecs_task_definition.bench_verify.arn
}
