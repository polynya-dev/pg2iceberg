variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "az" {
  description = "Single availability zone for all resources"
  type        = string
  default     = "us-east-1a"
}

variable "project" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "pg2iceberg-bench"
}

# RDS source database.
variable "source_db_instance_class" {
  description = "RDS instance class for the source PostgreSQL"
  type        = string
  default     = "db.r6g.xlarge"
}

variable "source_db_allocated_storage" {
  description = "Allocated storage in GB for source RDS"
  type        = number
  default     = 100
}

variable "source_db_password" {
  description = "Password for the source PostgreSQL"
  type        = string
  sensitive   = true
  default     = "benchpassword123"
}

# ECS.
variable "pg2iceberg_cpu" {
  description = "CPU units for pg2iceberg task (1024 = 1 vCPU)"
  type        = number
  default     = 1024
}

variable "pg2iceberg_memory" {
  description = "Memory in MB for pg2iceberg task"
  type        = number
  default     = 2048
}

variable "bench_writer_cpu" {
  description = "CPU units for bench-writer task"
  type        = number
  default     = 2048
}

variable "bench_writer_memory" {
  description = "Memory in MB for bench-writer task"
  type        = number
  default     = 4096
}

variable "bench_verify_cpu" {
  description = "CPU units for bench-verify task"
  type        = number
  default     = 2048
}

variable "bench_verify_memory" {
  description = "Memory in MB for bench-verify task"
  type        = number
  default     = 4096
}

# Benchmark config.
variable "seed_size" {
  description = "Data size to seed (e.g. 10GB)"
  type        = string
  default     = "10GB"
}

variable "stream_rate" {
  description = "Rows per second for streaming scenarios"
  type        = number
  default     = 1000
}

variable "stream_duration" {
  description = "Duration for streaming scenarios"
  type        = string
  default     = "15m"
}
