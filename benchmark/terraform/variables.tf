variable "region" {
  description = "AWS region (Glue is available in all commercial regions)."
  type        = string
  default     = "ap-southeast-1"
}

variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "pg2iceberg-bench"
}

variable "allowed_cidr" {
  description = <<-EOT
    CIDR allowed to reach the shim's public ALB. Defaults to open
    (0.0.0.0/0) so anyone can run it out of the box — STRONGLY recommend
    setting this to "<your-ip>/32" since the shim has no auth.
  EOT
  type        = string
  default     = "0.0.0.0/0"
}

# ── RDS ───────────────────────────────────────────────────────────────
variable "db_instance_class" {
  type    = string
  default = "db.m6g.large"
}

variable "db_allocated_storage" {
  type    = string
  default = 100
}

variable "db_engine_version" {
  type    = string
  default = "16"
}

variable "db_name" {
  type    = string
  default = "rideshare"
}

variable "db_username" {
  type    = string
  default = "benchadmin"
}

# ── Glue / Iceberg ────────────────────────────────────────────────────
variable "namespace" {
  description = "Iceberg namespace (Glue database) created for the tables."
  type        = string
  default     = "rideshare"
}

# ── Fargate ───────────────────────────────────────────────────────────
variable "pg2iceberg_cpu" {
  type    = number
  default = 1024
}

variable "pg2iceberg_memory" {
  type    = number
  default = 2048
}

variable "shim_cpu" {
  type    = number
  default = 1024
}

variable "shim_memory" {
  type    = number
  default = 2048
}

variable "shim_desired_count" {
  description = "Number of shim tasks behind the ALB. Scale up to push more write load."
  type        = number
  default     = 2
}

variable "pg2iceberg_desired_count" {
  description = <<-EOT
    Desired pg2iceberg tasks. Defaults to 0 so the service does not
    crash-loop before the schema is seeded; `make seed` scales it to 1.
    The service ignores out-of-band desired_count changes (lifecycle).
  EOT
  type        = number
  default     = 0
}

# ── pg2iceberg tuning ─────────────────────────────────────────────────
variable "flush_interval" {
  type    = string
  default = "10s"
}

variable "flush_rows" {
  type    = number
  default = 1000
}

variable "rust_log" {
  description = "RUST_LOG for the pg2iceberg task."
  type        = string
  default     = "info,pg2iceberg=debug,pg2iceberg_logical=debug,pg2iceberg_stream=debug,pg2iceberg_pg=debug"
}

variable "image_tag" {
  description = "Tag used for both ECR images (set by the Makefile, usually a git sha or 'latest')."
  type        = string
  default     = "latest"
}
