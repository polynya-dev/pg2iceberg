resource "aws_ecr_repository" "pg2iceberg" {
  name                 = "${var.project}/pg2iceberg"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = local.tags
}

resource "aws_ecr_repository" "shim" {
  name                 = "${var.project}/shim"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = local.tags
}
