resource "aws_cloudwatch_log_group" "ecs_qdrant" {
  count             = var.enable_qdrant ? 1 : 0
  name              = "/ecs/${local.name_prefix}/qdrant"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "ecs_admin" {
  count             = var.enable_admin ? 1 : 0
  name              = "/ecs/${local.name_prefix}/admin"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "ecs_stream" {
  count             = var.enable_msk ? 1 : 0
  name              = "/ecs/${local.name_prefix}/stream-consumer"
  retention_in_days = 14
}
