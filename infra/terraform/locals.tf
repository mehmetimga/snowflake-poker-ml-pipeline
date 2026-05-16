data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  name_prefix = "${var.project}-${var.env}"

  ecr_image_uri = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com/${aws_ecr_repository.poker_ml.name}:${var.byoc_image_tag}"

  s3_buckets = {
    data   = "${local.name_prefix}-data-${random_id.bucket_suffix.hex}"
    models = "${local.name_prefix}-models-${random_id.bucket_suffix.hex}"
    code   = "${local.name_prefix}-code-${random_id.bucket_suffix.hex}"
    logs   = "${local.name_prefix}-logs-${random_id.bucket_suffix.hex}"
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}
