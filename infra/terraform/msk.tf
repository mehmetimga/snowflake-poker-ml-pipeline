resource "aws_msk_serverless_cluster" "main" {
  count        = var.enable_msk ? 1 : 0
  cluster_name = "${local.name_prefix}-msk"

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.msk[0].id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
