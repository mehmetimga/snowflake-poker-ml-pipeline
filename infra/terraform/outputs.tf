output "account_id" {
  value = local.account_id
}

output "region" {
  value = var.region
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.poker_ml.repository_url
  description = "Push BYOC images here."
}

output "ecr_image_uri" {
  value = local.ecr_image_uri
}

output "s3_buckets" {
  value = { for k, b in aws_s3_bucket.buckets : k => b.bucket }
}

output "sagemaker_execution_role_arn" {
  value = aws_iam_role.sagemaker_execution.arn
}

output "sagemaker_pipeline_name" {
  value = "${local.name_prefix}-train"
}

output "msk_bootstrap_brokers" {
  value       = var.enable_msk ? aws_msk_serverless_cluster.main[0].bootstrap_brokers_sasl_iam : null
  description = "Use with SASL_SSL + AWS_MSK_IAM."
}

output "admin_url" {
  value       = var.enable_admin ? "http://${aws_lb.admin[0].dns_name}" : null
  description = "Streamlit admin URL."
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "private_subnet_ids" {
  value = module.vpc.private_subnets
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}
