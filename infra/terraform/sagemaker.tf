# SageMaker Pipeline registration.
# The pipeline definition is built by infra/sagemaker_pipeline.py (Python SDK)
# and registered/updated via the AWS CLI in a null_resource. We rerun on any
# change to the script, role, image, or buckets — so `terraform apply` keeps it
# in sync.

resource "aws_sagemaker_model_package_group" "main" {
  model_package_group_name        = "${local.name_prefix}-models"
  model_package_group_description = "Trained poker collusion-detection models."
}

resource "null_resource" "register_sagemaker_pipeline" {
  triggers = {
    role_arn        = aws_iam_role.sagemaker_execution.arn
    image_uri       = local.ecr_image_uri
    data_bucket     = aws_s3_bucket.buckets["data"].bucket
    models_bucket   = aws_s3_bucket.buckets["models"].bucket
    code_bucket     = aws_s3_bucket.buckets["code"].bucket
    pipeline_script = filemd5("${path.module}/../sagemaker_pipeline.py")
    pipeline_name   = "${local.name_prefix}-train"
  }

  provisioner "local-exec" {
    working_dir = "${path.module}/.."
    command     = <<-EOT
      python sagemaker_pipeline.py \
        --pipeline-name "${local.name_prefix}-train" \
        --role-arn "${aws_iam_role.sagemaker_execution.arn}" \
        --image-uri "${local.ecr_image_uri}" \
        --data-bucket "${aws_s3_bucket.buckets["data"].bucket}" \
        --models-bucket "${aws_s3_bucket.buckets["models"].bucket}" \
        --code-bucket "${aws_s3_bucket.buckets["code"].bucket}" \
        --region "${var.region}" \
        --upsert
    EOT
  }
}
