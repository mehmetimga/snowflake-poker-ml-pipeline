module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${local.name_prefix}-vpc"
  cidr = var.vpc_cidr

  azs             = var.azs
  private_subnets = [for i, az in var.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, az in var.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  enable_nat_gateway      = true
  single_nat_gateway      = true
  enable_dns_hostnames    = true
  enable_dns_support      = true
  map_public_ip_on_launch = false
}

resource "aws_security_group" "sagemaker" {
  name        = "${local.name_prefix}-sagemaker"
  description = "SageMaker training/processing jobs - egress only."
  vpc_id      = module.vpc.vpc_id

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "ecs_tasks" {
  name        = "${local.name_prefix}-ecs-tasks"
  description = "Fargate tasks for stream-consumer, qdrant, admin."
  vpc_id      = module.vpc.vpc_id

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "alb" {
  count       = var.enable_admin ? 1 : 0
  name        = "${local.name_prefix}-alb"
  description = "ALB for Streamlit admin."
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "HTTP from allowed CIDRs"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.admin_allowed_cidrs
  }

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "ecs_from_alb" {
  count                    = var.enable_admin ? 1 : 0
  type                     = "ingress"
  from_port                = 8501
  to_port                  = 8501
  protocol                 = "tcp"
  security_group_id        = aws_security_group.ecs_tasks.id
  source_security_group_id = aws_security_group.alb[0].id
  description              = "Streamlit admin from ALB"
}

resource "aws_security_group_rule" "ecs_qdrant_self" {
  count             = var.enable_qdrant ? 1 : 0
  type              = "ingress"
  from_port         = 6333
  to_port           = 6334
  protocol          = "tcp"
  security_group_id = aws_security_group.ecs_tasks.id
  self              = true
  description       = "Qdrant from sibling tasks in same SG"
}

resource "aws_security_group_rule" "ecs_efs_self" {
  count             = var.enable_qdrant ? 1 : 0
  type              = "ingress"
  from_port         = 2049
  to_port           = 2049
  protocol          = "tcp"
  security_group_id = aws_security_group.ecs_tasks.id
  self              = true
  description       = "NFS for EFS mount from Qdrant task"
}

resource "aws_security_group" "msk" {
  count       = var.enable_msk ? 1 : 0
  name        = "${local.name_prefix}-msk"
  description = "MSK Serverless cluster."
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Kafka IAM from ECS tasks"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  ingress {
    description     = "Kafka IAM from SageMaker"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.sagemaker.id]
  }

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
