resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_service_discovery_private_dns_namespace" "main" {
  count       = var.enable_qdrant ? 1 : 0
  name        = "${local.name_prefix}.local"
  description = "Service discovery for Fargate tasks."
  vpc         = module.vpc.vpc_id
}

# ---------- Qdrant ----------
resource "aws_service_discovery_service" "qdrant" {
  count = var.enable_qdrant ? 1 : 0
  name  = "qdrant"

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.main[0].id
    routing_policy = "MULTIVALUE"

    dns_records {
      ttl  = 10
      type = "A"
    }
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

resource "aws_ecs_task_definition" "qdrant" {
  count                    = var.enable_qdrant ? 1 : 0
  family                   = "${local.name_prefix}-qdrant"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  volume {
    name = "qdrant-storage"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.qdrant[0].id
      transit_encryption = "ENABLED"

      authorization_config {
        access_point_id = aws_efs_access_point.qdrant_storage[0].id
        iam             = "DISABLED"
      }
    }
  }

  container_definitions = jsonencode([
    {
      name      = "qdrant"
      image     = "qdrant/qdrant:v1.12.4"
      essential = true
      portMappings = [
        { containerPort = 6333, protocol = "tcp" },
        { containerPort = 6334, protocol = "tcp" },
      ]
      mountPoints = [
        { sourceVolume = "qdrant-storage", containerPath = "/qdrant/storage", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_qdrant[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "qdrant"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "qdrant" {
  count           = var.enable_qdrant ? 1 : 0
  name            = "qdrant"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.qdrant[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.qdrant[0].arn
  }

  depends_on = [aws_efs_mount_target.qdrant]
}

# ---------- Streamlit admin ----------
resource "aws_ecs_task_definition" "admin" {
  count                    = var.enable_admin ? 1 : 0
  family                   = "${local.name_prefix}-admin"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "admin"
      image     = local.ecr_image_uri
      essential = true
      command   = ["streamlit", "run", "admin/Home.py", "--server.address=0.0.0.0", "--server.port=8501"]
      portMappings = [
        { containerPort = 8501, protocol = "tcp" }
      ]
      environment = concat([
        { name = "WAREHOUSE_BACKEND", value = "duckdb" },
        { name = "DUCKDB_PATH", value = "/tmp/warehouse.duckdb" },
        { name = "DUCKDB_S3_BUCKET", value = aws_s3_bucket.buckets["data"].bucket },
        { name = "DUCKDB_S3_PREFIX", value = "warehouse/" },
        { name = "MODELS_DIR", value = "/tmp/models" },
        { name = "MODELS_S3_BUCKET", value = aws_s3_bucket.buckets["models"].bucket },
        { name = "AWS_REGION", value = var.region },
        ], var.enable_qdrant ? [
        { name = "QDRANT_URL", value = "http://qdrant.${local.name_prefix}.local:6333" },
      ] : [])
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_admin[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "admin"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "admin" {
  count           = var.enable_admin ? 1 : 0
  name            = "admin"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.admin[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.admin[0].arn
    container_name   = "admin"
    container_port   = 8501
  }

  depends_on = [aws_lb_listener.admin]
}

# ---------- Stream consumer (Kafka → S3 parquet) ----------
resource "aws_ecs_task_definition" "stream_consumer" {
  count                    = var.enable_msk ? 1 : 0
  family                   = "${local.name_prefix}-stream-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "stream-consumer"
      image     = local.ecr_image_uri
      essential = true
      command   = ["python", "scripts/stream.py", "--batch-size", "200"]
      environment = [
        { name = "WAREHOUSE_BACKEND", value = "duckdb" },
        { name = "DUCKDB_PATH", value = "/tmp/warehouse.duckdb" },
        { name = "DUCKDB_S3_BUCKET", value = aws_s3_bucket.buckets["data"].bucket },
        { name = "DUCKDB_S3_PREFIX", value = "warehouse/" },
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_serverless_cluster.main[0].bootstrap_brokers_sasl_iam },
        { name = "KAFKA_SECURITY_PROTOCOL", value = "SASL_SSL" },
        { name = "KAFKA_SASL_MECHANISM", value = "AWS_MSK_IAM" },
        { name = "KAFKA_HANDS_TOPIC", value = "hands.raw" },
        { name = "AWS_REGION", value = var.region },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_stream[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "stream"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "stream_consumer" {
  count           = var.enable_msk ? 1 : 0
  name            = "stream-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.stream_consumer[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }
}
