resource "aws_efs_file_system" "qdrant" {
  count          = var.enable_qdrant ? 1 : 0
  creation_token = "${local.name_prefix}-qdrant"
  encrypted      = true

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }
}

resource "aws_efs_mount_target" "qdrant" {
  count           = var.enable_qdrant ? length(module.vpc.private_subnets) : 0
  file_system_id  = aws_efs_file_system.qdrant[0].id
  subnet_id       = module.vpc.private_subnets[count.index]
  security_groups = [aws_security_group.ecs_tasks.id]
}

resource "aws_efs_access_point" "qdrant_storage" {
  count          = var.enable_qdrant ? 1 : 0
  file_system_id = aws_efs_file_system.qdrant[0].id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/qdrant"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "0755"
    }
  }
}
