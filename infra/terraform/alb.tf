resource "aws_lb" "admin" {
  count              = var.enable_admin ? 1 : 0
  name               = "${local.name_prefix}-admin"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb[0].id]
  subnets            = module.vpc.public_subnets
}

resource "aws_lb_target_group" "admin" {
  count       = var.enable_admin ? 1 : 0
  name        = "${local.name_prefix}-admin-tg"
  port        = 8501
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = module.vpc.vpc_id

  health_check {
    path                = "/_stcore/health"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "admin" {
  count             = var.enable_admin ? 1 : 0
  load_balancer_arn = aws_lb.admin[0].arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.admin[0].arn
  }
}
