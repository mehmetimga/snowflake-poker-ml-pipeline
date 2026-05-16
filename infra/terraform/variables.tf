variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-west-2"
}

variable "env" {
  description = "Environment name (dev/staging/prod). Used as a suffix on most resources."
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project slug used in resource names."
  type        = string
  default     = "poker-ml"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "azs" {
  description = "Availability zones to deploy into."
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b"]
}

variable "byoc_image_tag" {
  description = "Tag to use for the BYOC container image. Bump to force ECS to redeploy."
  type        = string
  default     = "latest"
}

variable "admin_allowed_cidrs" {
  description = "CIDR blocks allowed to hit the Streamlit admin ALB. Default open — restrict in prod."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enable_msk" {
  description = "Whether to provision MSK Serverless (Kafka). Disable to save ~$100/mo when not testing streaming."
  type        = bool
  default     = true
}

variable "enable_qdrant" {
  description = "Whether to provision Qdrant on Fargate + EFS."
  type        = bool
  default     = true
}

variable "enable_admin" {
  description = "Whether to provision the Streamlit admin on Fargate + ALB."
  type        = bool
  default     = true
}
