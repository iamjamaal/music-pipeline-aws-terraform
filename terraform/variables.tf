
# Root-level Variables
# https://github.com/iamjamaal/music-pipeline-aws-terraform


variable "aws_region" {
  description = "AWS region where all resources will be provisioned."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "sns_email_subscribers" {
  description = "Email addresses to receive pipeline failure alerts via SNS."
  type        = list(string)
  default     = []
}
