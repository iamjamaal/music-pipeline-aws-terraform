# ============================================================
# IAM Module
# ============================================================
# Two principals need permissions:
#   1. AWS Glue  – reads S3, writes S3, writes DynamoDB, logs
#   2. Step Functions – starts Glue jobs, reads their status
#
# The principle of least privilege is enforced: Glue only gets
# the specific DynamoDB tables and S3 buckets it owns, and
# Step Functions only gets the Glue actions it needs.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ── Glue IAM Role ─────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "music-pipeline-glue-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS-managed base policy for Glue (CloudWatch Logs, EC2 metadata, etc.)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom least-privilege policy for our resources
resource "aws_iam_role_policy" "glue_custom" {
  name = "music-pipeline-glue-custom-${var.environment}"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # S3: read raw files + read/write scripts + write archive
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation"
        ]
        Resource = [
          var.raw_bucket_arn,     "${var.raw_bucket_arn}/*",
          var.archive_bucket_arn, "${var.archive_bucket_arn}/*",
          var.scripts_bucket_arn, "${var.scripts_bucket_arn}/*",
        ]
      },
      # DynamoDB: read + write to the four KPI/catalog tables
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem", "dynamodb:BatchWriteItem",
          "dynamodb:GetItem",  "dynamodb:Query",
          "dynamodb:Scan",     "dynamodb:UpdateItem",
          "dynamodb:DescribeTable"
        ]
        Resource = var.dynamodb_table_arns
      },
      # CloudWatch Logs – structured job logging
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
      }
    ]
  })
}

# ── Step Functions IAM Role ────────────────────────────────
resource "aws_iam_role" "sfn" {
  name = "music-pipeline-sfn-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_custom" {
  name = "music-pipeline-sfn-custom-${var.environment}"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Start and monitor Glue jobs
      {
        Sid    = "GlueJobControl"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun", "glue:GetJobRun",
          "glue:GetJobRuns",  "glue:BatchStopJobRun"
        ]
        Resource = "arn:aws:glue:${local.region}:${local.account_id}:job/music-pipeline-*"
      },
      # Emit execution events to CloudWatch for observability
      {
        Sid    = "CloudWatchEvents"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery", "logs:GetLogDelivery",
          "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries", "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      # Allow SFN to publish SNS alerts on failure (optional alarm step)
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = "arn:aws:sns:${local.region}:${local.account_id}:music-pipeline-*"
      }
    ]
  })
}
