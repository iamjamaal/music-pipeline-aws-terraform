# ============================================================
# Step Functions Module
# ============================================================
# Provisions the state machine that orchestrates all four
# Glue stages.  The ASL definition is loaded from the repo's
# step_functions/state_machine.json and uses templatefile()
# so Terraform can inject the environment-specific Glue job
# names at plan time rather than hard-coding them.

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/music-pipeline-${var.environment}"
  retention_in_days = 90
}

resource "aws_sns_topic" "pipeline_alerts" {
  name = "music-pipeline-alerts-${var.environment}"
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "music-pipeline-${var.environment}"
  role_arn = var.sfn_role_arn
  type     = "STANDARD"  # STANDARD supports sync Glue integration

  # Load the ASL from the shared JSON file and inject resource names
  definition = templatefile("${path.module}/../../../step_functions/state_machine.json", {
    validate_job_name  = var.validate_job_name
    transform_job_name = var.transform_job_name
    ingest_job_name    = var.ingest_job_name
    archive_job_name   = var.archive_job_name
    archive_bucket     = var.archive_bucket
    environment        = var.environment
    sns_topic_arn      = aws_sns_topic.pipeline_alerts.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration { enabled = true }

  tags = {
    Name  = "music-pipeline-${var.environment}"
    Stage = "orchestration"
  }
}

# ── EventBridge Rule: auto-trigger on new S3 object ───────
# When a new stream file lands in raw/streams/, EventBridge
# fires this rule which starts a new Step Functions execution
# with the S3 event data as input.
resource "aws_cloudwatch_event_rule" "s3_trigger" {
  name        = "music-pipeline-s3-trigger-${var.environment}"
  description = "Triggers pipeline when a CSV lands in raw/streams/"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.raw_bucket] }
      object = { key = [{ prefix = "streams/" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  rule     = aws_cloudwatch_event_rule.s3_trigger.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = var.sfn_role_arn

  # Transform the EventBridge event into the payload our SFN expects
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = "{\"raw_bucket\": \"<bucket>\", \"file_key\": \"<key>\"}"
  }
}
