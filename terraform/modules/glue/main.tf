# ============================================================
# Glue Module
# ============================================================
# Four Glue jobs implement the ETL stages:
#
#   1. validate   (Python Shell) – column presence checks
#   2. transform  (PySpark)      – KPI computation
#   3. ingest     (Python Shell) – DynamoDB writes
#   4. archive    (Python Shell) – move processed files to archive/
#
# All scripts are uploaded from the scripts bucket by CI/CD
# before Step Functions triggers a new execution.

locals {
  prefix = "music-pipeline"
}

# ── Glue Catalog Database ──────────────────────────────────
resource "aws_glue_catalog_database" "main" {
  name        = "${local.prefix}_${var.environment}"
  description = "Music streaming pipeline catalog – ${var.environment}"
}

# ── Job 1: Validate ────────────────────────────────────────
# Python Shell (no Spark cluster) is sufficient for header checks.
resource "aws_glue_job" "validate" {
  name              = "${local.prefix}-validate-${var.environment}"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  max_capacity      = 0.0625   # 1/16 DPU – cheapest Python Shell tier

  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket}/glue_jobs/validate.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"          = "true"
    "--raw_bucket"              = var.raw_bucket
    "--TempDir"                 = "s3://${var.scripts_bucket}/tmp/"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Stage = "validate" }
}

# ── Job 2: Transform (PySpark) ─────────────────────────────
# Reads streams + songs catalog, joins, computes KPIs.
resource "aws_glue_job" "transform" {
  name         = "${local.prefix}-transform-${var.environment}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"
  number_of_workers = 2          # 2 G.1X workers = 8 vCPU, 16 GB RAM
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_jobs/transform.py"
    python_version  = "3"  # glueetl uses "3" not "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.scripts_bucket}/spark-logs/"
    "--raw_bucket"                       = var.raw_bucket
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Stage = "transform" }
}

# ── Job 3: Ingest to DynamoDB ──────────────────────────────
# Python Shell: reads the Parquet output of transform, writes to DDB.
resource "aws_glue_job" "ingest" {
  name         = "${local.prefix}-ingest-${var.environment}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"
  max_capacity = 0.0625

  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket}/glue_jobs/ingest.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--raw_bucket"                       = var.raw_bucket
    "--genre_kpis_table"                 = var.genre_kpis_table
    "--top_songs_table"                  = var.top_songs_table
    "--top_genres_table"                 = var.top_genres_table
    "--songs_table"                      = var.songs_table
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Stage = "ingest" }
}

# ── Job 4: Archive ─────────────────────────────────────────
# Python Shell: copies processed files from raw/ to archive/
resource "aws_glue_job" "archive" {
  name         = "${local.prefix}-archive-${var.environment}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"
  max_capacity = 0.0625

  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket}/glue_jobs/archive.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--raw_bucket"                       = var.raw_bucket
    "--archive_bucket"                   = var.archive_bucket
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Stage = "archive" }
}

# ── CloudWatch Log Groups (pre-created so CI/CD can tail them) ─
resource "aws_cloudwatch_log_group" "glue_validate" {
  name              = "/aws-glue/jobs/${local.prefix}-validate-${var.environment}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_transform" {
  name              = "/aws-glue/jobs/${local.prefix}-transform-${var.environment}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_ingest" {
  name              = "/aws-glue/jobs/${local.prefix}-ingest-${var.environment}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_archive" {
  name              = "/aws-glue/jobs/${local.prefix}-archive-${var.environment}"
  retention_in_days = 30
}
