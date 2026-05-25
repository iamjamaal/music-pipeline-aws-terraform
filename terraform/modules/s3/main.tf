# ============================================================
# S3 Module
# ============================================================
# Three buckets serve distinct roles:
#   raw/      – landing zone for incoming stream CSV files
#   archive/  – processed files moved here after the pipeline
#   scripts/  – Glue job Python scripts uploaded by CI/CD

locals {
  raw_bucket_name     = "music-pipeline-raw-${var.environment}-${var.suffix}"
  archive_bucket_name = "music-pipeline-archive-${var.environment}-${var.suffix}"
  scripts_bucket_name = "music-pipeline-scripts-${var.environment}-${var.suffix}"
}

# ── Raw Bucket ─────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: automatically transition old raw files to Glacier
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-old-raw"
    status = "Enabled"
    filter { prefix = "streams/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }
}

# S3 Event Notification – triggers EventBridge so we can later
# wire an EventBridge rule → Step Functions execution.
resource "aws_s3_bucket_notification" "raw" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}

# ── Archive Bucket ─────────────────────────────────────────
resource "aws_s3_bucket" "archive" {
  bucket        = local.archive_bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Scripts Bucket ─────────────────────────────────────────
resource "aws_s3_bucket" "scripts" {
  bucket        = local.scripts_bucket_name
  force_destroy = true   # CI/CD always re-uploads scripts
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Pre-create the folder prefixes so Glue knows where to look
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.raw.id
  key     = "streams/"
  content = ""
}

resource "aws_s3_object" "songs_prefix" {
  bucket  = aws_s3_bucket.raw.id
  key     = "songs/"
  content = ""
}

resource "aws_s3_object" "users_prefix" {
  bucket  = aws_s3_bucket.raw.id
  key     = "users/"
  content = ""
}

resource "aws_s3_object" "archive_prefix" {
  bucket  = aws_s3_bucket.archive.id
  key     = "streams/"
  content = ""
}
