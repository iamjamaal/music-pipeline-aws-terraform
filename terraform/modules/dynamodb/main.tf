# ============================================================
# DynamoDB Module
# ============================================================
# Four tables store distinct KPI shapes.  Each uses a
# composite key (partition key + sort key) so downstream apps
# can do O(1) lookups without full table scans.
#
# Key-design rationale:
#   genre_kpis    PK=genre_date "acoustic#2024-06-25"  SK=metric
#   top_songs     PK=genre_date "acoustic#2024-06-25"  SK=rank
#   top_genres    PK=date "2024-06-25"                  SK=rank
#   songs_catalog PK=track_id  (reference / lookup table)
#
# PAY_PER_REQUEST billing suits irregular write patterns.

locals {
  prefix = "music-pipeline-${var.environment}"
}

resource "aws_dynamodb_table" "genre_kpis" {
  name         = "${local.prefix}-genre-kpis"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "genre_date"
  range_key    = "metric"

  attribute {
    name = "genre_date"
    type = "S"
  }

  attribute {
    name = "metric"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-genre-kpis" }
}

resource "aws_dynamodb_table" "top_songs" {
  name         = "${local.prefix}-top-songs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "genre_date"
  range_key    = "rank"

  attribute {
    name = "genre_date"
    type = "S"
  }

  attribute {
    name = "rank"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-top-songs" }
}

resource "aws_dynamodb_table" "top_genres" {
  name         = "${local.prefix}-top-genres"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "date"
  range_key    = "rank"

  attribute {
    name = "date"
    type = "S"
  }

  attribute {
    name = "rank"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-top-genres" }
}

resource "aws_dynamodb_table" "songs_catalog" {
  name         = "${local.prefix}-songs-catalog"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "track_id"

  attribute {
    name = "track_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-songs-catalog" }
}
