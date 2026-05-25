
# Root Terraform Configuration
# Music Streaming Data Pipeline


terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }

  # Remote state: swap bucket/key per environment via workspace or
  # the environment-specific backend.hcl file supplied at init time.
  backend "s3" {
    bucket         = "music-pipeline-tfstate"
    key            = "pipeline/terraform.tfstate"
    region         = "us-east-1"
    use_lockfile = true
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "MusicStreamingPipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# A random suffix keeps bucket names globally unique across deploys
resource "random_id" "suffix" {
  byte_length = 4
}

# ── S3 
module "s3" {
  source      = "./modules/s3"
  environment = var.environment
  suffix      = random_id.suffix.hex
}

# ── IAM 
module "iam" {
  source              = "./modules/iam"
  environment         = var.environment
  raw_bucket_arn      = module.s3.raw_bucket_arn
  archive_bucket_arn  = module.s3.archive_bucket_arn
  scripts_bucket_arn  = module.s3.scripts_bucket_arn
  dynamodb_table_arns = module.dynamodb.table_arns
}

# ── DynamoDB 
module "dynamodb" {
  source      = "./modules/dynamodb"
  environment = var.environment
}

# ── AWS Glue 
module "glue" {
  source             = "./modules/glue"
  environment        = var.environment
  glue_role_arn      = module.iam.glue_role_arn
  scripts_bucket     = module.s3.scripts_bucket_name
  raw_bucket         = module.s3.raw_bucket_name
  archive_bucket     = module.s3.archive_bucket_name
  songs_table        = module.dynamodb.songs_table_name
  genre_kpis_table   = module.dynamodb.genre_kpis_table_name
  top_songs_table    = module.dynamodb.top_songs_table_name
  top_genres_table   = module.dynamodb.top_genres_table_name
}

# ── Step Functions 
module "step_functions" {
  source                        = "./modules/step_functions"
  environment                   = var.environment
  sfn_role_arn                  = module.iam.sfn_role_arn
  validate_job_name             = module.glue.validate_job_name
  transform_job_name            = module.glue.transform_job_name
  ingest_job_name               = module.glue.ingest_job_name
  archive_job_name              = module.glue.archive_job_name
  raw_bucket                    = module.s3.raw_bucket_name
  archive_bucket                = module.s3.archive_bucket_name
}
