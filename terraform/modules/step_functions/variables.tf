variable "environment"        { type = string }
variable "sfn_role_arn"       { type = string }
variable "validate_job_name"  { type = string }
variable "transform_job_name" { type = string }
variable "ingest_job_name"    { type = string }
variable "archive_job_name"   { type = string }
variable "raw_bucket"         { type = string }
variable "archive_bucket"     { type = string }
