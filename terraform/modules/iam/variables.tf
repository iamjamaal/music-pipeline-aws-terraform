variable "environment"         { type = string }
variable "raw_bucket_arn"      { type = string }
variable "archive_bucket_arn"  { type = string }
variable "scripts_bucket_arn"  { type = string }
variable "dynamodb_table_arns" { type = list(string) }
