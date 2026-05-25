variable "environment"             { type = string }
variable "sfn_role_arn"            { type = string }
variable "eventbridge_role_arn"    { type = string }
variable "validate_job_name"       { type = string }
variable "transform_job_name"      { type = string }
variable "ingest_job_name"         { type = string }
variable "archive_job_name"        { type = string }
variable "catalog_ingest_job_name" { type = string }
variable "raw_bucket"              { type = string }
variable "archive_bucket"          { type = string }

variable "sns_email_subscribers" {
  description = "List of email addresses to subscribe to pipeline failure alerts."
  type        = list(string)
  default     = []
}
