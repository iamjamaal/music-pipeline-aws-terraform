
# Root Outputs – handy references after apply


output "raw_bucket_name" {
  description = "S3 bucket that receives incoming streaming batch files."
  value       = module.s3.raw_bucket_name
}

output "archive_bucket_name" {
  description = "S3 bucket where processed files are archived."
  value       = module.s3.archive_bucket_name
}

output "scripts_bucket_name" {
  description = "S3 bucket holding all Glue PySpark / Python Shell scripts."
  value       = module.s3.scripts_bucket_name
}

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine."
  value       = module.step_functions.state_machine_arn
}

output "genre_kpis_table" {
  description = "DynamoDB table for daily genre-level KPIs."
  value       = module.dynamodb.genre_kpis_table_name
}

output "top_songs_table" {
  description = "DynamoDB table for top-3 songs per genre per day."
  value       = module.dynamodb.top_songs_table_name
}

output "top_genres_table" {
  description = "DynamoDB table for top-5 genres per day."
  value       = module.dynamodb.top_genres_table_name
}
