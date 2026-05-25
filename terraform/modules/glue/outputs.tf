output "validate_job_name"  { value = aws_glue_job.validate.name }
output "transform_job_name" { value = aws_glue_job.transform.name }
output "ingest_job_name"    { value = aws_glue_job.ingest.name }
output "archive_job_name"   { value = aws_glue_job.archive.name }
output "catalog_db_name"    { value = aws_glue_catalog_database.main.name }
