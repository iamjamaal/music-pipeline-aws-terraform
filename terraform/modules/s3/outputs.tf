output "raw_bucket_name"     { value = aws_s3_bucket.raw.id }
output "raw_bucket_arn"      { value = aws_s3_bucket.raw.arn }
output "archive_bucket_name" { value = aws_s3_bucket.archive.id }
output "archive_bucket_arn"  { value = aws_s3_bucket.archive.arn }
output "scripts_bucket_name" { value = aws_s3_bucket.scripts.id }
output "scripts_bucket_arn"  { value = aws_s3_bucket.scripts.arn }
