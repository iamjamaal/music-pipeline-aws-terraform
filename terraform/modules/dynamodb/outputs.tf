output "genre_kpis_table_name"  { value = aws_dynamodb_table.genre_kpis.name }
output "top_songs_table_name"   { value = aws_dynamodb_table.top_songs.name }
output "top_genres_table_name"  { value = aws_dynamodb_table.top_genres.name }
output "songs_table_name"       { value = aws_dynamodb_table.songs_catalog.name }

# Collected list used by the IAM module to build a least-privilege policy
output "table_arns" {
  value = [
    aws_dynamodb_table.genre_kpis.arn,
    aws_dynamodb_table.top_songs.arn,
    aws_dynamodb_table.top_genres.arn,
    aws_dynamodb_table.songs_catalog.arn,
  ]
}
