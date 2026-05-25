output "state_machine_arn"  { value = aws_sfn_state_machine.pipeline.arn }
output "state_machine_name" { value = aws_sfn_state_machine.pipeline.name }
output "sns_alerts_arn"     { value = aws_sns_topic.pipeline_alerts.arn }
