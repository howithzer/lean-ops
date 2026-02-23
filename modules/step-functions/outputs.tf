output "orchestrator_arn" {
  value = aws_sfn_state_machine.pipeline_orchestrator.arn
}

output "rebuilder_arn" {
  value = aws_sfn_state_machine.historical_rebuilder.arn
}
