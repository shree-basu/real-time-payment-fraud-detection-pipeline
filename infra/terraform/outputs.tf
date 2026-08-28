output "deployment_gate_open" {
  description = "True only when both deliberate deployment gates are supplied."
  value       = local.deploy
}

output "input_subscription" {
  value = try(google_pubsub_subscription.dataflow[0].id, null)
}

output "invalid_event_topic" {
  value = try(google_pubsub_topic.invalid_events[0].id, null)
}

output "sink_error_topic" {
  value = try(google_pubsub_topic.sink_errors[0].id, null)
}

output "dataflow_artifacts_bucket" {
  value = try(google_storage_bucket.dataflow_artifacts[0].url, null)
}

output "dataflow_worker_service_account" {
  value = try(google_service_account.dataflow_worker[0].email, null)
}

output "bigquery_tables" {
  value = local.deploy ? {
    raw      = "${var.project_id}:${google_bigquery_dataset.fraud[0].dataset_id}.${google_bigquery_table.raw_transactions[0].table_id}"
    fraud    = "${var.project_id}:${google_bigquery_dataset.fraud[0].dataset_id}.${google_bigquery_table.fraud_alerts[0].table_id}"
    velocity = "${var.project_id}:${google_bigquery_dataset.fraud[0].dataset_id}.${google_bigquery_table.velocity_alerts[0].table_id}"
  } : null
}
