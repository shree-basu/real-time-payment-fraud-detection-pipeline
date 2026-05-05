resource "google_pubsub_topic" "fraud_transactions"{
    name = "fraud-transactions"
    project = var.project_id
    message_retention_duration = "86600s"
}

resource "google_pubsub_subscription" "dataflow_sub"{
    name = "fraud-transactions-dataflow-sub"
    topic = google_pubsub_topic.fraud_transactions.name
    project = var.project_id
    ack_deadline_seconds = 60
    message_retention_duration = "600s"
 
}