locals {
  # Resource existence follows durable desired state, never the confirmation token.
  # Clearing the token while enabled fails validation instead of planning destruction.
  deploy = var.deployment_enabled
  required_apis = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
  ])
  worker_project_roles = toset([
    "roles/bigquery.jobUser",
    "roles/dataflow.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])
}

check "intentional_deployment" {
  assert {
    condition     = !var.deployment_enabled || var.deployment_confirmation == "DEPLOY"
    error_message = "Cloud resources require both deployment_enabled=true and confirmation DEPLOY."
  }
  assert {
    condition     = !local.deploy || length(trimspace(var.project_id)) > 0
    error_message = "project_id is required when the deliberate deployment gate is open."
  }
}

resource "google_project_service" "required" {
  for_each = local.deploy ? local.required_apis : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_pubsub_topic" "transactions" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-transactions"
  message_retention_duration = "604800s"
  labels                     = var.labels
  depends_on                 = [google_project_service.required]
}

resource "google_pubsub_subscription" "dataflow" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-dataflow"
  topic                      = google_pubsub_topic.transactions[0].id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = false
  labels                     = var.labels

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_topic" "invalid_events" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-invalid-events"
  message_retention_duration = "604800s"
  labels                     = var.labels
  depends_on                 = [google_project_service.required]
}

resource "google_pubsub_subscription" "invalid_replay" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-invalid-replay"
  topic                      = google_pubsub_topic.invalid_events[0].id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = true
  labels                     = var.labels

  expiration_policy {
    ttl = ""
  }
}

resource "google_pubsub_topic" "sink_errors" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-sink-errors"
  message_retention_duration = "604800s"
  labels                     = var.labels
  depends_on                 = [google_project_service.required]
}

resource "google_pubsub_subscription" "sink_error_replay" {
  count = local.deploy ? 1 : 0

  name                       = "${var.name_prefix}-sink-error-replay"
  topic                      = google_pubsub_topic.sink_errors[0].id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = true
  labels                     = var.labels

  expiration_policy {
    ttl = ""
  }
}

resource "google_storage_bucket" "dataflow_artifacts" {
  count = local.deploy ? 1 : 0

  name                        = "${var.project_id}-${var.name_prefix}-artifacts"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = var.labels

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      age = 14
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "fraud" {
  count = local.deploy ? 1 : 0

  dataset_id                 = "fraud_detection"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false
  max_time_travel_hours      = 168
  labels                     = var.labels
  depends_on                 = [google_project_service.required]

  lifecycle {
    prevent_destroy = true
  }
}

locals {
  transaction_schema = file("${path.module}/../../pipeline/schemas/bigquery_transaction.json")
  velocity_schema    = file("${path.module}/../../pipeline/schemas/bigquery_velocity.json")
}

resource "google_bigquery_table" "raw_transactions" {
  count = local.deploy ? 1 : 0

  dataset_id               = google_bigquery_dataset.fraud[0].dataset_id
  table_id                 = "raw_transactions"
  schema                   = local.transaction_schema
  deletion_protection      = true
  require_partition_filter = true
  clustering               = ["account_id", "merchant_category"]
  labels                   = var.labels

  time_partitioning {
    type  = "DAY"
    field = "event_date"
  }
}

resource "google_bigquery_table" "fraud_alerts" {
  count = local.deploy ? 1 : 0

  dataset_id               = google_bigquery_dataset.fraud[0].dataset_id
  table_id                 = "fraud_alerts"
  schema                   = local.transaction_schema
  deletion_protection      = true
  require_partition_filter = true
  clustering               = ["account_id", "risk_score"]
  labels                   = var.labels

  time_partitioning {
    type  = "DAY"
    field = "event_date"
  }
}

resource "google_bigquery_table" "velocity_alerts" {
  count = local.deploy ? 1 : 0

  dataset_id               = google_bigquery_dataset.fraud[0].dataset_id
  table_id                 = "account_velocity_alerts"
  schema                   = local.velocity_schema
  deletion_protection      = true
  require_partition_filter = true
  clustering               = ["account_id"]
  labels                   = var.labels

  time_partitioning {
    type  = "DAY"
    field = "window_date"
  }
}

resource "google_service_account" "dataflow_worker" {
  count = local.deploy ? 1 : 0

  account_id   = "fraud-dataflow-worker"
  display_name = "Fraud Dataflow worker"
  description  = "Least-privilege worker identity for the streaming reference pipeline"
  depends_on   = [google_project_service.required]
}

resource "google_project_iam_custom_role" "tracking_subscription" {
  count = local.deploy ? 1 : 0

  project     = var.project_id
  role_id     = "fraudTrackingSubscription"
  title       = "Fraud Dataflow tracking subscription"
  description = "Create, consume, and delete Dataflow event-time tracking subscriptions"
  permissions = [
    "pubsub.subscriptions.create",
    "pubsub.subscriptions.consume",
    "pubsub.subscriptions.delete",
  ]
  depends_on = [google_project_service.required]
}

resource "google_project_iam_custom_role" "tracking_topic_attachment" {
  count = local.deploy ? 1 : 0

  project     = var.project_id
  role_id     = "fraudTrackingTopicAttach"
  title       = "Fraud Dataflow tracking topic attachment"
  description = "Attach the Dataflow event-time tracking subscription to the input topic"
  permissions = ["pubsub.topics.attachSubscription"]
  depends_on  = [google_project_service.required]
}

resource "google_project_iam_member" "worker_project_roles" {
  for_each = local.deploy ? local.worker_project_roles : toset([])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_project_iam_member" "worker_tracking_subscription" {
  count = local.deploy ? 1 : 0

  project = var.project_id
  role    = google_project_iam_custom_role.tracking_subscription[0].name
  member  = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_pubsub_topic_iam_member" "worker_tracking_topic_attachment" {
  count = local.deploy ? 1 : 0

  topic  = google_pubsub_topic.transactions[0].name
  role   = google_project_iam_custom_role.tracking_topic_attachment[0].name
  member = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_pubsub_topic_iam_member" "worker_invalid_publisher" {
  count = local.deploy ? 1 : 0

  topic  = google_pubsub_topic.invalid_events[0].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_pubsub_topic_iam_member" "worker_sink_error_publisher" {
  count = local.deploy ? 1 : 0

  topic  = google_pubsub_topic.sink_errors[0].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_storage_bucket_iam_member" "worker_artifact_access" {
  count = local.deploy ? 1 : 0

  bucket = google_storage_bucket.dataflow_artifacts[0].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker[0].email}"
}

resource "google_bigquery_dataset_access" "worker_writer" {
  count = local.deploy ? 1 : 0

  dataset_id    = google_bigquery_dataset.fraud[0].dataset_id
  role          = "WRITER"
  user_by_email = google_service_account.dataflow_worker[0].email
}

resource "google_monitoring_alert_policy" "subscription_backlog" {
  count = local.deploy && var.enable_monitoring ? 1 : 0

  display_name          = "${var.name_prefix}: sustained Pub/Sub backlog"
  combiner              = "OR"
  enabled               = true
  notification_channels = var.notification_channel_ids

  conditions {
    display_name = "Undelivered messages above 10,000 for 10 minutes"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.label.subscription_id = \"${google_pubsub_subscription.dataflow[0].name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = 10000
      duration        = "600s"

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "oldest_unacked_age" {
  count = local.deploy && var.enable_monitoring ? 1 : 0

  display_name          = "${var.name_prefix}: oldest unacked message age"
  combiner              = "OR"
  enabled               = true
  notification_channels = var.notification_channel_ids

  conditions {
    display_name = "Oldest unacked message above 10 minutes"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\" AND resource.label.subscription_id = \"${google_pubsub_subscription.dataflow[0].name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = 600
      duration        = "300s"

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
      }
    }
  }
}
