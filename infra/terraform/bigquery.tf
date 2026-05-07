resource "google_bigquery_dataset" "fraud_detection"{
    dataset_id = var.dataset_id
    project = var.project_id
    location = var.region
}