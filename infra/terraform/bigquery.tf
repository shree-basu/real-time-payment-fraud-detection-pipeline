resource "google_bigquery_dataset" "fraud_detection"{
    dataset_id = var.dataset_id
    project = var.project_id
    location = var.region
}
resource "google_bigquery_table" "raw_transactions"{
    dataset_id = google_bigquery_dataset.fraud_detection.dataset_id
    table_id = "raw_transactions"
    project = var.project_id

    schema = jsonencode([
        {name = "transaction_id" , type = "STRING"},
        {name = "account_id" , type = "STRING"},
        {name = "amount" , type = "FLOAT"},
        {name = "currency" , type = "STRING"},
        {name = "merchant_category" , type = "STRING"},
        {name = "timestamp" , type = "TIMESTAMP"},
        {name = "country_code" , type = "STRING"},
        {name = "is_online" , type = "BOOLEAN"},
        {name = "hour_of_day" , type = "INTEGER"},
        {name = "is_high_risk_country" , type = "BOOLEAN"},
        {name = "has_ip_address" , type = "BOOLEAN"},
        {name = "fraud_signals" , type = "STRING"},
        {name = "risk_score" , type = "INTEGER"},
        {name = "is_fraud_alert" , type = "BOOLEAN"},
        {name = "processed_at" , type = "TIMESTAMP"}
        ])
}