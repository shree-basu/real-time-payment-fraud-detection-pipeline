"""BigQuery schemas and explicit sink-row normalization."""

COMMON_FIELDS = [
    {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "account_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "NUMERIC", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "merchant_category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "country_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "is_online", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "hour_of_day", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "is_high_risk_country", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "has_ip_address", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "fraud_signals", "type": "STRING", "mode": "REPEATED"},
    {"name": "risk_score", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "is_fraud_alert", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
RAW_SCHEMA = {"fields": COMMON_FIELDS}
FRAUD_SCHEMA = {"fields": COMMON_FIELDS}
VELOCITY_SCHEMA = {
    "fields": [
        {"name": "account_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "pane_index", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "pane_timing", "type": "STRING", "mode": "REQUIRED"},
        {"name": "is_late", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "transaction_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "maximum_amount", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


def to_scored_row(record: dict) -> dict:
    field_names = {field["name"] for field in COMMON_FIELDS}
    row = {name: record.get(name) for name in field_names}
    row["fraud_signals"] = list(record["fraud_signals"])
    return row


def to_velocity_row(record: dict) -> dict:
    return dict(record)
