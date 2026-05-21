# Real-Time Payment Fraud Detection Pipeline

A production-grade, real-time fraud detection pipeline built on Google Cloud Platform. 
Transactions are streamed through Pub/Sub, processed by Apache Beam on Dataflow, 
scored for fraud, and stored in BigQuery — running 24/7 with Cloud Monitoring alerts.

## Architecture
[Transaction Simulator]
↓
[Pub/Sub Topic]
↓
[Dataflow - Apache Beam]
├── Schema Validation
├── Feature Enrichment
├── Fraud Detection
└── DLQ → GCS (invalid records)
↓
[BigQuery Tables]
├── raw_transactions
└── fraud_alerts
↓
[Cloud Monitoring Alerts]

## Tech Stack
| Tool | Purpose |
|------|---------|
| Apache Beam | Stream processing framework |
| Google Cloud Dataflow | Managed runner for Beam pipelines |
| Google Cloud Pub/Sub | Real-time message ingestion |
| Google BigQuery | Data warehouse for results |
| Terraform | Infrastructure as Code |
| Cloud Monitoring | Alerting and observability |
| GitHub Actions | CI/CD — automated testing on every push |

## Project Structure
├── pipeline/
│ ├── main.py # Pipeline entry point — wires all transforms
│ ├── schemas/transaction.py # Data contract
│ ├── transforms/
│ │ ├── validate.py # Schema validation + DLQ routing
│ │ ├── enrich.py # Feature enrichment
│ │ └── detect.py # Fraud scoring
│ └── utils/bq_utils.py # BigQuery schemas
├── simulator/
│ └── publish_transactions.py # Generates fake transactions
├── infra/terraform/ # GCP infrastructure as code
├── tests/ # Unit + end-to-end tests
├── monitoring/alerts.yaml # Cloud Monitoring alert policies
└── .github/workflows/ci.yml # CI pipeline

## Pipeline Flow
1. **Simulator** generates realistic transactions and publishes to Pub/Sub
2. **Validation** checks every message for required fields and valid amounts — invalid records go to a Dead Letter Queue in GCS
3. **Enrichment** adds derived features: `hour_of_day`, `is_high_risk_country`, `has_ip_address`
4. **Fraud Detection** scores each transaction — risk score ≥ 60 triggers a fraud alert
5. **BigQuery** stores all transactions in `raw_transactions` and flagged ones in `fraud_alerts`

## Fraud Scoring
| Signal | Points |
|--------|--------|
| Amount > $5,000 | +40 |
| High risk country (NG, RU, CN) | +30 |
| Suspicious online merchant | +20 |
| **Fraud threshold** | **≥ 60** |
## How to Run

**1. Clone the repo**
```bash
git clone https://github.com/shree-basu/real-time-payment-fraud-detection-pipeline.git
cd real-time-payment-fraud-detection-pipeline
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Provision infrastructure**
```bash
cd infra/terraform
terraform init
terraform apply
```

**4. Run the simulator**
```bash
python simulator/publish_transactions.py
```

**5. Run tests**
```bash
pytest tests/ -v
```

## CI/CD

GitHub Actions automatically runs all tests and linting on every push to `main`.