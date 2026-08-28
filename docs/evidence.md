# Evidence and limitations

## Locally verified

- strict contract paths for valid, malformed, missing, numeric, timestamp, boolean, currency, and attribute-mismatch cases;
- exact Decimal handling and explicit BigQuery row normalization;
- pure rule scoring and repeated-string signals;
- deterministic normal/duplicate/invalid/late/fraud-burst generation;
- stdout default and Pub/Sub confirmation guard;
- DirectRunner valid/quarantine routing;
- event-time account aggregation;
- TestStream on-time, accumulating allowed-late, and too-late exclusion behavior;
- Python compile, Ruff, pytest, direct pins, transitive runtime/development locks, and dependency consistency;
- Terraform format, backend-disabled initialization, provider lock, and validation;
- obvious credential and workflow-deployment pattern checks.

## Statically implemented but not cloud-validated

- Pub/Sub source ID/timestamp attributes and durable subscriptions;
- application quarantine publication/replay;
- Dataflow exactly-once default and explicit at-least-once option;
- BigQuery Storage Write API tables, writes, and failed-row sink-error routing;
- worker service account/IAM;
- Pub/Sub backlog/oldest-age alert policies;
- cost and deployment gates.

## Not evidenced

- GCP authentication or Terraform plan/apply;
- Pub/Sub publish/consume behavior;
- Dataflow launch, restart, autoscaling, watermark, or native metrics in GCP;
- BigQuery authenticated syntax/write behavior or failed-row connector behavior;
- Cloud Monitoring evaluation/notification;
- production scale, throughput, latency, cost, availability, RPO/RTO, or SLA;
- active dashboard or alert notification channels.

These boundaries are intentional. The project is a production-realistic reference implementation and interview artifact, not a deployed production service.
