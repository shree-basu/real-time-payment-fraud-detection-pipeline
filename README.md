# Real-Time Payment Fraud Detection

Production-realistic streaming reference implementation for:

**Pub/Sub → Apache Beam / Dataflow → BigQuery**

It demonstrates strict payment-event validation, event-time processing, rule-based transaction alerts, sliding-window account velocity alerts, durable invalid-event quarantine, exact-money schemas, least-privilege infrastructure, and cloud-free CI.

> **Evidence boundary:** this repository is implemented and locally/CI tested, but it is not a running production system. No GCP project, Pub/Sub flow, Dataflow job, BigQuery write, alert, production SLA, scale, latency, or cost result is claimed.

## Cost and deployment safety

Normal repository activity cannot create GCP charges:

- there is no deployment workflow and CI has no Google authentication;
- `DirectRunner` is the default and requires a local JSONL input;
- Dataflow refuses to launch without complete cloud configuration and `--confirm_cloud_run=DATAFLOW`;
- the simulator defaults to stdout and requires `--mode=pubsub --confirm-publish=PUBSUB` before constructing a publisher;
- Terraform defaults to zero resources and requires both `deployment_enabled=true` and `deployment_confirmation="DEPLOY"`;
- CI rejects GCP authentication, `terraform apply`, and `gcloud dataflow` commands in workflows.

Dataflow streaming jobs incur charges continuously while running. Merely viewing, cloning, testing, or keeping this public repository does not deploy anything. See [the runbook](docs/runbook.md) before any future authorized cloud use.

## Architecture

```text
deterministic simulator
        │ stdout (default) / Pub/Sub publish (explicitly gated)
        ▼
durable Pub/Sub subscription
        │ Beam id_label=transaction_id (Pub/Sub attribute)
        │ timestamp_attribute=event_timestamp
        ▼
validation ── invalid ──► application quarantine topic ──► replay subscription
        │ valid
        ▼
enrichment → rule scoring ───────────────► all valid scored rows → raw_transactions
        │                                └► rule alerts → fraud_alerts
        └► account key → 5m sliding window / 1m slide
                           watermark + 10m lateness + accumulating late panes
                                      └► account_velocity_alerts
BigQuery Storage Write failures ──────► sink-error topic ──► replay subscription
```

The cloud sinks use Beam's BigQuery Storage Write API path and route its failed-row output to a separate durable sink-error topic. Terraform pre-creates partitioned/clustered tables and a dedicated worker service account. Beam-native counters capture received, valid, invalid, rule-alert, and velocity-alert counts; Terraform models real Pub/Sub backlog and oldest-unacked-age alerts.

## Core semantics

- Payment amounts use `Decimal` internally and BigQuery `NUMERIC`, never binary `FLOAT`.
- Timestamps must be timezone-aware ISO-8601 and are normalized to UTC.
- Pub/Sub payload and `transaction_id` / `event_timestamp` attributes must match.
- Every valid transaction is enriched and scored before reaching raw history; fraud alerts are a subset.
- Rule signals are a BigQuery `REPEATED STRING` and scoring returns a copy instead of mutating branch input.
- Velocity uses configurable event-time sliding windows, watermark firing, allowed lateness, late firing after each late element, and accumulating panes.
- Pane index/timing and window boundaries make repeated accumulating results intentional versions, not unexplained duplicates.
- A too-late event is excluded from its expired velocity window but still remains eligible for the upstream raw and rule-scoring branches.

See [streaming semantics](docs/streaming-semantics.md) for the exact deduplication and “exactly once” boundaries.

## Safe local use

Python 3.11–3.13 is supported. Apache Beam 2.75.0 is pinned; Google lists that SDK as supported for Dataflow at implementation time.

```bash
python -m venv .venv
python -m pip install -r requirements-lock.txt
python simulator/publish_transactions.py --seed 42 --count 20 --scenario normal > transactions.jsonl
python -m pipeline.main --runner=DirectRunner --input_file=transactions.jsonl
python -m pytest -q
python -m ruff check pipeline simulator tests
```

Available simulator scenarios are `normal`, `duplicate`, `invalid`, `late`, and `fraud_burst`. Stdout is always the default.

Terraform static validation is non-deploying:

```bash
cd infra/terraform
terraform fmt -recursive -check
terraform init -backend=false
terraform validate
```

`requirements.txt` / `requirements-dev.txt` hold reviewed direct pins; the generated runtime/development lockfiles freeze transitive versions. Do not run `terraform apply` or Dataflow commands without explicit authorization, a billing-aware project, and the cleanup plan in the runbook.

## Evidence matrix

| Status | Evidence |
|---|---|
| Implemented + locally tested | Contract, scoring, sink normalization, deterministic scenarios, DirectRunner validation, event-time aggregation, allowed-late pane, too-late exclusion |
| Implemented + statically validated | Terraform resources/IAM/monitoring, provider lock, Python packaging, cloud-free CI, Dataflow/BigQuery graph configuration |
| Designed but not deployed | Pub/Sub source/quarantine, Dataflow runner, Storage Write API sinks, GCP monitoring policies |
| Not evidenced | Authenticated GCP execution, production load, operational alerts, dashboards, SLA, latency, throughput, cost savings |

## Documentation

- [Architecture](docs/architecture.md)
- [Event contract](docs/event-contract.md)
- [Streaming, deduplication, windows, and sink semantics](docs/streaming-semantics.md)
- [Failure scenarios](docs/failure-scenarios.md)
- [Operations and cost-safety runbook](docs/runbook.md)
- [Validation evidence and limitations](docs/evidence.md)
