# Architecture

## Business objective

The reference pipeline separates immediate deterministic fraud signals from account-level velocity behavior while retaining a complete, scored transaction history. It is designed to explain the decisions and failure boundaries expected in a production streaming interview; it is not represented as deployed.

## Source and contract boundary

The deterministic simulator emits one JSON object per payment. Stdout is the safe default. Explicit Pub/Sub mode attaches `transaction_id` and `event_timestamp` attributes identical to the payload. Terraform models a durable input subscription, and Beam consumes that subscription with the two attributes configured as source ID and source event time.

Validation is the first application transform. Valid records become normalized internal dictionaries with `Decimal` amount and UTC time. Invalid records become structured envelopes containing the original payload, recoverable identity/time, code, reason, and processing time. They are published to an application-invalid topic with a durable replay subscription. This is distinct from Pub/Sub infrastructure dead-lettering for repeated delivery failures.

## Processing branches

After validation:

1. Enrichment derives event date/hour, high-risk-country, and IP-presence features.
2. Pure rule scoring adds a list of signals, integer score, decision, and processing time without mutating the input.
3. Every valid scored row is normalized to the raw schema.
4. The rule-alert subset is independently normalized to the alert schema.
5. All scored events are keyed by account and currency and enter event-time sliding windows for count, sum, and maximum amount. The current USD-only contract keeps thresholds and aggregation units defensible. Threshold crossings produce pane-aware velocity alerts.
6. Storage Write API failed-row outputs are tagged with the destination/error and sent to a separate sink-error topic with a durable replay subscription.

The rule and raw branches are not window-expiry dependent. A too-late event may be absent from velocity aggregation while still being preserved in raw history and evaluated by transaction rules.

## Serving model

Terraform defines:

- `raw_transactions`, partitioned on `event_date`, clustered by account and merchant category;
- `fraud_alerts`, partitioned on `event_date`, clustered by account and score;
- `account_velocity_alerts`, partitioned on `window_date`, clustered by account.

Partition filters are required. These choices match likely investigation access paths, but no measured query-cost improvement is claimed without a real workload.

## Runtime and infrastructure

Beam 2.75.0 is packaged through standard Python project metadata. DirectRunner is the default. Dataflow requires an explicit runner, full project/region/subscription/table/bucket/worker configuration, and a confirmation token. Storage Write API is selected with exactly-once-oriented mode by default; sink rows use Beam-native DATE/TIMESTAMP values and the required DATE type override. Python and Terraform load the same packaged BigQuery schema files.

Terraform models APIs, topics/subscriptions, dataset/tables, a protected temporary/staging bucket, worker identity, scoped access, and static Pub/Sub monitoring policies. It does not provision a continuously running Dataflow job.
