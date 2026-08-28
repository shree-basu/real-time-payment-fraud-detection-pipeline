# Streaming semantics

## Source IDs and duplicate boundaries

Beam reads the durable subscription with:

- `id_label="transaction_id"` (the Beam Python option naming the Pub/Sub attribute);
- `timestamp_attribute="event_timestamp"`;
- Pub/Sub attributes required to match the payload.

On Dataflow, the custom ID supports runner-managed source deduplication for redelivery/repeated source messages within its bounded operational horizon. It is not infinite or a business-key uniqueness constraint. The repository deliberately does not invent an exact duration that it cannot configure or prove.

Three cases remain distinct:

1. Pub/Sub redelivers the same message/ID: source deduplication is intended to suppress duplicates within the runner horizon.
2. A producer republishes the same logical transaction with the same ID: it may be suppressed within that horizon, but no forever guarantee is claimed.
3. A producer republishes the same logical transaction with a new ID: it is a new source identity and will be processed. Resolving that business-key conflict requires a separate durable business rule, not a claim of transport exactly-once.

## Event time, windows, and panes

The source timestamp attribute supplies event time. Velocity is keyed by `account_id` and defaults to a five-minute sliding window every one minute. The trigger fires when the watermark passes the window end. Allowed lateness defaults to ten minutes, with a late firing after each accepted late element and accumulating mode.

- **On time:** arrives before the watermark passes the window end and contributes to the on-time pane.
- **Late but allowed:** arrives after the on-time firing but before end plus allowed lateness; it creates another accumulating pane.
- **Too late:** arrives after the allowed-lateness horizon; it is excluded from that velocity window. It still traverses the upstream raw/rule branches.
- **Accumulating pane:** contains the revised aggregate, not just the late delta. Multiple rows for a window are intentional versions. `window_start`, `window_end`, `pane_index`, `pane_timing`, and `is_late` disambiguate them.

Watermarks are runner estimates of event-time completeness, not a promise that no earlier timestamp can arrive. TestStream exercises on-time, allowed-late, and too-late behavior locally.

## “Exactly once” is not one guarantee

- **Dataflow processing mode:** defaults to Dataflow's exactly-once streaming mode; at-least-once must be explicitly selected.
- **Pub/Sub source deduplication:** bounded and based on `transaction_id`; it is not business-key enforcement.
- **BigQuery Storage Write API:** the Beam sink uses its exactly-once-oriented mode when `use_at_least_once=false`; this path is implemented but not cloud-executed here.
- **Business transaction uniqueness:** not guaranteed forever by this transport design. BigQuery tables have no primary-key constraint.

Therefore the repository never summarizes all four as “exactly once.” A production business reconciliation layer would separately detect conflicting/late repeated transaction IDs and define correction policy.

## Sink failure boundary

Rows are explicitly normalized before the Storage Write API sink, and Terraform pre-creates tables so schema/configuration errors fail rather than silently creating drifted tables. Beam 2.75's `failed_rows_with_errors` outputs from all three sinks are tagged and published to a dedicated sink-error topic with a durable replay subscription. This capture path is implemented but not authenticated against BigQuery; job-level failure/retry behavior and the boundary between retryable failures and emitted failed rows remain cloud-unvalidated.
