# Streaming failure scenarios

| Scenario | Detection and automatic behavior | Duplicate/data-loss boundary | Operator action |
|---|---|---|---|
| Malformed JSON / schema violation | Validation counter increments; structured envelope is sent to application quarantine | No silent drop is intended; quarantine publishing is part of the job graph | Inspect replay subscription, correct producer or payload, republish deliberately |
| Payload/attribute mismatch | Rejected as `ATTRIBUTE_MISMATCH` | Prevents source ID/time from disagreeing with application identity/time | Fix producer attributes before replay |
| Pub/Sub redelivery | Dataflow source uses the stable ID attribute | Dedup is bounded, not forever | Correlate transaction IDs and source metrics; use business reconciliation if required |
| Producer duplicate | Same ID may dedup within runner horizon; new ID is processed | No BigQuery primary-key guarantee | Apply the domain's explicit correction/reconciliation policy |
| Late event | Allowed events create accumulating late panes | Repeated window rows are intentional pane versions | Query latest pane per window/account or retain all versions for audit |
| Too-late event | Excluded from expired velocity window; remains in raw/rule paths | Historical velocity aggregate is not reconstructed | Investigate source delay; use a separate correction/backfill design if required |
| Hot account | Dataflow key metrics/backpressure and subscription backlog expose pressure | One key can limit parallelism and increase latency | Investigate abusive account; consider fan-out/two-stage aggregation only with measured need |
| BigQuery sink failure | Connector failed-row outputs are tagged and sent to a durable sink-error topic; job-level errors can still fail/retry work; table creation is disabled | Exact cloud retry/output behavior is not tested; business duplicates remain separate | Inspect Dataflow/BigQuery errors and sink-error replay, correct schema/quota issue, replay or restart deliberately |
| Invalid-event sink failure | Pub/Sub sink error fails/retries pipeline work | No independent dual-write transaction with main sinks is claimed | Restore topic/IAM/quota, restart job, verify invalid counter versus quarantine messages |
| Worker retry or outage | Dataflow retries and durable subscription retain backlog | Source/sink semantics govern duplicates; no SLA is claimed | Inspect job state and backlog alerts; restart/cancel according to incident policy |
| Subscription backlog | Terraform alert policies model count and oldest-unacked age | Long delays can move events into late/too-late categories | Scale/investigate workers, quota, hot keys, and downstream sink health |
| Schema evolution | Normalizer/table schema mismatch causes validation or sink failure | Unknown source fields are not propagated | Deploy compatible contract/table change before producers; version breaking changes |
| Traffic spike | Beam metrics, backlog, worker and sink metrics show pressure | Unmeasured maximum throughput; latency may rise | Confirm autoscaling/quota and hot-key distribution; run authorized load tests before claims |

No row-level Cloud Monitoring API call is made from a `DoFn`. Business fraud-rate monitoring is intentionally left as a downstream analytical concern rather than an invented custom metric.
