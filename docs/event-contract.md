# Transaction event contract

## Required payload

| Field | Type | Rule |
|---|---|---|
| `transaction_id` | string | non-empty; also the Pub/Sub ID attribute |
| `account_id` | string | non-empty |
| `amount` | decimal-compatible JSON number/string | finite, positive, within BigQuery `NUMERIC`; normalized to nine decimal places internally |
| `currency` | string | one of USD, EUR, GBP, INR |
| `merchant_category` | string | electronics, grocery, jewelry, retail, travel, or utilities |
| `event_timestamp` | string | timezone-aware ISO-8601; normalized to UTC; also the Pub/Sub timestamp attribute |
| `country_code` | string | two alphabetic characters, normalized uppercase |
| `is_online` | JSON boolean | strings such as `"true"` are invalid |
| `ip_address` | string or null | optional; a supplied string must be non-empty |

Unknown input fields are not propagated. This avoids accidental schema drift into sinks.

## Invalid-event envelope

Application-invalid events are not silently discarded. Their quarantine message contains:

- `raw_payload`;
- `transaction_id` when recoverable;
- `event_timestamp` when recoverable;
- stable `error_code`;
- human-readable `error_reason`;
- UTC `processing_timestamp`.

Representative codes include `MALFORMED_JSON`, `MISSING_FIELD`, `INVALID_AMOUNT`, `INVALID_TIMESTAMP`, `INVALID_BOOLEAN`, `UNSUPPORTED_CURRENCY`, and `ATTRIBUTE_MISMATCH`.

## Schema evolution

Additive nullable input fields should first be added to this contract and tested, then added to sink normalization and Terraform schemas before producers send them. Breaking changes require a versioned topic or an explicit compatibility transform. Terraform tables use deletion protection and do not accept accidental unknown fields through the normalizer.
