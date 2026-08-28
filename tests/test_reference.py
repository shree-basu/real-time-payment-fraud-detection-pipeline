import json
from decimal import Decimal

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from pipeline.main import FraudPipelineOptions, _tag_sink_error
from pipeline.schemas.transaction import ContractViolation, parse_transaction
from pipeline.transforms.detect import FraudRuleConfig, score_transaction
from pipeline.transforms.enrich import enrich_transaction
from pipeline.transforms.validate import INVALID_TAG, ValidateTransaction
from pipeline.transforms.velocity import BuildVelocityAlerts
from pipeline.utils.bq_utils import to_scored_row, to_velocity_row
from simulator.publish_transactions import (
    generate_events,
    publish_to_pubsub,
    pubsub_attributes,
)


def transaction(**overrides):
    record = {
        "transaction_id": "tx-001",
        "account_id": "acct-001",
        "amount": "7500.25",
        "currency": "USD",
        "merchant_category": "electronics",
        "event_timestamp": "2026-01-01T12:01:00Z",
        "country_code": "NG",
        "is_online": True,
        "ip_address": "198.51.100.10",
    }
    record.update(overrides)
    return record


def encoded(**overrides):
    return json.dumps(transaction(**overrides)).encode("utf-8")


def scored_record(**overrides):
    parsed = parse_transaction(transaction(**overrides))
    enriched = enrich_transaction(parsed)
    return score_transaction(
        enriched,
        FraudRuleConfig(),
        processed_at="2026-01-01T12:02:00.000000Z",
    )


def test_valid_parse_is_utc_and_exact_decimal():
    parsed = parse_transaction(encoded())
    assert parsed["amount"] == Decimal("7500.250000000")
    assert parsed["event_timestamp"] == "2026-01-01T12:01:00.000000Z"


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (b"not-json", "MALFORMED_JSON"),
        (json.dumps({"transaction_id": "x"}), "MISSING_FIELD"),
        (encoded(amount="-1"), "INVALID_AMOUNT"),
        (encoded(event_timestamp="not-a-time"), "INVALID_TIMESTAMP"),
        (encoded(is_online="true"), "INVALID_BOOLEAN"),
        (encoded(currency="DOGE"), "UNSUPPORTED_CURRENCY"),
    ],
)
def test_contract_rejects_bad_records(payload, code):
    with pytest.raises(ContractViolation) as error:
        parse_transaction(payload)
    assert error.value.code == code


def test_validation_routes_attribute_mismatch_to_quarantine():
    class Message:
        data = encoded()
        attributes = {
            "transaction_id": "different-id",
            "event_timestamp": "2026-01-01T12:01:00Z",
        }

    output = list(ValidateTransaction(require_pubsub_attributes=True).process(Message()))
    assert len(output) == 1
    assert output[0].tag == INVALID_TAG
    assert output[0].value["error_code"] == "ATTRIBUTE_MISMATCH"


def test_rule_scoring_is_pure_and_has_repeated_signals():
    source = enrich_transaction(parse_transaction(transaction()))
    scored = score_transaction(
        source,
        FraudRuleConfig(),
        processed_at="2026-01-01T12:02:00.000000Z",
    )
    assert "risk_score" not in source
    assert scored["is_fraud_alert"] is True
    assert scored["fraud_signals"] == [
        "HIGH_AMOUNT",
        "HIGH_RISK_COUNTRY",
        "SUSPICIOUS_ONLINE_CATEGORY",
    ]


def test_sink_normalization_matches_numeric_and_array_types():
    raw_row = to_scored_row(scored_record())
    assert raw_row["amount"] == Decimal("7500.250000000")
    assert isinstance(raw_row["fraud_signals"], list)
    assert "is_high_risk_country" in raw_row

    velocity_row = to_velocity_row(
        {
            "account_id": "acct-001",
            "total_amount": Decimal("11.50"),
            "maximum_amount": Decimal("10.25"),
        }
    )
    assert velocity_row["total_amount"] == Decimal("11.50")
    assert velocity_row["maximum_amount"] == Decimal("10.25")


def test_bigquery_failed_row_is_tagged_for_durable_replay():
    tagged = _tag_sink_error(
        {"error_message": "schema mismatch", "failed_row": {"transaction_id": "tx-1"}},
        "raw_transactions",
    )
    assert tagged["sink_name"] == "raw_transactions"
    assert tagged["error_code"] == "BIGQUERY_WRITE_FAILED"
    assert tagged["failed_row"]["transaction_id"] == "tx-1"


def test_simulator_is_deterministic_and_duplicate_ids_are_stable():
    first = generate_events(seed=7, count=4, scenario="duplicate")
    second = generate_events(seed=7, count=4, scenario="duplicate")
    assert first == second
    assert first[0] == first[1]
    assert first[2] == first[3]
    assert pubsub_attributes(first[0]) == pubsub_attributes(first[1])


def test_malformed_event_gets_stable_fallback_pubsub_attributes():
    malformed = '{"transaction_id":"invalid-json"'
    assert pubsub_attributes(malformed) == pubsub_attributes(malformed)
    assert pubsub_attributes(malformed)["event_timestamp"].endswith("Z")


def test_pubsub_publish_is_blocked_without_confirmation():
    with pytest.raises(ValueError, match="cloud publication blocked"):
        publish_to_pubsub([], project="example", topic="example", confirmation="")


def test_directrunner_validation_keeps_valid_and_quarantines_invalid():
    with BeamTestPipeline() as pipeline:
        validated = (
            pipeline
            | beam.Create([encoded(), b"not-json"])
            | beam.ParDo(ValidateTransaction()).with_outputs(INVALID_TAG, main="valid")
        )
        assert_that(
            validated.valid | beam.Map(lambda row: row["transaction_id"]),
            equal_to(["tx-001"]),
            label="AssertValid",
        )
        assert_that(
            validated.invalid | beam.Map(lambda row: row["error_code"]),
            equal_to(["MALFORMED_JSON"]),
            label="AssertInvalid",
        )


def test_event_time_window_generates_velocity_alert():
    first = scored_record(transaction_id="tx-1", amount="60")
    second = scored_record(transaction_id="tx-2", amount="50")
    with BeamTestPipeline() as pipeline:
        alerts = (
            pipeline
            | beam.Create([TimestampedValue(first, 60), TimestampedValue(second, 120)])
            | BuildVelocityAlerts(
                window_duration_seconds=300,
                window_period_seconds=300,
                allowed_lateness_seconds=600,
                count_threshold=2,
                amount_threshold=Decimal("100"),
            )
            | beam.Map(
                lambda row: (
                    row["account_id"],
                    row["transaction_count"],
                    row["total_amount"],
                    row["window_start"],
                    row["window_end"],
                )
            )
        )
        assert_that(
            alerts,
            equal_to(
                [
                    (
                        "acct-001",
                        2,
                        Decimal("110.000000000"),
                        "1970-01-01T00:00:00.000000Z",
                        "1970-01-01T00:05:00.000000Z",
                    )
                ]
            ),
        )


def test_teststream_emits_accumulating_late_pane_and_drops_too_late_event():
    first = scored_record(transaction_id="tx-on-time", amount="60")
    late = scored_record(transaction_id="tx-late", amount="50")
    too_late = scored_record(transaction_id="tx-too-late", amount="40")
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(first, 60)])
        .advance_watermark_to(301)
        .add_elements([TimestampedValue(late, 120)])
        .advance_watermark_to(901)
        .add_elements([TimestampedValue(too_late, 180)])
        .advance_watermark_to_infinity()
    )
    options = PipelineOptions(["--streaming", "--allow_unsafe_triggers"])
    with BeamTestPipeline(options=options) as pipeline:
        panes = (
            pipeline
            | stream
            | BuildVelocityAlerts(
                window_duration_seconds=300,
                window_period_seconds=300,
                allowed_lateness_seconds=600,
                count_threshold=1,
                amount_threshold=Decimal("1"),
            )
            | beam.Map(
                lambda row: (
                    row["transaction_count"],
                    row["total_amount"],
                    row["is_late"],
                )
            )
        )
        assert_that(
            panes,
            equal_to(
                [
                    (1, Decimal("60.000000000"), False),
                    (2, Decimal("110.000000000"), True),
                ]
            ),
        )


def test_safe_options_default_to_exactly_once_semantics():
    options = PipelineOptions([]).view_as(FraudPipelineOptions)
    assert options.streaming_mode == "exactly_once"
    assert options.window_duration_seconds == 300
    assert options.allowed_lateness_seconds == 600
