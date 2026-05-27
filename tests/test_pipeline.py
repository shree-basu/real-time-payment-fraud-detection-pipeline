import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty
from pipeline.transforms.validate import ValidateTransaction
from pipeline.transforms.enrich import EnrichTransaction
from pipeline.transforms.detect import DetectFraud
import json


def test_fraud_transaction_reaches_fraud_output():
    record = {
        "transaction_id": "T001",
        "account_id": "ACC123",
        "amount": 9999.99,
        "currency": "USD",
        "merchant_category": "electronics",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "NG",
        "is_online": False,
    }
    raw = json.dumps(record).encode("utf-8")

    with _TestPipeline() as p:
        validated = (
            p
            | beam.Create([raw])
            | beam.ParDo(ValidateTransaction()).with_outputs("valid", "invalid")
        )
        enriched = (
            validated.valid
            | beam.ParDo(EnrichTransaction())
        )
        result = (
            enriched
            | beam.ParDo(DetectFraud()).with_outputs("fraud", "clean")
        )
        assert_that(result.fraud, is_not_empty())


def test_clean_transaction_reaches_clean_output():
    record = {
        "transaction_id": "T002",
        "account_id": "ACC456",
        "amount": 25.0,
        "currency": "USD",
        "merchant_category": "grocery",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "US",
        "is_online": False,
    }
    raw = json.dumps(record).encode("utf-8")

    with _TestPipeline() as p:
        validated = (
            p
            | beam.Create([raw])
            | beam.ParDo(ValidateTransaction()).with_outputs("valid", "invalid")
        )
        enriched = (
            validated.valid
            | beam.ParDo(EnrichTransaction())
        )
        result = (
            enriched
            | beam.ParDo(DetectFraud()).with_outputs("fraud", "clean")
        )
        assert_that(result.clean, is_not_empty())