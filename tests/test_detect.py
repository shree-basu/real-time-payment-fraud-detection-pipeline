import pytest
from pipeline.transforms.detect import DetectFraud
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty

def test_high_amount_triggers_fraud():
    record = {
        "transaction_id": "TOO1",
        "account_id": "ACC123",
        "amount": 9999.99,
        "currency": "USD",
        "merchant_category": "electronics",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "US",
        "is_online": False
    }
    with TestPipeline() as p:
        result = (
            p
            | beam.Create([record])
            | beam.ParDo(DetectFraud()).with_outputs("fraud", "clean")
        )
        assert_that(result.fraud, is_not_empty())

def test_clean_transactions_goes_to_clean():
    record = {
        "transaction_id": "TOO2",
        "account_id": "ACC456",
        "amount": 25.0,
        "currency": "USD",
        "merchant_category": "grocery",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "US",
        "is_online": False
    }
    with TestPipeline as p:
        result = (
            p
            | beam.Create([record])
            | beam.ParDo(DetectFraud()).with_outputs("fraud","clean")
        )
        assert_that(result.clean, is_not_empty())