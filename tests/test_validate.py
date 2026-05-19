import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty
from pipeline.transforms.validate import ValidateTransaction
import json

def test_missing_fields_goes_to_invalid():
    record = {
        "transaction_id": "T001",
        "amount": 150.0,
        "currency": "USD"
    }
    raw = json.dumps(record).encode("utf-8")

    with TestPipeline as p:
        result = (
            p
            | beam.Create([raw])
            | beam.ParDo(ValidateTransaction()).with_outputs("valid","invalid")
        )
        assert_that(result.invalid, is_not_empty())

def test_invalid_amount_goes_to_invalid():
    record = {
        "transaction_id": "T002",
        "account_id": "ACC123",
        "amount": -50.0,
        "currency": "USD",
        "merchant_category": "grocery",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "US",
        "is_online": False
    }
    raw = json.dumps(record).encode("utf-8")

    with TestPipeline as p:
        result = (
            p
            | beam.Create([raw])
            | beam.ParDo(ValidateTransaction()).with_outputs("valid","invalid")
        )
        assert_that(result.invalid, is_not_empty())

def test_valid_record_goes_to_valid():
    record = {
        "transaction_id": "TOO3",
        "account_id": "ACC123",
        "amount": 150.0,
        "currency": "USD",
        "merchant_category": "grocery",
        "timestamp": "2024-01-01T00:00:00Z",
        "country_code": "US",
        "is_online": False
    }
    raw = json.dump(record).encode("utf-8")

    with TestPipeline as p:
        result = (
            p
            | beam.Create([raw])
            | beam.ParDo(ValidateTransaction()).with_outputs("valid","invalid")
        )
        assert_that(result.valid, is_not_empty())
    