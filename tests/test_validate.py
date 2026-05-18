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