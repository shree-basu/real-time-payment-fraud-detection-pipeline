import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty
from pipeline.transforms.validate import ValidateTransaction
import json