import apache_beam as beam
import json
from pipeline.schemas.transaction import REQUIRED_FIELDS

class ValidateTransaction(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            
            # Check 1: Required fields
            missing = [f for f in REQUIRED_FIELDS if f not in record]
            if missing:
                yield beam.pvalue.TaggedOutput(
                    "invalid",
                    {"raw": str(element), "error": f"Missing fields: {missing}"}
                )
                return

            # Check 2: Amount must be positive number
            if not isinstance(record["amount"], (int, float)) or record["amount"] <= 0:
                yield beam.pvalue.TaggedOutput(
                    "invalid",
                    {"raw": str(element), "error": "Invalid amount"}
                )
                return

            yield beam.pvalue.TaggedOutput("valid", record)

        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                "invalid",
                {"raw": str(element), "error": str(e)}
            )
