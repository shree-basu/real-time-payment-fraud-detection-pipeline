"""Beam validation transform with structured invalid-event routing."""

from __future__ import annotations

import json
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from pipeline.schemas.transaction import (
    ContractViolation,
    invalid_event_envelope,
    normalize_event_timestamp,
    parse_transaction,
)

INVALID_TAG = "invalid"


def _payload_and_attributes(element: Any) -> tuple[Any, dict[str, str]]:
    if hasattr(element, "data") and hasattr(element, "attributes"):
        return element.data, dict(element.attributes or {})
    return element, {}


def _best_effort_identity(payload: Any) -> tuple[str | None, str | None]:
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None, None
    if not isinstance(decoded, dict):
        return None, None
    tx_id = decoded.get("transaction_id")
    event_timestamp = decoded.get("event_timestamp")
    return tx_id if isinstance(tx_id, str) else None, (
        event_timestamp if isinstance(event_timestamp, str) else None
    )


class ValidateTransaction(beam.DoFn):
    def __init__(self, *, require_pubsub_attributes: bool = False) -> None:
        self.require_pubsub_attributes = require_pubsub_attributes
        self.received = Metrics.counter("fraud_pipeline", "messages_received")
        self.valid = Metrics.counter("fraud_pipeline", "valid_transactions")
        self.invalid = Metrics.counter("fraud_pipeline", "invalid_transactions")

    def process(self, element: Any):
        self.received.inc()
        payload, attributes = _payload_and_attributes(element)
        tx_id, event_timestamp = _best_effort_identity(payload)
        try:
            record = parse_transaction(payload)
            attribute_id = attributes.get("transaction_id")
            attribute_timestamp = attributes.get("event_timestamp")
            if self.require_pubsub_attributes and (not attribute_id or not attribute_timestamp):
                raise ContractViolation(
                    "MISSING_ATTRIBUTE",
                    "Pub/Sub transaction_id and event_timestamp attributes are required",
                )
            if attribute_id and attribute_id != record["transaction_id"]:
                raise ContractViolation(
                    "ATTRIBUTE_MISMATCH", "transaction_id attribute does not match payload"
                )
            if attribute_timestamp:
                normalized_attribute = normalize_event_timestamp(attribute_timestamp)
                if normalized_attribute != record["event_timestamp"]:
                    raise ContractViolation(
                        "ATTRIBUTE_MISMATCH", "event_timestamp attribute does not match payload"
                    )
        except ContractViolation as violation:
            self.invalid.inc()
            yield TaggedOutput(
                INVALID_TAG,
                invalid_event_envelope(
                    payload,
                    violation,
                    transaction_id=tx_id,
                    event_timestamp=event_timestamp,
                ),
            )
            return
        self.valid.inc()
        yield record
