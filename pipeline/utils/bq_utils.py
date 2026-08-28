"""BigQuery schemas and Beam-native Storage Write API row normalization."""

from __future__ import annotations

import json
from datetime import date, datetime
from importlib.resources import files

from apache_beam.utils.timestamp import Timestamp


def _schema(name: str) -> dict:
    fields = json.loads(files("pipeline.schemas").joinpath(name).read_text(encoding="utf-8"))
    return {"fields": fields}


def _timestamp(value: str) -> Timestamp:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return Timestamp.from_utc_datetime(parsed)


COMMON_FIELDS = _schema("bigquery_transaction.json")["fields"]
RAW_SCHEMA = {"fields": COMMON_FIELDS}
FRAUD_SCHEMA = {"fields": COMMON_FIELDS}
VELOCITY_SCHEMA = _schema("bigquery_velocity.json")


def to_scored_row(record: dict) -> dict:
    field_names = {field["name"] for field in COMMON_FIELDS}
    row = {name: record.get(name) for name in field_names}
    row["fraud_signals"] = list(record["fraud_signals"])
    row["event_date"] = date.fromisoformat(record["event_date"])
    row["event_timestamp"] = _timestamp(record["event_timestamp"])
    row["processed_at"] = _timestamp(record["processed_at"])
    return row


def to_velocity_row(record: dict) -> dict:
    row = dict(record)
    row["window_start"] = _timestamp(record["window_start"])
    row["window_end"] = _timestamp(record["window_end"])
    row["window_date"] = date.fromisoformat(record["window_date"])
    row["processed_at"] = _timestamp(record["processed_at"])
    return row
