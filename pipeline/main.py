"""Parameterized, safe-by-default Beam pipeline entry point."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from pipeline.transforms.detect import FraudRuleConfig, ScoreTransaction
from pipeline.transforms.enrich import EnrichTransaction
from pipeline.transforms.validate import INVALID_TAG, ValidateTransaction
from pipeline.transforms.velocity import BuildVelocityAlerts
from pipeline.utils.bq_utils import (
    FRAUD_SCHEMA,
    RAW_SCHEMA,
    VELOCITY_SCHEMA,
    to_scored_row,
    to_velocity_row,
)

CLOUD_CONFIRMATION = "DATAFLOW"


class FraudPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription")
        parser.add_argument("--raw_table")
        parser.add_argument("--fraud_alert_table")
        parser.add_argument("--velocity_alert_table")
        parser.add_argument("--invalid_event_topic")
        parser.add_argument("--sink_error_topic")
        parser.add_argument("--worker_service_account")
        parser.add_argument(
            "--streaming_mode", choices=("exactly_once", "at_least_once"), default="exactly_once"
        )
        parser.add_argument("--window_duration_seconds", type=int, default=300)
        parser.add_argument("--window_period_seconds", type=int, default=60)
        parser.add_argument("--allowed_lateness_seconds", type=int, default=600)
        parser.add_argument("--velocity_count_threshold", type=int, default=5)
        parser.add_argument("--velocity_amount_threshold", default="10000")
        parser.add_argument("--high_amount_threshold", default="5000")
        parser.add_argument("--fraud_alert_threshold", type=int, default=60)
        parser.add_argument("--input_file")
        parser.add_argument("--local_output_prefix", default="local-output/fraud")
        parser.add_argument("--confirm_cloud_run", default="")


def _require_positive(options: FraudPipelineOptions) -> None:
    numeric_options = {
        "window_duration_seconds": options.window_duration_seconds,
        "window_period_seconds": options.window_period_seconds,
        "allowed_lateness_seconds": options.allowed_lateness_seconds,
        "velocity_count_threshold": options.velocity_count_threshold,
    }
    for name, value in numeric_options.items():
        minimum = 0 if name == "allowed_lateness_seconds" else 1
        if value < minimum:
            raise ValueError(f"--{name} must be at least {minimum}")


def _event_timestamped_line(line: str) -> TimestampedValue:
    """Assign local JSONL records the same event time used by Pub/Sub in cloud mode."""

    try:
        value = json.loads(line)
        parsed = datetime.fromisoformat(value["event_timestamp"].replace("Z", "+00:00"))
        timestamp = parsed.timestamp()
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        timestamp = 0
    return TimestampedValue(line.encode("utf-8"), timestamp)


def build_pipeline(
    messages,
    options: FraudPipelineOptions,
    *,
    require_pubsub_attributes: bool,
) -> dict[str, beam.PCollection]:
    _require_positive(options)
    validated = messages | "ValidateTransactions" >> beam.ParDo(
        ValidateTransaction(require_pubsub_attributes=require_pubsub_attributes)
    ).with_outputs(INVALID_TAG, main="valid")
    enriched = validated.valid | "EnrichTransactions" >> beam.ParDo(EnrichTransaction())
    scored = enriched | "ScoreAllValidTransactions" >> beam.ParDo(
        ScoreTransaction(
            FraudRuleConfig(
                high_amount_threshold=Decimal(options.high_amount_threshold),
                alert_threshold=options.fraud_alert_threshold,
            )
        )
    )
    fraud_alerts = scored | "FilterRuleFraudAlerts" >> beam.Filter(
        lambda row: row["is_fraud_alert"]
    )
    velocity_alerts = scored | "BuildVelocityAlerts" >> BuildVelocityAlerts(
        window_duration_seconds=options.window_duration_seconds,
        window_period_seconds=options.window_period_seconds,
        allowed_lateness_seconds=options.allowed_lateness_seconds,
        count_threshold=options.velocity_count_threshold,
        amount_threshold=Decimal(options.velocity_amount_threshold),
    )
    return {
        "invalid": validated.invalid,
        "scored": scored,
        "fraud_alerts": fraud_alerts,
        "velocity_alerts": velocity_alerts,
    }


def _require_cloud_configuration(
    options: FraudPipelineOptions,
    cloud_options: GoogleCloudOptions,
    setup_options: SetupOptions,
) -> None:
    if options.confirm_cloud_run != CLOUD_CONFIRMATION:
        raise ValueError("Dataflow launch blocked: pass --confirm_cloud_run=DATAFLOW deliberately")
    required = {
        "project": cloud_options.project,
        "region": cloud_options.region,
        "temp_location": cloud_options.temp_location,
        "staging_location": cloud_options.staging_location,
        "input_subscription": options.input_subscription,
        "raw_table": options.raw_table,
        "fraud_alert_table": options.fraud_alert_table,
        "velocity_alert_table": options.velocity_alert_table,
        "invalid_event_topic": options.invalid_event_topic,
        "sink_error_topic": options.sink_error_topic,
        "worker_service_account": options.worker_service_account,
        "requirements_file": setup_options.requirements_file,
        "setup_file": setup_options.setup_file,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Dataflow launch blocked; missing options: {', '.join(missing)}")


def _write_bigquery(collection, *, table: str, schema: dict, use_at_least_once: bool, label: str):
    return collection | label >> WriteToBigQuery(
        table=table,
        schema=schema,
        method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        use_at_least_once=use_at_least_once,
        with_auto_sharding=True,
        triggering_frequency=5,
        type_overrides={"DATE": date, "TIMESTAMP": Timestamp},
    )


def _tag_sink_error(error: dict, sink_name: str) -> dict:
    return {
        "sink_name": sink_name,
        "error_code": "BIGQUERY_WRITE_FAILED",
        "error_message": error["error_message"],
        "failed_row": error["failed_row"],
        "processing_timestamp": datetime.now(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
    }


def _attach_cloud_sinks(outputs, options: FraudPipelineOptions) -> None:
    use_at_least_once = options.streaming_mode == "at_least_once"
    (
        outputs["invalid"]
        | "SerializeInvalidEvents"
        >> beam.Map(lambda row: json.dumps(row, sort_keys=True).encode("utf-8"))
        | "PublishInvalidEvents" >> beam.io.WriteToPubSub(topic=options.invalid_event_topic)
    )
    raw_result = _write_bigquery(
        outputs["scored"] | "NormalizeRawRows" >> beam.Map(to_scored_row),
        table=options.raw_table,
        schema=RAW_SCHEMA,
        use_at_least_once=use_at_least_once,
        label="WriteAllScoredTransactions",
    )
    fraud_result = _write_bigquery(
        outputs["fraud_alerts"] | "NormalizeFraudRows" >> beam.Map(to_scored_row),
        table=options.fraud_alert_table,
        schema=FRAUD_SCHEMA,
        use_at_least_once=use_at_least_once,
        label="WriteRuleFraudAlerts",
    )
    velocity_result = _write_bigquery(
        outputs["velocity_alerts"] | "NormalizeVelocityRows" >> beam.Map(to_velocity_row),
        table=options.velocity_alert_table,
        schema=VELOCITY_SCHEMA,
        use_at_least_once=use_at_least_once,
        label="WriteVelocityAlerts",
    )
    failed_writes = (
        raw_result.failed_rows_with_errors
        | "TagRawWriteErrors" >> beam.Map(_tag_sink_error, "raw_transactions"),
        fraud_result.failed_rows_with_errors
        | "TagFraudWriteErrors" >> beam.Map(_tag_sink_error, "fraud_alerts"),
        velocity_result.failed_rows_with_errors
        | "TagVelocityWriteErrors" >> beam.Map(_tag_sink_error, "account_velocity_alerts"),
    ) | "FlattenBigQueryWriteErrors" >> beam.Flatten()
    (
        failed_writes
        | "SerializeBigQueryWriteErrors"
        >> beam.Map(lambda row: json.dumps(row, default=str, sort_keys=True).encode("utf-8"))
        | "PublishBigQueryWriteErrors" >> beam.io.WriteToPubSub(topic=options.sink_error_topic)
    )


def _attach_local_sinks(outputs, prefix: str) -> None:
    for name, collection in outputs.items():
        (
            collection
            | f"SerializeLocal{name}"
            >> beam.Map(lambda row: json.dumps(row, default=str, sort_keys=True))
            | f"WriteLocal{name}"
            >> beam.io.WriteToText(f"{prefix}-{name}", file_name_suffix=".jsonl")
        )


def create_pipeline(pipeline_options: PipelineOptions) -> beam.Pipeline:
    """Construct the selected local or cloud graph without submitting it."""

    standard = pipeline_options.view_as(StandardOptions)
    custom = pipeline_options.view_as(FraudPipelineOptions)
    cloud = pipeline_options.view_as(GoogleCloudOptions)
    setup = pipeline_options.view_as(SetupOptions)
    runner_name = standard.runner or "DirectRunner"
    is_dataflow = "dataflowrunner" in runner_name.lower()
    if not is_dataflow and "directrunner" not in runner_name.lower():
        raise ValueError(
            "Only DirectRunner (safe local default) or explicit DataflowRunner is supported"
        )

    if is_dataflow:
        _require_cloud_configuration(custom, cloud, setup)
        standard.streaming = True
        cloud.service_account_email = custom.worker_service_account
        setup.save_main_session = True
        if custom.streaming_mode == "at_least_once":
            existing = list(cloud.dataflow_service_options or [])
            cloud.dataflow_service_options = [*existing, "streaming_mode_at_least_once"]
    elif not custom.input_file:
        raise ValueError("Safe DirectRunner mode requires --input_file=<local JSONL file>")

    pipeline = beam.Pipeline(options=pipeline_options)
    if is_dataflow:
        messages = pipeline | "ReadDurableSubscription" >> beam.io.ReadFromPubSub(
            subscription=custom.input_subscription,
            with_attributes=True,
            id_label="transaction_id",
            timestamp_attribute="event_timestamp",
        )
    else:
        messages = (
            pipeline
            | "ReadLocalJSONL" >> beam.io.ReadFromText(custom.input_file)
            | "AssignLocalEventTimestamps" >> beam.Map(_event_timestamped_line)
        )

    outputs = build_pipeline(messages, custom, require_pubsub_attributes=is_dataflow)
    if is_dataflow:
        _attach_cloud_sinks(outputs, custom)
    else:
        _attach_local_sinks(outputs, custom.local_output_prefix)
    return pipeline


def run(argv: list[str] | None = None):
    return create_pipeline(PipelineOptions(argv)).run()


if __name__ == "__main__":
    run()
