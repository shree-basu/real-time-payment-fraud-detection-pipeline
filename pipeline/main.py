import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from pipeline.transforms.validate import ValidateTransaction
from pipeline.transforms.enrich import EnrichTransaction
from pipeline.transforms.detect import DetectFraud
from pipeline.utils.bq_utils import RAW_SCHEMA, FRAUD_SCHEMA

PROJECT = "your-project-id"
REGION = "us-central1"
INPUT_TOPIC = f"projects/{PROJECT}/topics/fraud-transactions"
DLQ_BUCKET = f"gs://{PROJECT}-dlq/invalid-transactions"
RAW_TABLE = f"{PROJECT}: fraud_detection.raw_transactions"
FRAUD_TABLE = f"{PROJECT}:fraud_detection.fraud_alerts"

def run():
    options = PipelineOptions(
        project=PROJECT,
        region=REGION,
        runner='DataflowRunner',
        streaming=True,
        save_main_session=True,
        job_name="fraud-detection-pipeline",
        temp_location=f"gs://{PROJECT}-temp/dataflow"
        staging_location=f"gs://{PROJECT}-staging/dataflow",
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = p | "ReadFromPubSub" >>
        beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        
        validated = (
             messages
             | "ValidateSchema" >>
        beam.ParDO(ValidateTransaction()).with_outputs("valid", "invalid")
        )
        validated = (
            |"SerializeDLQ">>beam.Map(lambda x:json.dumps(x))
            |"WriteDLQ">>beam.io.WriteToText(DLQ_BUCKET,file_name_suffix=".json")
        )