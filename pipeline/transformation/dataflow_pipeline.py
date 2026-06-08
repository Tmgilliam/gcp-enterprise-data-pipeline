"""
Apache Beam / Dataflow Pipeline — Unified batch and streaming ETL.

One shared pipeline codebase for both execution modes:
  - Batch: GCS raw → BigQuery staging (daily schedule)
  - Streaming: Pub/Sub → BigQuery (real-time events)

Multi-cloud equivalent:
  - Azure Data Factory + Stream Analytics
  - AWS Glue + Kinesis Data Analytics
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

logger = logging.getLogger(__name__)

INVENTORY_SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sku_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "vendor_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "transaction_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source_system", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "pipeline_run_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ValidateInventoryRecord(beam.DoFn):
    """Validate inventory transaction records before BigQuery write."""

    def process(self, element: dict[str, Any]):
        required = ["transaction_id", "sku_id", "quantity", "transaction_type"]
        if all(element.get(f) is not None for f in required):
            yield element
        else:
            logger.warning("Invalid record dropped: %s", element.get("transaction_id", "unknown"))


class EnrichWithMetadata(beam.DoFn):
    """Add pipeline metadata fields to each record."""

    def __init__(self, pipeline_run_id: str, source_system: str = "sage100"):
        self.pipeline_run_id = pipeline_run_id
        self.source_system = source_system

    def process(self, element: dict[str, Any]):
        now = datetime.now(timezone.utc)
        element["ingestion_date"] = now.strftime("%Y-%m-%d")
        element["pipeline_run_id"] = self.pipeline_run_id
        element["_ingested_at"] = now.isoformat()
        element["source_system"] = element.get("source_system", self.source_system)
        yield element


class ParsePubSubEvent(beam.DoFn):
    """Parse Pub/Sub JSON event into inventory transaction format."""

    def process(self, element: bytes):
        import json

        try:
            event = json.loads(element.decode("utf-8"))
            yield {
                "transaction_id": event.get("transaction_id", event.get("id", "")),
                "sku_id": event.get("sku_id", ""),
                "vendor_id": event.get("vendor_id"),
                "quantity": int(event.get("quantity", 0)),
                "transaction_type": event.get("event_type", event.get("transaction_type", "")),
                "transaction_timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            }
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Failed to parse Pub/Sub event: %s", exc)


def run_batch_pipeline(
    project_id: str,
    gcs_input_pattern: str,
    bq_table: str,
    pipeline_run_id: str,
    pipeline_options: PipelineOptions,
):
    """Batch pipeline: GCS Parquet → validate → enrich → BigQuery staging."""
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadGCS" >> ReadFromParquet(gcs_input_pattern)
            | "Validate" >> beam.ParDo(ValidateInventoryRecord())
            | "Enrich" >> beam.ParDo(EnrichWithMetadata(pipeline_run_id))
            | "WriteBQ" >> WriteToBigQuery(
                table=bq_table,
                schema=INVENTORY_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


def run_streaming_pipeline(
    project_id: str,
    pubsub_subscription: str,
    bq_table: str,
    pipeline_run_id: str,
    pipeline_options: PipelineOptions,
):
    """Streaming pipeline: Pub/Sub → parse → validate → enrich → BigQuery."""
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadPubSub" >> ReadFromPubSub(subscription=pubsub_subscription)
            | "Parse" >> beam.ParDo(ParsePubSubEvent())
            | "Validate" >> beam.ParDo(ValidateInventoryRecord())
            | "Enrich" >> beam.ParDo(EnrichWithMetadata(pipeline_run_id))
            | "WriteBQ" >> WriteToBigQuery(
                table=bq_table,
                schema=INVENTORY_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description="ERP Dataflow ETL Pipeline")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--gcs_input", default="gs://BUCKET/raw/prod/sage100/inventory/**/*.parquet")
    parser.add_argument("--pubsub_subscription", default="projects/PROJECT/subscriptions/inventory-events-dataflow-dev")
    parser.add_argument("--bq_table", default="PROJECT.staging_dev.inventory_transactions")
    parser.add_argument("--pipeline_run_id", default=None)
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--temp_location", default="gs://BUCKET/dataflow/temp")
    parser.add_argument("--staging_location", default="gs://BUCKET/dataflow/staging")

    known_args, pipeline_args = parser.parse_known_args(argv)

    run_id = known_args.pipeline_run_id or f"df-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    options = PipelineOptions(
        pipeline_args,
        project=known_args.project_id,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        streaming=(known_args.mode == "streaming"),
    )
    options.view_as(SetupOptions).save_main_session = True

    if known_args.mode == "batch":
        run_batch_pipeline(
            known_args.project_id,
            known_args.gcs_input,
            known_args.bq_table,
            run_id,
            options,
        )
    else:
        run_streaming_pipeline(
            known_args.project_id,
            known_args.pubsub_subscription,
            known_args.bq_table,
            run_id,
            options,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
