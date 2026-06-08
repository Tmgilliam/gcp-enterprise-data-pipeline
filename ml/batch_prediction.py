"""
Batch Prediction Job — Score ERP open orders at pipeline cadence.

Reads feature table from BigQuery, scores with registered Vertex AI model,
writes predictions back to BigQuery for ERP AI Delay Risk dashboard consumption.

Multi-cloud equivalent:
  - Vertex AI Batch Prediction / Azure ML Batch Endpoints / SageMaker Batch Transform
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import aiplatform, bigquery

logger = logging.getLogger(__name__)


class BatchPredictor:
    """
    Batch prediction for ERP delay risk scoring.

    Scores all SKUs in ml_features.sku_risk_features and writes
    predictions to analytics.delay_risk_scores for dashboard consumption.
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        model_name: str = "erp-delay-risk-classifier",
        feature_table: str = "ml_features.sku_risk_features",
        output_table: str = "analytics.delay_risk_scores",
    ):
        self.project_id = project_id
        self.region = region
        self.model_name = model_name
        self.feature_table = feature_table
        self.output_table = output_table
        aiplatform.init(project=project_id, location=region)
        self._bq_client = bigquery.Client(project=project_id)

    def run_batch_prediction(
        self,
        gcs_output_uri: str,
        machine_type: str = "n1-standard-4",
    ) -> dict[str, Any]:
        """
        Execute Vertex AI batch prediction job.

        Args:
            gcs_output_uri: GCS path for prediction output.
            machine_type: Compute type for batch job.

        Returns:
            Job summary dict.
        """
        models = aiplatform.Model.list(
            filter=f'display_name="{self.model_name}"',
            order_by="create_time desc",
        )
        if not models:
            raise ValueError(f"Model not found: {self.model_name}")

        model = models[0]
        input_uri = f"bq://{self.project_id}.{self.feature_table}"

        batch_job = model.batch_predict(
            job_display_name=f"delay-risk-batch-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            instances_format="bigquery",
            bigquery_source_input_uri=input_uri,
            predictions_format="jsonl",
            gcs_destination_output_uri_prefix=gcs_output_uri,
            machine_type=machine_type,
            sync=True,
        )

        result = {
            "job_id": batch_job.resource_name,
            "model": model.display_name,
            "input_table": input_uri,
            "output_uri": gcs_output_uri,
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info("Batch prediction complete", extra=result)
        return result

    def score_from_bigquery(self) -> dict[str, Any]:
        """
        Score directly in BigQuery using pre-computed delay_risk_score.

        Fallback for development when Vertex AI batch prediction is not available.
        Uses the delay_risk_score from ml_features.sku_risk_features.
        """
        query = f"""
            CREATE OR REPLACE TABLE `{self.project_id}.{self.output_table}`
            PARTITION BY score_date
            AS
            SELECT
                sku_id,
                vendor_id,
                delay_risk_score AS risk_score,
                CASE
                    WHEN delay_risk_score >= 0.7 THEN 'HIGH'
                    WHEN delay_risk_score >= 0.4 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS risk_level,
                supply_risk_flag,
                days_of_supply,
                lead_time_p90,
                CURRENT_DATE() AS score_date,
                CURRENT_TIMESTAMP() AS scored_at
            FROM `{self.project_id}.{self.feature_table}`
            WHERE feature_date = (
                SELECT MAX(feature_date) FROM `{self.project_id}.{self.feature_table}`
            )
        """

        job = self._bq_client.query(query)
        job.result()

        count_query = f"SELECT COUNT(*) AS cnt FROM `{self.project_id}.{self.output_table}`"
        count = list(self._bq_client.query(count_query).result())[0].cnt

        result = {
            "output_table": f"{self.project_id}.{self.output_table}",
            "records_scored": count,
            "method": "bigquery_direct",
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info("BigQuery scoring complete", extra=result)
        return result
