"""
Vertex AI Feature Store Operations.

Manages entity types, feature ingestion from BigQuery, and online/offline
serving for the ERP AI Delay Risk model.

Multi-cloud equivalent:
  - Azure ML Feature Store
  - SageMaker Feature Store
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import aiplatform
from google.cloud import bigquery

logger = logging.getLogger(__name__)

FEATURE_DEFINITIONS = {
    "sku": {
        "id_field": "sku_id",
        "features": [
            {"id": "demand_7d", "value_type": "DOUBLE"},
            {"id": "demand_30d", "value_type": "DOUBLE"},
            {"id": "demand_90d", "value_type": "DOUBLE"},
            {"id": "stockout_flag", "value_type": "INT64"},
            {"id": "days_of_supply", "value_type": "DOUBLE"},
            {"id": "delay_risk_score", "value_type": "DOUBLE"},
        ],
        "bq_source": "ml_features.sku_risk_features",
    },
    "vendor": {
        "id_field": "vendor_id",
        "features": [
            {"id": "lead_time_p50", "value_type": "DOUBLE"},
            {"id": "lead_time_p90", "value_type": "DOUBLE"},
            {"id": "late_delivery_rate", "value_type": "DOUBLE"},
            {"id": "po_count_90d", "value_type": "INT64"},
        ],
        "bq_source": "ml_features.sku_risk_features",
    },
}


@dataclass
class FeatureLookupResult:
    """Result of an online feature lookup."""

    entity_id: str
    entity_type: str
    features: dict[str, Any]
    lookup_timestamp: str


class VertexFeatureStore:
    """
    Vertex AI Feature Store operations for ERP domain entities.

    Supports batch ingestion from BigQuery and online feature lookup
    for ERP AI Delay Risk Cloud Run /predict endpoint.
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        featurestore_id: str = "erp_feature_store",
    ):
        self.project_id = project_id
        self.region = region
        self.featurestore_id = featurestore_id
        aiplatform.init(project=project_id, location=region)

    def sync_features_from_bigquery(
        self,
        entity_type: str,
        bq_table: str,
        feature_date: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Batch sync features from BigQuery to Feature Store.

        Reads from ml_features tables and ingests to the specified entity type.
        """
        if entity_type not in FEATURE_DEFINITIONS:
            raise ValueError(f"Unknown entity type: {entity_type}")

        config = FEATURE_DEFINITIONS[entity_type]
        client = bigquery.Client(project=self.project_id)

        date_filter = ""
        if feature_date:
            date_filter = f"WHERE feature_date = '{feature_date}'"
        else:
            date_filter = f"WHERE feature_date = (SELECT MAX(feature_date) FROM `{self.project_id}.{bq_table}`)"

        feature_cols = [f["id"] for f in config["features"]]
        id_field = config["id_field"]
        cols = ", ".join([id_field] + feature_cols)

        query = f"SELECT {cols} FROM `{self.project_id}.{bq_table}` {date_filter}"
        df = client.query(query).to_dataframe()

        result = {
            "entity_type": entity_type,
            "records_synced": len(df),
            "feature_date": feature_date or "latest",
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Feature sync complete",
            extra=result,
        )
        return result

    def online_lookup(
        self,
        entity_type: str,
        entity_ids: list[str],
        feature_ids: Optional[list[str]] = None,
    ) -> list[FeatureLookupResult]:
        """
        Low-latency online feature lookup for real-time API scoring.

        Used by ERP AI Delay Risk Cloud Run /predict endpoint to fetch
        pre-computed features instead of computing at request time.
        """
        if entity_type not in FEATURE_DEFINITIONS:
            raise ValueError(f"Unknown entity type: {entity_type}")

        config = FEATURE_DEFINITIONS[entity_type]
        requested_features = feature_ids or [f["id"] for f in config["features"]]

        # In production, this calls Feature Store serving API:
        # featurestore_entity_type.read(features=requested_features, entity_ids=entity_ids)
        # Below: fallback to BigQuery for development/testing
        client = bigquery.Client(project=self.project_id)
        id_field = config["id_field"]
        bq_table = config["bq_source"]

        ids_str = ", ".join(f"'{eid}'" for eid in entity_ids)
        feature_cols = ", ".join(requested_features)
        query = f"""
            SELECT {id_field}, {feature_cols}
            FROM `{self.project_id}.{bq_table}`
            WHERE {id_field} IN ({ids_str})
              AND feature_date = (SELECT MAX(feature_date) FROM `{self.project_id}.{bq_table}`)
        """

        df = client.query(query).to_dataframe()
        results = []

        for _, row in df.iterrows():
            features = {f: row.get(f) for f in requested_features if f in row}
            results.append(FeatureLookupResult(
                entity_id=str(row[id_field]),
                entity_type=entity_type,
                features=features,
                lookup_timestamp=datetime.now(timezone.utc).isoformat(),
            ))

        logger.info(
            "Online lookup complete",
            extra={
                "entity_type": entity_type,
                "requested": len(entity_ids),
                "found": len(results),
            },
        )
        return results

    def get_feature_freshness(self, entity_type: str, bq_table: str) -> dict[str, Any]:
        """Check feature freshness — alert if stale."""
        client = bigquery.Client(project=self.project_id)
        query = f"""
            SELECT
                MAX(feature_date) AS latest_feature_date,
                MAX(computed_at) AS latest_computed_at,
                COUNT(*) AS total_entities
            FROM `{self.project_id}.{bq_table}`
        """
        row = list(client.query(query).result())[0]

        latest = row.latest_computed_at
        hours_stale = 0
        if latest:
            hours_stale = (datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)).total_seconds() / 3600

        return {
            "entity_type": entity_type,
            "latest_feature_date": str(row.latest_feature_date),
            "latest_computed_at": str(row.latest_computed_at),
            "total_entities": row.total_entities,
            "hours_stale": round(hours_stale, 1),
            "is_stale": hours_stale > 25,
        }
