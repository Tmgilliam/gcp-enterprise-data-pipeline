"""
BigQuery Transformation Module — SQL-based dataset layer management.

Manages transformations between raw → staging → analytics → ml_features
dataset layers. Executes parameterized SQL files as BigQuery jobs.

Multi-cloud equivalent:
  - Azure Synapse SQL pools with stored procedures
  - AWS Redshift with scheduled SQL transformations via Glue
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"
WRITE_DISPOSITIONS = {
    "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
    "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
    "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
}


@dataclass
class TableStats:
    """Statistics for a BigQuery table."""

    table_id: str
    row_count: int
    size_bytes: int
    last_modified: Optional[datetime]
    num_partitions: Optional[int] = None


class BQTransformer:
    """
    Manages SQL-based transformations between BigQuery dataset layers.

    Dataset layers:
        raw → staging → analytics → ml_features
    """

    def __init__(
        self,
        project_id: str,
        dataset_raw: str = "raw",
        dataset_staging: str = "staging",
        dataset_analytics: str = "analytics",
        dataset_ml_features: str = "ml_features",
        location: str = "US",
    ):
        self.project_id = project_id
        self.dataset_raw = dataset_raw
        self.dataset_staging = dataset_staging
        self.dataset_analytics = dataset_analytics
        self.dataset_ml_features = dataset_ml_features
        self.location = location
        self._client = bigquery.Client(project=project_id, location=location)

    def _resolve_sql_params(
        self,
        sql_content: str,
        extra_params: Optional[dict[str, str]] = None,
    ) -> str:
        """Substitute template variables in SQL files."""
        params = {
            "project_id": self.project_id,
            "dataset_raw": self.dataset_raw,
            "dataset_staging": self.dataset_staging,
            "dataset_analytics": self.dataset_analytics,
            "dataset_ml_features": self.dataset_ml_features,
            "pipeline_run_id": f"bq-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        }
        if extra_params:
            params.update(extra_params)

        resolved = sql_content
        for key, value in params.items():
            resolved = resolved.replace(f"{{{key}}}", value)
        return resolved

    def run_transformation(
        self,
        sql_file_path: str,
        destination_table: Optional[str] = None,
        write_disposition: str = "WRITE_TRUNCATE",
        query_params: Optional[dict[str, str]] = None,
        timeout_seconds: int = 3600,
    ) -> bigquery.job.QueryJob:
        """
        Execute a SQL file as a BigQuery job.

        Args:
            sql_file_path: Path to .sql file (absolute or relative to sql/ dir).
            destination_table: Optional override for destination (project.dataset.table).
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY.
            query_params: Additional template parameters for SQL substitution.
            timeout_seconds: Job timeout.

        Returns:
            Completed QueryJob.
        """
        sql_path = Path(sql_file_path)
        if not sql_path.exists():
            sql_path = SQL_DIR / sql_file_path
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        sql_content = sql_path.read_text(encoding="utf-8")
        resolved_sql = self._resolve_sql_params(sql_content, query_params)

        job_config = bigquery.QueryJobConfig(
            use_legacy_sql=False,
            write_disposition=WRITE_DISPOSITIONS.get(
                write_disposition, bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        )

        if destination_table:
            job_config.destination = destination_table
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

        logger.info(
            "Starting transformation",
            extra={
                "sql_file": str(sql_path),
                "destination": destination_table,
                "write_disposition": write_disposition,
            },
        )

        job = self._client.query(resolved_sql, job_config=job_config)
        job.result(timeout=timeout_seconds)

        logger.info(
            "Transformation complete",
            extra={
                "job_id": job.job_id,
                "bytes_processed": job.total_bytes_processed,
                "destination": destination_table,
            },
        )
        return job

    def validate_row_count(
        self,
        table: str,
        expected_min: int,
    ) -> bool:
        """
        Assert that a transformed table has at least N rows.

        Args:
            table: Fully qualified table ID (project.dataset.table).
            expected_min: Minimum expected row count.

        Returns:
            True if validation passes.

        Raises:
            ValueError: If row count is below threshold.
        """
        stats = self.get_table_stats(table)

        if stats.row_count < expected_min:
            raise ValueError(
                f"Row count validation failed for {table}: "
                f"got {stats.row_count}, expected minimum {expected_min}"
            )

        logger.info(
            "Row count validation passed",
            extra={
                "table": table,
                "row_count": stats.row_count,
                "expected_min": expected_min,
            },
        )
        return True

    def get_table_stats(self, table: str) -> TableStats:
        """
        Return row count, byte size, and last modified timestamp for a table.

        Args:
            table: Fully qualified table ID (project.dataset.table).

        Returns:
            TableStats dataclass.
        """
        try:
            bq_table = self._client.get_table(table)
        except NotFound:
            raise ValueError(f"Table not found: {table}")

        # Get accurate row count via INFORMATION_SCHEMA or table metadata
        row_count = bq_table.num_rows or 0

        # For partitioned tables, get partition count
        num_partitions = None
        if bq_table.time_partitioning or bq_table.range_partitioning:
            partition_query = f"""
                SELECT COUNT(DISTINCT partition_id) AS partition_count
                FROM `{table.split('.')[0]}.{table.split('.')[1]}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE table_name = '{table.split('.')[-1]}'
                  AND partition_id IS NOT NULL
                  AND partition_id != '__NULL__'
            """
            try:
                result = list(self._client.query(partition_query).result())
                if result:
                    num_partitions = result[0].partition_count
            except Exception:
                pass  # INFORMATION_SCHEMA may not be available in all contexts

        stats = TableStats(
            table_id=table,
            row_count=row_count,
            size_bytes=bq_table.num_bytes or 0,
            last_modified=bq_table.modified,
            num_partitions=num_partitions,
        )

        logger.info(
            "Table stats retrieved",
            extra={
                "table": table,
                "row_count": stats.row_count,
                "size_bytes": stats.size_bytes,
            },
        )
        return stats

    def apply_partition_filter(
        self,
        table: str,
        date_column: str,
        start_date: date,
        end_date: date,
        columns: str = "*",
        additional_filters: Optional[str] = None,
    ) -> str:
        """
        Generate a partition-filtered query for cost-efficient reads.

        Args:
            table: Fully qualified table ID.
            date_column: Partition column name.
            start_date: Filter start (inclusive).
            end_date: Filter end (inclusive).
            columns: Column selection (default: *).
            additional_filters: Optional AND clause (without leading AND).

        Returns:
            SQL query string with partition filter applied.
        """
        if not re.match(r"^[\w.*,\s]+$", columns):
            raise ValueError(f"Invalid column selection: {columns}")

        query = f"""
SELECT {columns}
FROM `{table}`
WHERE {date_column} BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
"""
        if additional_filters:
            query += f"  AND ({additional_filters})\n"

        logger.info(
            "Generated partition-filtered query",
            extra={
                "table": table,
                "date_column": date_column,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )
        return query.strip()

    def run_staging_pipeline(self, pipeline_run_id: Optional[str] = None) -> dict[str, Any]:
        """
        Execute the full staging → analytics → ml_features transformation chain.

        Returns:
            Summary dict with job IDs and validation results.
        """
        run_id = pipeline_run_id or f"pipeline-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        params = {"pipeline_run_id": run_id}
        results: dict[str, Any] = {"pipeline_run_id": run_id, "steps": []}

        transformations = [
            {
                "sql": "staging_inventory_transactions.sql",
                "table": f"{self.project_id}.{self.dataset_staging}.inventory_transactions",
                "min_rows": 1,
            },
            {
                "sql": "analytics_demand_signals.sql",
                "table": f"{self.project_id}.{self.dataset_analytics}.demand_signals_by_sku",
                "min_rows": 1,
            },
            {
                "sql": "ml_features_sku_risk.sql",
                "table": f"{self.project_id}.{self.dataset_ml_features}.sku_risk_features",
                "min_rows": 1,
            },
        ]

        for step in transformations:
            job = self.run_transformation(
                sql_file_path=step["sql"],
                destination_table=step["table"],
                query_params=params,
            )
            self.validate_row_count(step["table"], step["min_rows"])
            stats = self.get_table_stats(step["table"])

            results["steps"].append({
                "sql": step["sql"],
                "table": step["table"],
                "job_id": job.job_id,
                "row_count": stats.row_count,
                "size_bytes": stats.size_bytes,
            })

        logger.info("Staging pipeline complete", extra={"pipeline_run_id": run_id})
        return results
