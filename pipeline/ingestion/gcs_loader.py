"""
GCS Landing Zone Loader — ERP structured data ingestion.

Handles CSV, JSON, and Parquet files landing into the GCS raw zone with
metadata tracking, schema validation, and lifecycle zone transitions.

Multi-cloud equivalent:
  - Azure Blob Storage with metadata tags
  - AWS S3 with object tagging and lifecycle policies
"""

from __future__ import annotations

import json
import logging
import mimetypes
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = {".csv", ".json", ".parquet", ".jsonl"}
RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
ARCHIVE_PREFIX = "archive/"
STAGING_PREFIX = "staging/"


@dataclass
class SchemaDefinition:
    """Column schema for pre-ingestion validation."""

    columns: dict[str, str]  # column_name -> expected_type (string, int, float, date)
    required_columns: list[str] = field(default_factory=list)
    max_null_rate: float = 0.05  # maximum allowed null rate per required column


@dataclass
class FileMetadata:
    """Tracked metadata for an ingested file."""

    source_system: str
    ingestion_timestamp: str
    schema_version: str
    record_count: Optional[int] = None
    pipeline_run_id: Optional[str] = None
    original_filename: Optional[str] = None
    content_type: Optional[str] = None

    def to_gcs_metadata(self) -> dict[str, str]:
        """Convert to GCS object metadata (all values must be strings)."""
        meta = {
            "source_system": self.source_system,
            "ingestion_timestamp": self.ingestion_timestamp,
            "schema_version": self.schema_version,
        }
        if self.record_count is not None:
            meta["record_count"] = str(self.record_count)
        if self.pipeline_run_id:
            meta["pipeline_run_id"] = self.pipeline_run_id
        if self.original_filename:
            meta["original_filename"] = self.original_filename
        return meta


class GCSLoader:
    """
    Handles structured ERP data files landing into the GCS raw zone.

    Bucket structure:
        raw/{env}/{source_system}/{date}/{file}
        processed/{env}/{source_system}/{date}/{file}
        archive/{env}/{source_system}/{date}/{file}
    """

    def __init__(
        self,
        bucket_name: str,
        project_id: Optional[str] = None,
        max_retries: int = 5,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 32.0,
    ):
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self._client = storage.Client(project=project_id)
        self._bucket = self._client.bucket(bucket_name)

    def _retry_with_backoff(self, operation: Callable[[], Any], operation_name: str) -> Any:
        """Execute an operation with exponential backoff retry."""
        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                return operation()
            except (
                gcp_exceptions.ServiceUnavailable,
                gcp_exceptions.TooManyRequests,
                gcp_exceptions.InternalServerError,
                ConnectionError,
                TimeoutError,
            ) as exc:
                last_exception = exc
                if attempt == self.max_retries - 1:
                    break
                delay = min(
                    self.base_delay_seconds * (2 ** attempt),
                    self.max_delay_seconds,
                )
                logger.warning(
                    "Retry %d/%d for %s after %s: waiting %.1fs",
                    attempt + 1,
                    self.max_retries,
                    operation_name,
                    type(exc).__name__,
                    delay,
                )
                time.sleep(delay)

        logger.error("Operation %s failed after %d retries", operation_name, self.max_retries)
        raise last_exception  # type: ignore[misc]

    def upload_file(
        self,
        local_path: str,
        gcs_path: str,
        metadata: FileMetadata,
    ) -> str:
        """
        Upload a local file to GCS with source system metadata tags.

        Args:
            local_path: Path to local file.
            gcs_path: Destination path within bucket (e.g., raw/prod/sage100/2026-06-08/file.parquet).
            metadata: FileMetadata with source system tags.

        Returns:
            Full GCS URI (gs://bucket/path).
        """
        local = Path(local_path)
        if not local.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        suffix = local.suffix.lower()
        if suffix not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{suffix}'. Supported: {SUPPORTED_FORMATS}"
            )

        content_type = metadata.content_type or mimetypes.guess_type(local_path)[0]
        gcs_metadata = metadata.to_gcs_metadata()

        def _upload() -> str:
            blob = self._bucket.blob(gcs_path)
            blob.metadata = gcs_metadata
            blob.upload_from_filename(
                local_path,
                content_type=content_type,
            )
            uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(
                "Uploaded file",
                extra={
                    "gcs_uri": uri,
                    "source_system": metadata.source_system,
                    "record_count": metadata.record_count,
                    "schema_version": metadata.schema_version,
                },
            )
            return uri

        return self._retry_with_backoff(_upload, f"upload_file({gcs_path})")

    def list_unprocessed(self, prefix: str = RAW_PREFIX) -> list[str]:
        """
        List files in raw/ that have not yet been copied to processed/.

        Compares blob names by stripping the zone prefix (raw/ vs processed/).

        Args:
            prefix: GCS prefix to scan (default: raw/).

        Returns:
            List of GCS paths in raw/ without a corresponding processed/ copy.
        """
        def _list_blobs(p: str) -> set[str]:
            blobs = self._client.list_blobs(self.bucket_name, prefix=p)
            return {blob.name for blob in blobs if not blob.name.endswith("/")}

        raw_blobs = self._retry_with_backoff(
            lambda: _list_blobs(prefix), f"list_unprocessed({prefix})"
        )
        processed_blobs = self._retry_with_backoff(
            lambda: _list_blobs(PROCESSED_PREFIX), "list_processed"
        )

        # Map processed paths back to their raw equivalents
        processed_raw_equivalents = set()
        for proc_path in processed_blobs:
            if proc_path.startswith(PROCESSED_PREFIX):
                raw_equivalent = RAW_PREFIX + proc_path[len(PROCESSED_PREFIX):]
                processed_raw_equivalents.add(raw_equivalent)

        unprocessed = sorted(raw_blobs - processed_raw_equivalents)
        logger.info(
            "Found unprocessed files",
            extra={
                "raw_count": len(raw_blobs),
                "processed_count": len(processed_blobs),
                "unprocessed_count": len(unprocessed),
            },
        )
        return unprocessed

    def move_to_processed(self, gcs_path: str) -> str:
        """
        Copy file from raw/ to processed/ after successful pipeline run.

        The original raw/ file is retained for audit (lifecycle policy handles deletion).

        Args:
            gcs_path: Source path in raw/ zone.

        Returns:
            Destination path in processed/ zone.
        """
        if not gcs_path.startswith(RAW_PREFIX):
            raise ValueError(f"Expected path under {RAW_PREFIX}, got: {gcs_path}")

        dest_path = PROCESSED_PREFIX + gcs_path[len(RAW_PREFIX):]

        def _copy() -> str:
            source_blob = self._bucket.blob(gcs_path)
            if not source_blob.exists():
                raise FileNotFoundError(f"Source blob not found: {gcs_path}")

            metadata = dict(source_blob.metadata or {})
            metadata["processed_timestamp"] = datetime.now(timezone.utc).isoformat()
            metadata["processing_status"] = "success"

            self._bucket.copy_blob(source_blob, self._bucket, dest_path)
            dest_blob = self._bucket.blob(dest_path)
            dest_blob.metadata = metadata
            dest_blob.patch()

            logger.info(
                "Moved to processed",
                extra={"source": gcs_path, "destination": dest_path},
            )
            return dest_path

        return self._retry_with_backoff(_copy, f"move_to_processed({gcs_path})")

    def move_to_archive(self, gcs_path: str) -> str:
        """
        Move file to archive/ zone after retention period in processed/.

        Args:
            gcs_path: Source path (raw/ or processed/).

        Returns:
            Destination path in archive/ zone.
        """
        # Normalize to archive path regardless of source zone
        for zone_prefix in (RAW_PREFIX, PROCESSED_PREFIX, STAGING_PREFIX):
            if gcs_path.startswith(zone_prefix):
                relative = gcs_path[len(zone_prefix):]
                break
        else:
            relative = gcs_path

        dest_path = ARCHIVE_PREFIX + relative

        def _archive() -> str:
            source_blob = self._bucket.blob(gcs_path)
            if not source_blob.exists():
                raise FileNotFoundError(f"Source blob not found: {gcs_path}")

            metadata = dict(source_blob.metadata or {})
            metadata["archived_timestamp"] = datetime.now(timezone.utc).isoformat()

            self._bucket.copy_blob(source_blob, self._bucket, dest_path)
            dest_blob = self._bucket.blob(dest_path)
            dest_blob.metadata = metadata
            dest_blob.patch()
            source_blob.delete()

            logger.info(
                "Archived file",
                extra={"source": gcs_path, "destination": dest_path},
            )
            return dest_path

        return self._retry_with_backoff(_archive, f"move_to_archive({gcs_path})")

    def validate_file(self, gcs_path: str, schema: SchemaDefinition) -> dict[str, Any]:
        """
        Validate file schema before ingestion.

        Checks column names, types, and null rates against the schema definition.
        Supports CSV, JSON/JSONL, and Parquet formats.

        Args:
            gcs_path: Path to file in GCS.
            schema: SchemaDefinition with expected columns and constraints.

        Returns:
            Validation report dict with pass/fail status and details.
        """
        report: dict[str, Any] = {
            "gcs_path": gcs_path,
            "valid": True,
            "errors": [],
            "warnings": [],
            "column_stats": {},
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        suffix = Path(gcs_path).suffix.lower()

        try:
            if suffix == ".parquet":
                records = self._read_parquet_sample(gcs_path)
            elif suffix == ".csv":
                records = self._read_csv_sample(gcs_path)
            elif suffix in (".json", ".jsonl"):
                records = self._read_json_sample(gcs_path)
            else:
                report["valid"] = False
                report["errors"].append(f"Unsupported format for validation: {suffix}")
                return report
        except Exception as exc:
            report["valid"] = False
            report["errors"].append(f"Failed to read file: {exc}")
            return report

        if not records:
            report["valid"] = False
            report["errors"].append("File contains no records")
            return report

        actual_columns = set(records[0].keys())
        expected_columns = set(schema.columns.keys())

        missing = expected_columns - actual_columns
        if missing:
            report["valid"] = False
            report["errors"].append(f"Missing required columns: {sorted(missing)}")

        extra = actual_columns - expected_columns
        if extra:
            report["warnings"].append(f"Unexpected columns (will be ignored): {sorted(extra)}")

        # Validate required columns and null rates
        for col in schema.required_columns:
            if col not in actual_columns:
                continue
            null_count = sum(1 for r in records if r.get(col) is None or r.get(col) == "")
            null_rate = null_count / len(records)
            report["column_stats"][col] = {
                "null_count": null_count,
                "null_rate": round(null_rate, 4),
                "sample_size": len(records),
            }
            if null_rate > schema.max_null_rate:
                report["valid"] = False
                report["errors"].append(
                    f"Column '{col}' null rate {null_rate:.2%} exceeds "
                    f"threshold {schema.max_null_rate:.2%}"
                )

        # Type validation on sample
        for col, expected_type in schema.columns.items():
            if col not in actual_columns:
                continue
            type_errors = self._validate_column_types(records, col, expected_type)
            if type_errors:
                report["valid"] = False
                report["errors"].extend(type_errors[:5])  # cap error detail

        logger.info(
            "Validation complete",
            extra={
                "gcs_path": gcs_path,
                "valid": report["valid"],
                "error_count": len(report["errors"]),
            },
        )
        return report

    def _read_parquet_sample(self, gcs_path: str, max_rows: int = 1000) -> list[dict]:
        """Read a sample of rows from a Parquet file."""
        import io

        import pyarrow.parquet as pq

        blob = self._bucket.blob(gcs_path)
        data = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas().head(max_rows)
        return df.to_dict(orient="records")

    def _read_csv_sample(self, gcs_path: str, max_rows: int = 1000) -> list[dict]:
        """Read a sample of rows from a CSV file."""
        import io

        import pandas as pd

        blob = self._bucket.blob(gcs_path)
        data = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data), nrows=max_rows)
        return df.to_dict(orient="records")

    def _read_json_sample(self, gcs_path: str, max_rows: int = 1000) -> list[dict]:
        """Read a sample of records from JSON or JSONL file."""
        blob = self._bucket.blob(gcs_path)
        content = blob.download_as_text()

        if gcs_path.endswith(".jsonl"):
            records = []
            for line in content.strip().split("\n")[:max_rows]:
                records.append(json.loads(line))
            return records

        parsed = json.loads(content)
        if isinstance(parsed, list):
            return parsed[:max_rows]
        return [parsed]

    def _validate_column_types(
        self, records: list[dict], column: str, expected_type: str
    ) -> list[str]:
        """Validate column values match expected type on non-null sample."""
        errors = []
        type_checkers = {
            "string": lambda v: isinstance(v, str),
            "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            "date": lambda v: isinstance(v, str) and len(v) >= 8,
            "timestamp": lambda v: isinstance(v, str),
            "bool": lambda v: isinstance(v, bool),
        }
        checker = type_checkers.get(expected_type)
        if not checker:
            return [f"Unknown expected type '{expected_type}' for column '{column}'"]

        checked = 0
        for record in records:
            value = record.get(column)
            if value is None or value == "":
                continue
            if not checker(value):
                errors.append(
                    f"Column '{column}' expected type '{expected_type}', "
                    f"got {type(value).__name__} (value: {repr(value)[:50]})"
                )
                break
            checked += 1
            if checked >= 100:
                break

        return errors
