"""Deterministic BigQuery load jobs for content-addressed GCS objects."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from google.api_core.exceptions import Conflict
from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.ingestion.landing import LandingManifest

_TABLE_REFERENCE = re.compile(
    r"^[a-z][a-z0-9-]{4,28}[a-z0-9]\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$"
)


class BigQueryLoadResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str
    table: str
    source_uri: str
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    input_rows: int = Field(ge=0)
    output_rows: int = Field(ge=0)


class BigQueryJsonLoader:
    """Submit replay-safe JSONL loads using a content-derived BigQuery job ID."""

    def __init__(self, *, client: Any | None = None, location: str) -> None:
        if not location:
            raise ValueError("location is required")
        if client is None:
            import google.cloud.bigquery as bigquery

            client = bigquery.Client()
        self.client = client
        self.location = location

    def load(
        self,
        manifest: LandingManifest,
        *,
        table: str,
        allow_field_addition: bool = False,
    ) -> BigQueryLoadResult:
        if not manifest.object_path.startswith("gs://"):
            raise ValueError("BigQuery loads require a gs:// landing object")
        if not _TABLE_REFERENCE.fullmatch(table):
            raise ValueError("table must use project.dataset.table form")

        import google.cloud.bigquery as bigquery

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=False,
            max_bad_records=0,
        )
        if allow_field_addition:
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ]
        job_key = f"{table}|{manifest.sha256}|{manifest.schema_version}"
        job_id = f"pj360_{hashlib.sha256(job_key.encode()).hexdigest()[:48]}"
        project_id = table.split(".", maxsplit=1)[0]
        try:
            job = self.client.load_table_from_uri(
                manifest.object_path,
                table,
                job_id=job_id,
                job_config=job_config,
                location=self.location,
            )
        except Conflict:
            job = self.client.get_job(
                job_id,
                project=project_id,
                location=self.location,
            )
            self._validate_recovered_job(
                job,
                job_id=job_id,
                project_id=project_id,
                table=table,
                source_uri=manifest.object_path,
                allow_field_addition=allow_field_addition,
                bigquery=bigquery,
            )
        job.result()
        if job.errors:
            raise RuntimeError(f"BigQuery load job {job_id} failed: {job.errors}")
        if job.output_rows is None:
            raise RuntimeError(f"BigQuery load job {job_id} did not report output rows")
        output_rows = int(job.output_rows)
        if output_rows != manifest.record_count:
            raise RuntimeError(
                f"BigQuery load job {job_id} output-row mismatch: "
                f"expected {manifest.record_count}, observed {output_rows}"
            )
        return BigQueryLoadResult(
            job_id=job_id,
            table=table,
            source_uri=manifest.object_path,
            source_sha256=manifest.sha256,
            input_rows=manifest.record_count,
            output_rows=output_rows,
        )

    def _validate_recovered_job(
        self,
        job: Any,
        *,
        job_id: str,
        project_id: str,
        table: str,
        source_uri: str,
        allow_field_addition: bool,
        bigquery: Any,
    ) -> None:
        expected_schema_options = (
            (bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,) if allow_field_addition else ()
        )
        observed_destination = str(job.destination) if job.destination is not None else None
        observed_source_uris = tuple(job.source_uris or ())
        observed_schema_options = tuple(job.schema_update_options or ())
        observed_location = str(job.location).lower() if job.location is not None else None
        mismatches = {
            "job_id": (job.job_id, job_id),
            "project": (job.project, project_id),
            "location": (observed_location, self.location.lower()),
            "source_uris": (observed_source_uris, (source_uri,)),
            "destination": (observed_destination, table),
            "source_format": (
                job.source_format,
                bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
            "write_disposition": (
                job.write_disposition,
                bigquery.WriteDisposition.WRITE_APPEND,
            ),
            "ignore_unknown_values": (job.ignore_unknown_values, False),
            "max_bad_records": (job.max_bad_records, 0),
            "schema_update_options": (
                observed_schema_options,
                expected_schema_options,
            ),
        }
        mismatched_fields = [
            field for field, (observed, expected) in mismatches.items() if observed != expected
        ]
        if mismatched_fields:
            raise RuntimeError(
                "recovered BigQuery load job does not match the requested load: "
                + ", ".join(mismatched_fields)
            )
