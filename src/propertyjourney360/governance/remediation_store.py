"""Controlled append-only BigQuery persistence for remediation transitions."""

# Table references cannot be query parameters; constructor validation allowlists every identifier.
# ruff: noqa: S608

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from propertyjourney360.domain.models import RemediationStatus

_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_DATASET_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CASE_ID = re.compile(r"^[a-f0-9]{64}$")

_EXPECTED_FROM = {
    RemediationStatus.IN_REVIEW: RemediationStatus.OPEN,
    RemediationStatus.RESOLVED: RemediationStatus.IN_REVIEW,
    RemediationStatus.REPLAYED: RemediationStatus.RESOLVED,
    RemediationStatus.VERIFIED: RemediationStatus.REPLAYED,
    RemediationStatus.CLOSED: RemediationStatus.VERIFIED,
}


class RemediationTransitionResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    remediation_case_id: str
    from_status: RemediationStatus
    to_status: RemediationStatus
    occurred_at: datetime
    job_id: str


def validate_transition_evidence(
    *,
    to_status: RemediationStatus,
    resolution_action: str | None,
    replay_id: str | None,
    verification_result_id: str | None,
) -> RemediationStatus:
    if to_status is RemediationStatus.OPEN:
        raise ValueError("OPEN cases are created only from governed DQ issues")
    expected = _EXPECTED_FROM[to_status]
    if to_status is RemediationStatus.RESOLVED and not resolution_action:
        raise ValueError("RESOLVED requires --resolution-action")
    if to_status is RemediationStatus.REPLAYED and not replay_id:
        raise ValueError("REPLAYED requires --replay-id")
    if to_status is RemediationStatus.VERIFIED and not verification_result_id:
        raise ValueError("VERIFIED requires --verification-result-id")
    return expected


class BigQueryRemediationStore:
    """Append one legal event with deterministic retry and transactional predecessor checks."""

    def __init__(
        self,
        *,
        project_id: str,
        governance_dataset: str,
        location: str,
        client: Any | None = None,
    ) -> None:
        if not _PROJECT_ID.fullmatch(project_id):
            raise ValueError("invalid GCP project ID")
        if not _DATASET_ID.fullmatch(governance_dataset):
            raise ValueError("invalid BigQuery governance dataset ID")
        if not location:
            raise ValueError("BigQuery location is required")
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=project_id, location=location)
        self.client = client
        self.project_id = project_id
        self.governance_dataset = governance_dataset
        self.location = location

    def transition(
        self,
        *,
        remediation_case_id: str,
        to_status: RemediationStatus,
        actor: str,
        note: str,
        resolution_action: str | None = None,
        replay_id: str | None = None,
        verification_result_id: str | None = None,
    ) -> RemediationTransitionResult:
        if not _CASE_ID.fullmatch(remediation_case_id):
            raise ValueError("remediation_case_id must be a 64-character lowercase hash")
        if not actor.strip() or not note.strip():
            raise ValueError("actor and note are required")
        from_status = validate_transition_evidence(
            to_status=to_status,
            resolution_action=resolution_action,
            replay_id=replay_id,
            verification_result_id=verification_result_id,
        )
        occurred_at = datetime.now(UTC)
        event_material = (
            f"{remediation_case_id}|{from_status.value}|{to_status.value}|"
            f"{actor.strip()}|{note.strip()}|{resolution_action or ''}|"
            f"{replay_id or ''}|{verification_result_id or ''}"
        )
        event_id = hashlib.sha256(event_material.encode()).hexdigest()
        job_id = f"pj360_remediation_{event_id}"

        from google.api_core.exceptions import Conflict
        from google.cloud import bigquery

        case_table = f"`{self.project_id}.{self.governance_dataset}.remediation_case`"
        event_table = f"`{self.project_id}.{self.governance_dataset}.remediation_event`"
        sql = f"""
        DECLARE observed_status STRING;
        DECLARE persisted_at TIMESTAMP DEFAULT (
          SELECT occurred_at FROM {event_table} WHERE event_id = @event_id LIMIT 1
        );

        IF persisted_at IS NULL THEN
          BEGIN TRANSACTION;
          UPDATE {case_table}
          SET updated_at = updated_at
          WHERE remediation_case_id = @case_id;
          SET persisted_at = (
            SELECT occurred_at FROM {event_table} WHERE event_id = @event_id LIMIT 1
          );
          IF persisted_at IS NULL THEN
            SET observed_status = COALESCE(
              (
                SELECT to_status FROM {event_table}
                WHERE remediation_case_id = @case_id
                ORDER BY occurred_at DESC, event_id DESC LIMIT 1
              ),
              (SELECT status FROM {case_table} WHERE remediation_case_id = @case_id LIMIT 1)
            );
            ASSERT observed_status IS NOT NULL AS 'remediation case does not exist';
            ASSERT observed_status = @from_status AS 'illegal or stale remediation transition';
            INSERT INTO {event_table} (
              event_id, remediation_case_id, from_status, to_status, actor, note, occurred_at,
              resolution_action, replay_id, verification_result_id
            ) VALUES (
              @event_id, @case_id, @from_status, @to_status, @actor, @note, @occurred_at,
              @resolution_action, @replay_id, @verification_result_id
            );
            SET persisted_at = @occurred_at;
          END IF;
          COMMIT TRANSACTION;
        END IF;

        SELECT persisted_at AS occurred_at;
        """
        parameters = [
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
            bigquery.ScalarQueryParameter("case_id", "STRING", remediation_case_id),
            bigquery.ScalarQueryParameter("from_status", "STRING", from_status.value),
            bigquery.ScalarQueryParameter("to_status", "STRING", to_status.value),
            bigquery.ScalarQueryParameter("actor", "STRING", actor.strip()),
            bigquery.ScalarQueryParameter("note", "STRING", note.strip()),
            bigquery.ScalarQueryParameter("occurred_at", "TIMESTAMP", occurred_at),
            bigquery.ScalarQueryParameter("resolution_action", "STRING", resolution_action),
            bigquery.ScalarQueryParameter("replay_id", "STRING", replay_id),
            bigquery.ScalarQueryParameter(
                "verification_result_id", "STRING", verification_result_id
            ),
        ]
        job_config = bigquery.QueryJobConfig(
            query_parameters=parameters,
            use_legacy_sql=False,
        )
        try:
            job = self.client.query(
                sql,
                job_id=job_id,
                job_config=job_config,
                location=self.location,
            )
        except Conflict:
            job = self.client.get_job(
                job_id,
                project=self.project_id,
                location=self.location,
            )
            self._validate_recovered_job(
                job,
                job_id=job_id,
                sql=sql,
                parameters=parameters,
            )
        persisted = list(job.result())
        if job.errors:
            raise RuntimeError(f"BigQuery remediation job {job_id} failed: {job.errors}")
        if len(persisted) != 1 or not isinstance(persisted[0]["occurred_at"], datetime):
            raise RuntimeError("remediation transition did not return one persisted event")
        persisted_at = persisted[0]["occurred_at"]
        if persisted_at.tzinfo is None:
            persisted_at = persisted_at.replace(tzinfo=UTC)
        return RemediationTransitionResult(
            event_id=event_id,
            remediation_case_id=remediation_case_id,
            from_status=from_status,
            to_status=to_status,
            occurred_at=persisted_at.astimezone(UTC),
            job_id=job.job_id,
        )

    def _validate_recovered_job(
        self,
        job: Any,
        *,
        job_id: str,
        sql: str,
        parameters: list[Any],
    ) -> None:
        def parameter_contract(values: Any) -> tuple[str, ...]:
            if values is None:
                return ()
            return tuple(
                json.dumps(value.to_api_repr(), sort_keys=True, separators=(",", ":"))
                for value in values
            )

        observed_location = str(job.location).lower() if job.location is not None else None
        mismatches = {
            "job_type": (job.job_type, "query"),
            "job_id": (job.job_id, job_id),
            "project": (job.project, self.project_id),
            "location": (observed_location, self.location.lower()),
            "query": (job.query, sql),
            "use_legacy_sql": (job.use_legacy_sql, False),
            "query_parameters": (
                parameter_contract(job.query_parameters),
                parameter_contract(parameters),
            ),
        }
        mismatched_fields = [
            field for field, (observed, expected) in mismatches.items() if observed != expected
        ]
        if mismatched_fields:
            raise RuntimeError(
                "recovered BigQuery remediation job does not match the requested transition: "
                + ", ".join(mismatched_fields)
            )
