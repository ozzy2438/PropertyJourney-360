from datetime import UTC, datetime

import pytest
from google.api_core.exceptions import Conflict

from propertyjourney360.domain.models import RemediationStatus
from propertyjourney360.governance.remediation_store import (
    BigQueryRemediationStore,
    validate_transition_evidence,
)


def test_cloud_transition_evidence_is_fail_closed() -> None:
    with pytest.raises(ValueError, match="OPEN cases"):
        validate_transition_evidence(
            to_status=RemediationStatus.OPEN,
            resolution_action=None,
            replay_id=None,
            verification_result_id=None,
        )
    assert (
        validate_transition_evidence(
            to_status=RemediationStatus.IN_REVIEW,
            resolution_action=None,
            replay_id=None,
            verification_result_id=None,
        )
        is RemediationStatus.OPEN
    )
    with pytest.raises(ValueError, match="resolution-action"):
        validate_transition_evidence(
            to_status=RemediationStatus.RESOLVED,
            resolution_action=None,
            replay_id=None,
            verification_result_id=None,
        )
    with pytest.raises(ValueError, match="replay-id"):
        validate_transition_evidence(
            to_status=RemediationStatus.REPLAYED,
            resolution_action=None,
            replay_id=None,
            verification_result_id=None,
        )
    with pytest.raises(ValueError, match="verification-result-id"):
        validate_transition_evidence(
            to_status=RemediationStatus.VERIFIED,
            resolution_action=None,
            replay_id="replay-1",
            verification_result_id=None,
        )


class FakeQueryJob:
    def __init__(self) -> None:
        self.awaited = False
        self.job_id = "job-remediation-1"
        self.job_type = "query"
        self.project = "pj360-valid"
        self.location = "australia-southeast1"
        self.query = ""
        self.use_legacy_sql = False
        self.query_parameters: list[object] = []
        self.errors: list[dict[str, str]] = []

    def result(self) -> list[dict[str, datetime]]:
        self.awaited = True
        return [{"occurred_at": datetime(2026, 8, 24, tzinfo=UTC)}]


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.job = FakeQueryJob()

    def query(self, sql: str, **kwargs: object) -> FakeQueryJob:
        self.calls.append({"sql": sql, **kwargs})
        config = kwargs["job_config"]
        self.job.job_id = str(kwargs["job_id"])
        self.job.query = sql
        self.job.use_legacy_sql = config.use_legacy_sql  # type: ignore[attr-defined]
        self.job.query_parameters = config.query_parameters  # type: ignore[attr-defined]
        return self.job


class ExistingJobClient(FakeBigQueryClient):
    def query(self, sql: str, **kwargs: object) -> FakeQueryJob:
        self.calls.append({"sql": sql, **kwargs})
        config = kwargs["job_config"]
        self.job.job_id = str(kwargs["job_id"])
        self.job.query = sql
        self.job.use_legacy_sql = config.use_legacy_sql  # type: ignore[attr-defined]
        self.job.query_parameters = config.query_parameters  # type: ignore[attr-defined]
        raise Conflict("job already exists")

    def get_job(self, job_id: str, **kwargs: object) -> FakeQueryJob:
        self.calls.append({"get_job": job_id, **kwargs})
        return self.job


def test_bigquery_remediation_store_validates_identifiers_and_writes_transition() -> None:
    with pytest.raises(ValueError, match="project"):
        BigQueryRemediationStore(
            project_id="INVALID", governance_dataset="governance", location="australia"
        )
    with pytest.raises(ValueError, match="dataset"):
        BigQueryRemediationStore(
            project_id="pj360-valid", governance_dataset="bad-dataset", location="australia"
        )
    with pytest.raises(ValueError, match="location"):
        BigQueryRemediationStore(
            project_id="pj360-valid", governance_dataset="governance", location=""
        )

    client = FakeBigQueryClient()
    store = BigQueryRemediationStore(
        project_id="pj360-valid",
        governance_dataset="governance",
        location="australia-southeast1",
        client=client,
    )
    with pytest.raises(ValueError, match="64-character"):
        store.transition(
            remediation_case_id="bad",
            to_status=RemediationStatus.IN_REVIEW,
            actor="operator",
            note="review",
        )
    with pytest.raises(ValueError, match="actor and note"):
        store.transition(
            remediation_case_id="a" * 64,
            to_status=RemediationStatus.IN_REVIEW,
            actor=" ",
            note="review",
        )

    result = store.transition(
        remediation_case_id="a" * 64,
        to_status=RemediationStatus.RESOLVED,
        actor=" operator ",
        note=" corrected source record ",
        resolution_action="normalised verified email",
    )
    assert result.from_status is RemediationStatus.IN_REVIEW
    assert result.to_status is RemediationStatus.RESOLVED
    assert result.job_id == client.calls[0]["job_id"]
    assert client.job.awaited
    sql = str(client.calls[0]["sql"])
    assert sql.index("WHERE event_id = @event_id") < sql.index("ASSERT observed_status")
    assert "BEGIN TRANSACTION" in sql
    assert "COMMIT TRANSACTION" in sql
    assert "SET updated_at = updated_at" in sql
    assert "ASSERT observed_status = @from_status" in sql
    assert client.calls[0]["job_id"] == f"pj360_remediation_{result.event_id}"
    assert "job_id_prefix" not in client.calls[0]


def test_bigquery_remediation_retry_recovers_the_existing_deterministic_job() -> None:
    client = ExistingJobClient()
    store = BigQueryRemediationStore(
        project_id="pj360-valid",
        governance_dataset="governance",
        location="australia-southeast1",
        client=client,
    )
    result = store.transition(
        remediation_case_id="a" * 64,
        to_status=RemediationStatus.IN_REVIEW,
        actor="operator",
        note="reviewing evidence",
    )

    assert client.calls[1]["get_job"] == f"pj360_remediation_{result.event_id}"
    assert client.calls[1]["project"] == "pj360-valid"
    assert result.job_id == client.calls[0]["job_id"]


def test_bigquery_remediation_retry_rejects_a_mismatched_existing_job() -> None:
    client = ExistingJobClient()
    store = BigQueryRemediationStore(
        project_id="pj360-valid",
        governance_dataset="governance",
        location="australia-southeast1",
        client=client,
    )
    original_get_job = client.get_job

    def mismatched_get_job(job_id: str, **kwargs: object) -> FakeQueryJob:
        job = original_get_job(job_id, **kwargs)
        job.query = "SELECT 'unrelated'"
        return job

    client.get_job = mismatched_get_job  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="query"):
        store.transition(
            remediation_case_id="a" * 64,
            to_status=RemediationStatus.IN_REVIEW,
            actor="operator",
            note="reviewing evidence",
        )
