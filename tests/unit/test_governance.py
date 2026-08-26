import pytest

from propertyjourney360.domain.models import RemediationStatus
from propertyjourney360.governance.reconciliation import ReconciliationStatus, reconcile
from propertyjourney360.governance.remediation import open_case, transition_case


def test_reconciliation_passes_only_when_every_record_is_accounted_for() -> None:
    result = reconcile(
        run_id="run-1",
        dataset="contacts",
        source=100,
        accepted=90,
        quarantined=4,
        rejected=1,
        explained_duplicates=5,
    )
    assert result.status is ReconciliationStatus.PASSED
    assert result.counts.difference == 0


def test_reconciliation_failure_blocks_publication() -> None:
    result = reconcile(
        run_id="run-2",
        dataset="contacts",
        source=100,
        accepted=99,
        quarantined=0,
        rejected=0,
        explained_duplicates=0,
    )
    assert result.status is ReconciliationStatus.FAILED
    assert "publication must remain blocked" in result.explanation


def test_remediation_requires_ordered_replay_and_verification_evidence() -> None:
    case = open_case(
        source_system="legacy_crm",
        source_record_id="123",
        issue_type="MALFORMED_EMAIL",
        actor="dq-worker",
        note="Quarantined by email contract.",
    )
    case = transition_case(
        case,
        to_status=RemediationStatus.IN_REVIEW,
        actor="data-steward",
        note="Owner assigned.",
    )
    case = transition_case(
        case,
        to_status=RemediationStatus.RESOLVED,
        actor="data-steward",
        note="Corrected source value.",
    )

    with pytest.raises(ValueError, match="replay_run_id"):
        transition_case(
            case,
            to_status=RemediationStatus.REPLAYED,
            actor="airflow",
            note="Replay requested.",
        )

    case = transition_case(
        case,
        to_status=RemediationStatus.REPLAYED,
        actor="airflow",
        note="Reprocessed original source payload.",
        evidence={"replay_run_id": "replay-123"},
    )
    case = transition_case(
        case,
        to_status=RemediationStatus.VERIFIED,
        actor="dq-worker",
        note="Record passed contract and reconciliation.",
        evidence={"verification_result_id": "dq-456"},
    )
    case = transition_case(
        case,
        to_status=RemediationStatus.CLOSED,
        actor="data-steward",
        note="Case closed after independent verification.",
    )

    assert case.status is RemediationStatus.CLOSED
    assert len(case.events) == 6
