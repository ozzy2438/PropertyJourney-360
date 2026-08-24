"""Append-only remediation workflow with replay and verification gates."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.domain.models import RemediationStatus


class RemediationEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    from_status: RemediationStatus | None
    to_status: RemediationStatus
    actor: str
    occurred_at: datetime
    note: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class RemediationCase(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    case_id: UUID = Field(default_factory=uuid4)
    source_system: str
    source_record_id: str
    issue_type: str
    status: RemediationStatus = RemediationStatus.OPEN
    opened_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    events: tuple[RemediationEvent, ...] = ()


_TRANSITIONS: dict[RemediationStatus, RemediationStatus] = {
    RemediationStatus.OPEN: RemediationStatus.IN_REVIEW,
    RemediationStatus.IN_REVIEW: RemediationStatus.RESOLVED,
    RemediationStatus.RESOLVED: RemediationStatus.REPLAYED,
    RemediationStatus.REPLAYED: RemediationStatus.VERIFIED,
    RemediationStatus.VERIFIED: RemediationStatus.CLOSED,
}


def open_case(
    *,
    source_system: str,
    source_record_id: str,
    issue_type: str,
    actor: str,
    note: str,
    case_id: UUID | None = None,
    opened_at: datetime | None = None,
) -> RemediationCase:
    effective_opened_at = opened_at or datetime.now(UTC)
    event = RemediationEvent(
        from_status=None,
        to_status=RemediationStatus.OPEN,
        actor=actor,
        occurred_at=effective_opened_at,
        note=note,
    )
    return RemediationCase(
        case_id=case_id or uuid4(),
        source_system=source_system,
        source_record_id=source_record_id,
        issue_type=issue_type,
        opened_at=effective_opened_at,
        events=(event,),
    )


def transition_case(
    case: RemediationCase,
    *,
    to_status: RemediationStatus,
    actor: str,
    note: str,
    evidence: dict[str, Any] | None = None,
) -> RemediationCase:
    """Return the next immutable state or reject skips/reopens.

    Replay and verification transitions require concrete identifiers so a human status update cannot
    masquerade as execution evidence.
    """

    expected = _TRANSITIONS.get(case.status)
    if expected is None:
        raise ValueError("closed remediation cases cannot be transitioned")
    if to_status is not expected:
        raise ValueError(f"invalid transition {case.status.value} -> {to_status.value}")

    supplied_evidence = evidence or {}
    if to_status is RemediationStatus.REPLAYED and not supplied_evidence.get("replay_run_id"):
        raise ValueError("REPLAYED requires replay_run_id evidence")
    if to_status is RemediationStatus.VERIFIED and not supplied_evidence.get(
        "verification_result_id"
    ):
        raise ValueError("VERIFIED requires verification_result_id evidence")

    event = RemediationEvent(
        from_status=case.status,
        to_status=to_status,
        actor=actor,
        occurred_at=datetime.now(UTC),
        note=note,
        evidence=supplied_evidence,
    )
    return case.model_copy(update={"status": to_status, "events": (*case.events, event)})
