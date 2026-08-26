"""Domain models shared across ingestion, identity resolution, governance and API layers."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceSystem(StrEnum):
    """Operational sources that can contribute a private customer identity."""

    SALESFORCE = "salesforce"
    LEGACY_CRM = "legacy_crm"
    SUPPORT = "support"
    WEB = "web"


class MatchDecision(StrEnum):
    """Permitted identity-candidate outcomes."""

    AUTO_MATCH = "AUTO_MATCH"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    NO_MATCH = "NO_MATCH"


class RemediationStatus(StrEnum):
    """Auditable remediation workflow states."""

    OPEN = "OPEN"
    IN_REVIEW = "IN_REVIEW"
    RESOLVED = "RESOLVED"
    REPLAYED = "REPLAYED"
    VERIFIED = "VERIFIED"
    CLOSED = "CLOSED"


class IdentityRecord(BaseModel):
    """A source identity before canonicalisation.

    ``truth_customer_id`` exists only in synthetic/evaluation datasets and must not be used by
    matching logic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_system: SourceSystem
    source_record_id: str = Field(min_length=1, max_length=256)
    source_updated_at: datetime
    email: str | None = None
    email_verified: bool = False
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    address_line: str | None = None
    suburb: str | None = None
    postcode: str | None = None
    trusted_cross_system_id: str | None = None
    truth_customer_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @property
    def source_key(self) -> str:
        return f"{self.source_system.value}:{self.source_record_id}"

    @model_validator(mode="after")
    def require_identity_signal(self) -> IdentityRecord:
        signals = (
            self.email,
            self.phone,
            self.first_name,
            self.last_name,
            self.address_line,
            self.trusted_cross_system_id,
        )
        if not any(value and value.strip() for value in signals):
            raise ValueError("identity record must contain at least one identity signal")
        return self


class MatchEvidence(BaseModel):
    """One explainable signal used to score an identity candidate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    field: str
    comparator: str
    left_normalized: str | None = None
    right_normalized: str | None = None
    similarity: float = Field(ge=0, le=1)
    weight: float = Field(ge=0, le=1)
    contribution: float = Field(ge=0, le=1)
    note: str | None = None


class MatchResult(BaseModel):
    """Versioned and fully explainable identity-candidate decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    left_source_key: str
    right_source_key: str
    score: float = Field(ge=0, le=1)
    decision: MatchDecision
    rule_version: str
    evidence: tuple[MatchEvidence, ...]
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def disallow_self_match(self) -> MatchResult:
        if self.left_source_key == self.right_source_key:
            raise ValueError("an identity cannot be matched to itself")
        return self
