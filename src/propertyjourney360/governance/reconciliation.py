"""Conservation-of-records reconciliation for meaningful pipeline runs."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, computed_field


class ReconciliationStatus(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"


class ReconciliationCounts(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: int = Field(ge=0)
    accepted: int = Field(ge=0)
    quarantined: int = Field(ge=0)
    rejected: int = Field(ge=0)
    explained_duplicates: int = Field(ge=0)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def accounted(self) -> int:
        return self.accepted + self.quarantined + self.rejected + self.explained_duplicates

    @computed_field  # type: ignore[prop-decorator]
    @property
    def difference(self) -> int:
        return self.source - self.accounted


class ReconciliationResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    dataset: str
    status: ReconciliationStatus
    counts: ReconciliationCounts
    checked_at: datetime
    explanation: str


def reconcile(
    *,
    run_id: str,
    dataset: str,
    source: int,
    accepted: int,
    quarantined: int,
    rejected: int,
    explained_duplicates: int,
) -> ReconciliationResult:
    counts = ReconciliationCounts(
        source=source,
        accepted=accepted,
        quarantined=quarantined,
        rejected=rejected,
        explained_duplicates=explained_duplicates,
    )
    if counts.difference == 0:
        status = ReconciliationStatus.PASSED
        explanation = "All source records are accounted for exactly once."
    else:
        status = ReconciliationStatus.FAILED
        explanation = (
            f"Source conservation failed: difference={counts.difference}; "
            "curated publication must remain blocked."
        )
    return ReconciliationResult(
        run_id=run_id,
        dataset=dataset,
        status=status,
        counts=counts,
        checked_at=datetime.now(UTC),
        explanation=explanation,
    )
