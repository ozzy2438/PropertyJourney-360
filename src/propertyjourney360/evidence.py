"""Machine-readable evidence contracts that keep targets separate from observations."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.identity.evaluation import ClusterMetrics, IdentityMetrics

_GENERATED_EVIDENCE = {
    Path("evidence/local_validation.json"),
    Path("evidence/local_pipeline_validation.json"),
    Path("evidence/platform_validation.json"),
    Path("evidence/coverage.json"),
    Path("evidence/identity_evaluation.json"),
    Path("evidence/public_source_validation.json"),
}


def hash_source_tree(root: Path) -> str:
    """Hash Git-visible source content while excluding self-referential evidence outputs."""
    git_executable = shutil.which("git")
    process = (
        subprocess.run(  # noqa: S603 - resolved Git executable; fixed read-only arguments
            (
                git_executable,
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "-z",
            ),
            cwd=root,
            capture_output=True,
            check=False,
        )
        if git_executable
        else None
    )
    if process is not None and process.returncode == 0:
        relative_paths = sorted(
            Path(value.decode()) for value in process.stdout.split(b"\0") if value
        )
    else:
        skipped_parts = {".git", ".venv", "__pycache__", "target", ".terraform", "data"}
        relative_paths = sorted(
            path.relative_to(root)
            for path in root.rglob("*")
            if path.is_file() and not skipped_parts.intersection(path.relative_to(root).parts)
        )

    digest = hashlib.sha256()
    for relative in relative_paths:
        path = root / relative
        if relative in _GENERATED_EVIDENCE or not path.is_file():
            continue
        digest.update(str(relative).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


class EvidenceLevel(StrEnum):
    DESIGNED = "DESIGNED"
    IMPLEMENTED = "IMPLEMENTED"
    LOCALLY_VERIFIED = "LOCALLY_VERIFIED"
    CLOUD_VERIFIED = "CLOUD_VERIFIED"
    SUSTAINED = "SUSTAINED"


class CheckResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    command: tuple[str, ...]
    exit_code: int
    duration_seconds: float = Field(ge=0)
    output_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_tail: str

    @property
    def passed(self) -> bool:
        return self.exit_code == 0


class EvidenceClaim(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    claim_id: str
    description: str
    level: EvidenceLevel
    target: str | float | int | None
    actual: str | float | int | None
    evidence_status: str
    limitation: str | None = None


class LocalEvidenceReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_version: int = 1
    generated_at: datetime
    environment: str
    python_version: str
    source_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    checks: tuple[CheckResult, ...]
    claims: tuple[EvidenceClaim, ...]
    measurements: dict[str, Any] = Field(default_factory=dict)


class ConfidenceInterval(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    confidence: float = Field(gt=0, lt=1)
    lower: float = Field(ge=0, le=1)
    upper: float = Field(ge=0, le=1)
    method: str


class IdentityEvaluationReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_version: int = 1
    generated_at: datetime
    ruleset_version: str
    evaluation_seed: int
    total_synthetic_customers: int = Field(ge=1)
    held_out_customer_count: int = Field(ge=1)
    held_out_source_identity_count: int = Field(ge=1)
    pair_metrics: IdentityMetrics
    cluster_metrics: ClusterMetrics
    cluster_conflict_count: int = Field(ge=0)
    cluster_precision_interval_95: ConfidenceInterval
    cluster_recall_interval_95: ConfidenceInterval
    source_hashes: dict[str, str]
    target_precision: float = Field(ge=0, le=1)
    target_recall: float = Field(ge=0, le=1)
    target_status: str
    evidence_level: EvidenceLevel
    scope_limitation: str


class PublicSourceObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    publisher: str
    retrieved_at: datetime
    byte_count: int = Field(ge=0)
    response_record_count: int | None = Field(default=None, ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    licence_name: str
    licence_url: str | None
    attribution_text: str | None
    limitations: tuple[str, ...]


class PublicSourceEvidenceReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_version: int = 1
    generated_at: datetime
    catalog_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    catalog_terms_checked_at: str
    observations: tuple[PublicSourceObservation, ...]
    evidence_level: EvidenceLevel
    scope_limitation: str
