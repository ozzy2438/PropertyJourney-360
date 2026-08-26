#!/usr/bin/env python3
"""Export version-controlled JSON Schemas from the executable Pydantic contracts."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from propertyjourney360.domain.models import IdentityRecord, MatchResult
from propertyjourney360.events.models import PropertyEvent
from propertyjourney360.evidence import (
    IdentityEvaluationReport,
    LocalEvidenceReport,
    PublicSourceEvidenceReport,
)
from propertyjourney360.governance.reconciliation import ReconciliationResult
from propertyjourney360.governance.remediation import RemediationCase
from propertyjourney360.ingestion.landing import LandingManifest
from propertyjourney360.pipeline import LocalPipelineReport
from propertyjourney360.public_data.catalog import SourceCatalog

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "schemas" / "json"
MODELS: dict[str, type[BaseModel]] = {
    "identity-record-v1": IdentityRecord,
    "identity-match-result-v1": MatchResult,
    "identity-evaluation-report-v1": IdentityEvaluationReport,
    "landing-manifest-v1": LandingManifest,
    "local-evidence-report-v1": LocalEvidenceReport,
    "local-pipeline-report-v1": LocalPipelineReport,
    "property-event-v1-v2": PropertyEvent,
    "public-source-evidence-report-v1": PublicSourceEvidenceReport,
    "reconciliation-result-v1": ReconciliationResult,
    "remediation-case-v1": RemediationCase,
    "source-catalog-v1": SourceCatalog,
}


def main() -> None:
    TARGET.mkdir(parents=True, exist_ok=True)
    for filename, model in MODELS.items():
        payload = json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n"
        (TARGET / f"{filename}.schema.json").write_text(payload)
    print(f"exported {len(MODELS)} schemas to {TARGET.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
