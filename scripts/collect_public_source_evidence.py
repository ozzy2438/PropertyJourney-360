#!/usr/bin/env python3
"""Summarise live public fetch manifests without committing raw source records."""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path

from propertyjourney360.evidence import (
    EvidenceLevel,
    PublicSourceEvidenceReport,
    PublicSourceObservation,
)
from propertyjourney360.public_data.catalog import load_catalog
from propertyjourney360.public_data.fetch import PublicFetchManifest

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REQUIRED = (
    "melbourne_clue_business_context",
    "rba_f1_interest_rates",
    "vicmap_address",
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, default=ROOT / "data" / "raw" / "public")
    parser.add_argument("--catalog", type=Path, default=ROOT / "config" / "sources.yml")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "evidence" / "public_source_validation.json",
    )
    parser.add_argument("--require", action="append", default=[])
    return parser


def main() -> int:
    args = _parser().parse_args()
    required = tuple(args.require) or DEFAULT_REQUIRED
    catalog = load_catalog(args.catalog)
    manifests = [
        PublicFetchManifest.model_validate_json(path.read_text())
        for path in args.raw_root.rglob("*.manifest.json")
    ]
    latest: dict[str, PublicFetchManifest] = {}
    for manifest in manifests:
        previous = latest.get(manifest.source_id)
        if previous is None or manifest.retrieved_at > previous.retrieved_at:
            latest[manifest.source_id] = manifest
    missing = sorted(set(required) - set(latest))
    if missing:
        raise SystemExit(f"missing required public fetch evidence: {', '.join(missing)}")

    observations = tuple(
        PublicSourceObservation(
            source_id=manifest.source_id,
            publisher=manifest.publisher,
            retrieved_at=manifest.retrieved_at,
            byte_count=manifest.byte_count,
            response_record_count=manifest.response_record_count,
            sha256=manifest.sha256,
            licence_name=manifest.licence_name,
            licence_url=manifest.licence_url,
            attribution_text=manifest.attribution_text,
            limitations=manifest.limitations,
        )
        for manifest in (latest[source_id] for source_id in sorted(required))
    )
    report = PublicSourceEvidenceReport(
        generated_at=datetime.now(UTC),
        catalog_sha256=hashlib.sha256(args.catalog.read_bytes()).hexdigest(),
        catalog_terms_checked_at=catalog.terms_checked_at.isoformat(),
        observations=observations,
        evidence_level=EvidenceLevel.LOCALLY_VERIFIED,
        scope_limitation=(
            "HTTP acquisition and local content hashes only; not GCS or BigQuery runtime evidence. "
            "Raw records remain outside Git and source licences continue to apply."
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report.model_dump_json(indent=2))
    print(f"wrote {args.output.relative_to(ROOT)} with {len(observations)} observations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
