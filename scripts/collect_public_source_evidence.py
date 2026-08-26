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
from propertyjourney360.public_data.fetch import (
    PublicFetchManifest,
    verify_public_fetch_manifest,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REQUIRED = (
    "abs_c21_g01_sa2_vic",
    "abs_c21_g02_sa2_vic",
    "abs_c21_g37_sa2_vic",
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
    sources = {source.source_id: source for source in catalog.sources}
    latest: dict[str, tuple[PublicFetchManifest, Path]] = {}
    for manifest_path in sorted(args.raw_root.rglob("*.manifest.json")):
        parsed = PublicFetchManifest.model_validate_json(manifest_path.read_text())
        source = sources.get(parsed.source_id)
        if source is None:
            raise ValueError(f"public fetch manifest references unknown source: {parsed.source_id}")
        manifest = verify_public_fetch_manifest(
            raw_root=args.raw_root,
            manifest_path=manifest_path,
            source=source,
        )
        candidate = (manifest, manifest_path)
        previous = latest.get(manifest.source_id)
        if previous is None or (manifest.retrieved_at, str(manifest_path)) > (
            previous[0].retrieved_at,
            str(previous[1]),
        ):
            latest[manifest.source_id] = candidate
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
        for manifest in (latest[source_id][0] for source_id in sorted(required))
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
