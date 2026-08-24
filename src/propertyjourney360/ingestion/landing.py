"""Immutable, content-addressed local landing adapter mirroring GCS raw semantics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LandingManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    run_id: str
    schema_version: str
    extracted_at: datetime
    landed_at: datetime
    watermark: str | None
    record_count: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    object_path: str


def validate_manifest_replay(
    previous: LandingManifest,
    expected: LandingManifest,
    *,
    location: str,
) -> None:
    """Reject an existing manifest unless every immutable replay field matches."""
    immutable_fields = (
        "source",
        "run_id",
        "schema_version",
        "extracted_at",
        "watermark",
        "record_count",
        "sha256",
        "object_path",
    )
    mismatches = [
        field for field in immutable_fields if getattr(previous, field) != getattr(expected, field)
    ]
    if mismatches:
        raise RuntimeError(
            f"existing {location} manifest conflicts with replay envelope: " + ", ".join(mismatches)
        )


def _canonical_json_line(record: Mapping[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode() + b"\n"


class LocalRawLanding:
    """Write immutable JSONL and a manifest; identical reruns reuse the same object."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def land(
        self,
        *,
        source: str,
        run_id: str,
        schema_version: str,
        records: Iterable[Mapping[str, Any]],
        extracted_at: datetime,
        watermark: str | None = None,
    ) -> LandingManifest:
        lines = tuple(_canonical_json_line(record) for record in records)
        payload = b"".join(lines)
        digest = hashlib.sha256(payload).hexdigest()
        partition = extracted_at.astimezone(UTC).strftime("extracted_date=%Y-%m-%d")
        target_dir = self.root / source / partition / f"run_id={run_id}"
        target_dir.mkdir(parents=True, exist_ok=True)
        data_path = target_dir / f"{digest}.jsonl"
        manifest_path = target_dir / f"{digest}.manifest.json"

        if data_path.exists():
            if hashlib.sha256(data_path.read_bytes()).hexdigest() != digest:
                raise RuntimeError(f"immutable landing object was modified: {data_path}")
        else:
            data_path.write_bytes(payload)

        manifest = LandingManifest(
            source=source,
            run_id=run_id,
            schema_version=schema_version,
            extracted_at=extracted_at,
            landed_at=datetime.now(UTC),
            watermark=watermark,
            record_count=len(lines),
            sha256=digest,
            object_path=str(data_path.relative_to(self.root)),
        )
        if manifest_path.exists():
            previous = LandingManifest.model_validate_json(manifest_path.read_text())
            validate_manifest_replay(previous, manifest, location="local landing")
            return previous
        manifest_path.write_text(manifest.model_dump_json(indent=2))
        return manifest
