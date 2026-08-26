#!/usr/bin/env python3
"""Execute the deterministic local vertical slice and persist only its evidence report."""

from __future__ import annotations

import tempfile
from pathlib import Path

from propertyjourney360.pipeline import run_local_pipeline

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "local_pipeline_validation.json"


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="pj360-pipeline-evidence-") as temporary:
        workspace = Path(temporary)
        report = run_local_pipeline(
            output_root=workspace / "run",
            database_path=workspace / "propertyjourney360.db",
            seed=20260824,
            customer_count=100,
        )
    OUTPUT.write_text(report.model_dump_json(indent=2))
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
