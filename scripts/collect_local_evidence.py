#!/usr/bin/env python3
"""Execute local quality gates and write a hash-bound, non-cloud evidence report."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from propertyjourney360.evidence import (
    CheckResult,
    EvidenceClaim,
    EvidenceLevel,
    LocalEvidenceReport,
    hash_source_tree,
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "local_validation.json"


def run_check(name: str, command: tuple[str, ...]) -> CheckResult:
    started = time.perf_counter()
    process = subprocess.run(  # noqa: S603 - every command is a fixed local quality gate
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    duration = time.perf_counter() - started
    output = f"{process.stdout}\n{process.stderr}".strip()
    output_tail = "\n".join(output.splitlines()[-30:])
    return CheckResult(
        name=name,
        command=command,
        exit_code=process.returncode,
        duration_seconds=round(duration, 6),
        output_sha256=hashlib.sha256(output.encode()).hexdigest(),
        output_tail=output_tail,
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="pj360-evidence-") as temporary:
        coverage_json = Path(temporary) / "coverage.json"
        commands = (
            ("ruff", (sys.executable, "-m", "ruff", "check", ".")),
            ("ruff_format", (sys.executable, "-m", "ruff", "format", "--check", ".")),
            ("mypy", (sys.executable, "-m", "mypy", "src")),
            (
                "pytest",
                (
                    sys.executable,
                    "-m",
                    "pytest",
                    "--cov=propertyjourney360",
                    f"--cov-report=json:{coverage_json}",
                    "-q",
                ),
            ),
        )
        checks = tuple(run_check(name, command) for name, command in commands)
        coverage = None
        if coverage_json.exists():
            coverage_payload = json.loads(coverage_json.read_text())
            coverage = coverage_payload["totals"]["percent_covered"]

    all_passed = all(check.passed for check in checks)
    claims = (
        EvidenceClaim(
            claim_id="local_quality_gates",
            description="Lint, strict type checks and Python tests execute successfully.",
            level=EvidenceLevel.LOCALLY_VERIFIED if all_passed else EvidenceLevel.IMPLEMENTED,
            target="all exit codes 0",
            actual="all exit codes 0" if all_passed else "one or more checks failed",
            evidence_status="PASSED" if all_passed else "FAILED",
            limitation="Local checks do not prove Salesforce or GCP runtime behaviour.",
        ),
        EvidenceClaim(
            claim_id="python_branch_coverage",
            description="Python branch coverage for the local test suite.",
            level=(
                EvidenceLevel.LOCALLY_VERIFIED
                if coverage is not None
                else EvidenceLevel.IMPLEMENTED
            ),
            target=85,
            actual=round(float(coverage), 2) if coverage is not None else None,
            evidence_status="MEASURED" if coverage is not None else "NOT_OBSERVED",
            limitation="Coverage percentage is not a production reliability metric.",
        ),
        EvidenceClaim(
            claim_id="cloud_runtime",
            description="Salesforce and GCP pipeline execution.",
            level=EvidenceLevel.IMPLEMENTED,
            target="CLOUD_VERIFIED",
            actual=None,
            evidence_status="NOT_OBSERVED",
            limitation="Requires owner-configured Salesforce and GCP environments.",
        ),
    )
    report = LocalEvidenceReport(
        generated_at=datetime.now(UTC),
        environment="local",
        python_version=sys.version.split()[0],
        source_tree_sha256=hash_source_tree(ROOT),
        checks=checks,
        claims=claims,
        measurements={"python_branch_coverage_percent": coverage},
    )
    OUTPUT.write_text(report.model_dump_json(indent=2))
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
