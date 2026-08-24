#!/usr/bin/env python3
"""Collect local Docker/dbt/Terraform evidence without promoting it to cloud proof."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
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
OUTPUT = ROOT / "evidence" / "platform_validation.json"
TERRAFORM_IMAGE = "hashicorp/terraform:1.15.9"
PYTHON_IMAGE = (
    "python:3.12.14-slim-bookworm@"
    "sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134"
)


def run_check(name: str, command: tuple[str, ...]) -> CheckResult:
    started = time.perf_counter()
    process = subprocess.run(  # noqa: S603 - fixed local validation commands only
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    duration = time.perf_counter() - started
    output = f"{process.stdout}\n{process.stderr}".strip()
    return CheckResult(
        name=name,
        command=command,
        exit_code=process.returncode,
        duration_seconds=round(duration, 6),
        output_sha256=hashlib.sha256(output.encode()).hexdigest(),
        output_tail="\n".join(output.splitlines()[-30:]),
    )


def run_locked_dependency_audit() -> CheckResult:
    export_command = (
        "docker",
        "run",
        "--rm",
        "-v",
        f"{ROOT}:/workspace:ro",
        "-w",
        "/workspace",
        PYTHON_IMAGE,
        "sh",
        "-c",
        (
            "python -m pip install --disable-pip-version-check --root-user-action=ignore "
            "--no-deps uv==0.12.5 "
            ">/dev/null && uv export --quiet --locked --all-extras --no-emit-project "
            "--format requirements.txt"
        ),
    )
    audit_command = (
        str(Path(sys.executable).with_name("pip-audit")),
        "--strict",
        "--require-hashes",
        "--requirement",
        "/dev/stdin",
    )
    started = time.perf_counter()
    exported = subprocess.run(  # noqa: S603 - fixed, pinned local validation command
        export_command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    audited = None
    if exported.returncode == 0:
        audited = subprocess.run(  # noqa: S603 - fixed local audit executable and arguments
            audit_command,
            cwd=ROOT,
            input=exported.stdout,
            capture_output=True,
            text=True,
            check=False,
        )
    output = exported.stderr.strip()
    exit_code = exported.returncode
    if audited is not None:
        output = f"{output}\n{audited.stdout}\n{audited.stderr}".strip()
        exit_code = audited.returncode
    return CheckResult(
        name="locked_dependency_audit",
        command=(*export_command, "|", *audit_command),
        exit_code=exit_code,
        duration_seconds=round(time.perf_counter() - started, 6),
        output_sha256=hashlib.sha256(output.encode()).hexdigest(),
        output_tail="\n".join(output.splitlines()[-30:]),
    )


def main() -> int:
    terraform_mount = f"{ROOT / 'infra' / 'terraform'}:/workspace"
    dbt_command = (
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        "DBT_BIGQUERY_PROJECT=propertyjourney360-ci",
        "-e",
        "DBT_BIGQUERY_DATASET=propertyjourney_ci",
        "-e",
        "DBT_BIGQUERY_LOCATION=australia-southeast1",
        "airflow-scheduler",
        "/opt/dbt-venv/bin/dbt",
        "--log-path",
        "/opt/airflow/logs/pj360-dbt-evidence-logs",
        "parse",
        "--project-dir",
        "/opt/airflow/dbt",
        "--profiles-dir",
        "/opt/airflow/dbt",
        "--target-path",
        "/opt/airflow/logs/pj360-dbt-evidence-target",
        "--target",
        "dev",
        "--no-partial-parse",
    )
    postgres_query = (
        "SELECT json_build_object("
        "'legacy_customers',(SELECT count(*) FROM legacy.customer),"
        "'support_customers',(SELECT count(*) FROM support.customer),"
        "'support_cases',(SELECT count(*) FROM support.case_record),"
        "'consent_events',(SELECT count(*) FROM marketing.consent_history),"
        "'orphan_support_cases',(SELECT count(*) FROM support.case_record AS cases "
        "LEFT JOIN support.customer AS customers "
        "ON cases.customer_source_record_id = customers.source_record_id "
        "WHERE customers.source_record_id IS NULL)"
        ");"
    )
    commands = (
        ("compose_config", ("docker", "compose", "config", "--quiet")),
        (
            "compose_services",
            ("docker", "compose", "ps", "--services", "--status", "running"),
        ),
        (
            "api_health",
            ("curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:18081/health"),
        ),
        (
            "airflow_health",
            (
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "http://127.0.0.1:18082/api/v2/monitor/health",
            ),
        ),
        (
            "airflow_dags",
            (
                "docker",
                "compose",
                "exec",
                "-T",
                "airflow-scheduler",
                "airflow",
                "dags",
                "list",
                "--output",
                "json",
            ),
        ),
        ("dbt_parse", dbt_command),
        (
            "terraform_validate",
            (
                "docker",
                "run",
                "--rm",
                "-v",
                terraform_mount,
                "-w",
                "/workspace",
                TERRAFORM_IMAGE,
                "validate",
            ),
        ),
        (
            "postgres_seed_counts",
            (
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "pj360",
                "-d",
                "propertyjourney360",
                "-Atqc",
                postgres_query,
            ),
        ),
        (
            "api_image",
            (
                "docker",
                "image",
                "inspect",
                "propertyjourney360-api:local",
                "--format",
                "{{.Id}}",
            ),
        ),
        (
            "gitleaks_worktree",
            ("gitleaks", "dir", "--redact", "--no-banner", "--config", ".gitleaks.toml", "."),
        ),
    )
    checks = (
        *tuple(run_check(name, command) for name, command in commands),
        run_locked_dependency_audit(),
    )
    checks_by_name = {check.name: check for check in checks}
    counts = json.loads(checks_by_name["postgres_seed_counts"].output_tail or "{}")
    services = sorted(checks_by_name["compose_services"].output_tail.splitlines())
    all_passed = all(check.passed for check in checks)
    claims = (
        EvidenceClaim(
            claim_id="local_platform_gates",
            description="Compose, API, Airflow, dbt parse, Terraform and secret scan gates.",
            level=EvidenceLevel.LOCALLY_VERIFIED if all_passed else EvidenceLevel.IMPLEMENTED,
            target="all exit codes 0",
            actual="all exit codes 0" if all_passed else "one or more checks failed",
            evidence_status="PASSED" if all_passed else "FAILED",
            limitation="Local services and static validation are not GCP runtime proof.",
        ),
        EvidenceClaim(
            claim_id="synthetic_postgres_seed",
            description="Deterministic operational-source row counts observed in local PostgreSQL.",
            level=EvidenceLevel.LOCALLY_VERIFIED,
            target="repeatable non-zero source records with intentional defects",
            actual=json.dumps(counts, sort_keys=True),
            evidence_status="MEASURED",
            limitation="Synthetic records only; no private real-customer data.",
        ),
    )
    report = LocalEvidenceReport(
        generated_at=datetime.now(UTC),
        environment="local-docker",
        python_version="containerised Python 3.12 / host evidence runner",
        source_tree_sha256=hash_source_tree(ROOT),
        checks=checks,
        claims=claims,
        measurements={
            "running_services": services,
            "postgres_row_counts": counts,
            "api_image_id": checks_by_name["api_image"].output_tail,
        },
    )
    OUTPUT.write_text(report.model_dump_json(indent=2))
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
