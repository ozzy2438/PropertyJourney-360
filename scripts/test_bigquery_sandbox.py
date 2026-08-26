"""Free-tier-only smoke test for a BigQuery Sandbox project (no billing account required).

BigQuery Sandbox grants a free query/storage quota directly on a GCP project without a billing
account attached. This script only ever runs a zero-byte literal `SELECT`, guarded by
`maximum_bytes_billed=0` so it can never accidentally scan or bill for a real table, plus optional
dry-run (no execution, no cost, no data read) permission checks against operator-named datasets.
It creates nothing, enables no billing, and never touches Terraform-managed infrastructure.

Prerequisites (run locally, never in this repository's CI):
    gcloud auth application-default login

Usage:
    uv run python scripts/test_bigquery_sandbox.py --project propertyjourney-360
    uv run python scripts/test_bigquery_sandbox.py --project propertyjourney-360 \\
        --check-dataset propertyjourney_dev
"""

from __future__ import annotations

import argparse
import json

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="GCP project id, e.g. propertyjourney-360")
    parser.add_argument("--location", default="australia-southeast1")
    parser.add_argument(
        "--check-dataset",
        action="append",
        default=[],
        dest="check_datasets",
        help="Dataset id to dry-run a permission check against; repeatable. Dry runs cost nothing "
        "and read no data.",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    client = bigquery.Client(project=args.project, location=args.location)
    result: dict[str, object] = {"project": args.project, "location": args.location}

    connectivity_job = client.query(
        "SELECT 1 AS sandbox_check",
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=0),
    )
    rows = list(connectivity_job.result())
    result["connectivity"] = "succeeded"
    result["sandbox_check_value"] = rows[0]["sandbox_check"]
    result["job_id"] = connectivity_job.job_id
    result["total_bytes_processed"] = connectivity_job.total_bytes_processed

    dataset_checks: dict[str, object] = {}
    for dataset_id in args.check_datasets:
        query = f"SELECT * FROM `{args.project}.{dataset_id}`.INFORMATION_SCHEMA.TABLES LIMIT 0"  # noqa: S608
        try:
            dry_run_job = client.query(
                query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            )
            dataset_checks[dataset_id] = {
                "dry_run": "succeeded",
                "estimated_bytes_processed": dry_run_job.total_bytes_processed,
            }
        except GoogleAPICallError as exc:
            dataset_checks[dataset_id] = {"dry_run": "failed", "error": str(exc)}
    if dataset_checks:
        result["dataset_permission_checks"] = dataset_checks

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
