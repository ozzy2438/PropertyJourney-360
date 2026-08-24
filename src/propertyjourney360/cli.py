"""Command-line entry points for local validation and operator workflows."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import NoReturn

import uvicorn

from propertyjourney360.config import Settings
from propertyjourney360.domain.models import RemediationStatus
from propertyjourney360.governance.remediation_store import BigQueryRemediationStore
from propertyjourney360.ingestion.operator import (
    build_event_operator_from_environment,
    build_operator_from_environment,
    frozen_window_from_values,
)
from propertyjourney360.pipeline import run_local_pipeline
from propertyjourney360.public_data.catalog import load_catalog
from propertyjourney360.public_data.fetch import PublicDataFetcher
from propertyjourney360.synthetic.postgres import seed_operational_postgres


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pj360")
    commands = parser.add_subparsers(dest="command", required=True)

    local = commands.add_parser("run-local", help="execute the deterministic local vertical slice")
    local.add_argument("--output", type=Path, default=Path("data/processed/local_run"))
    local.add_argument(
        "--database",
        type=Path,
        default=Path("data/processed/propertyjourney360.db"),
    )
    local.add_argument("--seed", type=int, default=20260824)
    local.add_argument("--customers", type=int, default=100)

    sources = commands.add_parser("validate-sources", help="validate public-source policy")
    sources.add_argument("--catalog", type=Path, default=Path("config/sources.yml"))

    fetch = commands.add_parser(
        "fetch-public", help="fetch one enabled public source into raw storage"
    )
    fetch.add_argument("--source", required=True)
    fetch.add_argument("--catalog", type=Path, default=Path("config/sources.yml"))
    fetch.add_argument("--output", type=Path, default=Path("data/raw/public"))
    fetch.add_argument("--where", help="required explicit filter for ArcGIS sources")
    fetch.add_argument("--out-field", action="append", default=[])
    fetch.add_argument("--record-limit", type=int, default=100)
    fetch.add_argument("--result-offset", type=int, default=0)
    fetch.add_argument("--include-geometry", action="store_true")
    fetch.add_argument("--resolved-url", help="exact official file or bounded SDMX query URL")
    fetch.add_argument("--expected-sha256", help="required for a cross-domain resolved URL")

    serve = commands.add_parser("serve", help="run the internal API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    seed_postgres = commands.add_parser(
        "seed-postgres", help="seed local synthetic operational-source tables"
    )
    seed_postgres.add_argument("--seed", type=int, default=20260824)
    seed_postgres.add_argument("--customers", type=int, default=100)

    batch = commands.add_parser("ingest-batch", help="execute one Airflow batch operator phase")
    batch_actions = batch.add_subparsers(dest="batch_action", required=True)
    for action in ("extract", "load-raw"):
        action_parser = batch_actions.add_parser(action)
        action_parser.add_argument("--source", required=True)
        _add_window_arguments(action_parser, modes=("INCREMENTAL", "BACKFILL", "REPLAY"))
    commit = batch_actions.add_parser("commit-watermarks")
    commit.add_argument("--sources-json", required=True)
    _add_window_arguments(commit, modes=("INCREMENTAL", "BACKFILL", "REPLAY"))

    events = commands.add_parser("ingest-events", help="execute one Airflow event operator phase")
    event_actions = events.add_subparsers(dest="event_action", required=True)
    consume = event_actions.add_parser("consume")
    _add_window_arguments(consume, modes=("LIVE", "BACKFILL", "REPLAY"))
    consume.add_argument("--max-messages", type=int, default=5000)
    for action in ("load-validated", "commit-checkpoint"):
        action_parser = event_actions.add_parser(action)
        _add_window_arguments(action_parser, modes=("LIVE", "BACKFILL", "REPLAY"))

    remediation = commands.add_parser(
        "remediation", help="append one controlled cloud remediation transition"
    )
    remediation_actions = remediation.add_subparsers(dest="remediation_action", required=True)
    transition = remediation_actions.add_parser("transition")
    transition.add_argument("--case-id", required=True)
    transition.add_argument(
        "--to-status",
        required=True,
        choices=[
            status.value for status in RemediationStatus if status is not RemediationStatus.OPEN
        ],
    )
    transition.add_argument("--actor", required=True)
    transition.add_argument("--note", required=True)
    transition.add_argument("--resolution-action")
    transition.add_argument("--replay-id")
    transition.add_argument("--verification-result-id")
    return parser


def _add_window_arguments(parser: argparse.ArgumentParser, *, modes: tuple[str, ...]) -> None:
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--window-id", required=True)
    parser.add_argument("--mode", required=True, type=str.upper, choices=modes)
    parser.add_argument("--replay-id")


def _window_from_args(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    return frozen_window_from_values(
        window_start=args.window_start,
        window_end=args.window_end,
        window_id=args.window_id,
        mode=args.mode,
        replay_id=args.replay_id,
    )


def _fail(message: str) -> NoReturn:
    raise SystemExit(message)


def main() -> None:
    args = _parser().parse_args()
    if args.command == "run-local":
        local_report = run_local_pipeline(
            output_root=args.output,
            database_path=args.database,
            seed=args.seed,
            customer_count=args.customers,
        )
        print(local_report.model_dump_json(indent=2))
    elif args.command == "validate-sources":
        catalog = load_catalog(args.catalog)
        enabled = sum(source.enabled for source in catalog.sources)
        print(f"source catalogue valid: {enabled}/{len(catalog.sources)} enabled")
    elif args.command == "fetch-public":
        catalog = load_catalog(args.catalog)
        source = catalog.require_enabled(args.source)
        with PublicDataFetcher(catalog=catalog, root=args.output) as fetcher:
            if source.access_method == "arcgis_rest":
                if not args.where or not args.out_field:
                    _fail("ArcGIS fetch requires --where and at least one --out-field")
                manifest = fetcher.fetch_arcgis_page(
                    args.source,
                    where=args.where,
                    out_fields=args.out_field,
                    record_limit=args.record_limit,
                    result_offset=args.result_offset,
                    include_geometry=args.include_geometry,
                )
            elif source.access_method in {"metadata_resolved_file", "sdmx_rest"}:
                if not args.resolved_url:
                    _fail(f"{source.access_method} fetch requires --resolved-url")
                manifest = fetcher.fetch_resolved(
                    args.source,
                    resolved_url=args.resolved_url,
                    expected_sha256=args.expected_sha256,
                )
            else:
                manifest = fetcher.fetch_file(args.source)
        print(manifest.model_dump_json(indent=2))
    elif args.command == "serve":
        uvicorn.run("propertyjourney360.api.main:app", host=args.host, port=args.port)
    elif args.command == "seed-postgres":
        dsn = Settings().postgres_dsn
        if dsn is None:
            _fail("PJ360_POSTGRES_DSN must be set outside the command line")
        seed_report = seed_operational_postgres(
            dsn=dsn.get_secret_value(),
            seed=args.seed,
            customer_count=args.customers,
        )
        print(seed_report.model_dump_json(indent=2))
    elif args.command == "ingest-batch":
        batch_operator = build_operator_from_environment()
        window = _window_from_args(args)
        if args.batch_action == "extract":
            batch_report = batch_operator.extract(source=args.source, window=window)
            print(batch_report.model_dump_json(indent=2))
        elif args.batch_action == "load-raw":
            load_report = batch_operator.load_raw(source=args.source, window=window)
            print(load_report.model_dump_json(indent=2))
        elif args.batch_action == "commit-watermarks":
            sources = json.loads(args.sources_json)
            if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
                _fail("--sources-json must be a JSON array of source names")
            commit_report = batch_operator.commit_watermarks(sources=sources, window=window)
            print(commit_report.model_dump_json(indent=2))
        else:
            _fail(f"unsupported batch action: {args.batch_action}")
    elif args.command == "ingest-events":
        event_operator = build_event_operator_from_environment()
        window = _window_from_args(args)
        if args.event_action == "consume":
            consume_report = event_operator.consume(window=window, max_messages=args.max_messages)
            print(consume_report.model_dump_json(indent=2))
        elif args.event_action == "load-validated":
            event_load_report = event_operator.load_validated(window=window)
            print(event_load_report.model_dump_json(indent=2))
        elif args.event_action == "commit-checkpoint":
            checkpoint_report = event_operator.commit_checkpoint(window=window)
            print(checkpoint_report.model_dump_json(indent=2))
        else:
            _fail(f"unsupported event action: {args.event_action}")
    elif args.command == "remediation":
        project_id = os.getenv("PJ360_PROJECT_ID", os.getenv("GCP_PROJECT_ID", "")).strip()
        governance_dataset = os.getenv(
            "PJ360_GOVERNANCE_DATASET", os.getenv("DBT_BIGQUERY_GOVERNANCE_DATASET", "")
        ).strip()
        location = os.getenv("PJ360_LOCATION", os.getenv("BIGQUERY_LOCATION", "")).strip()
        if not project_id or not governance_dataset or not location:
            _fail("remediation requires project, governance dataset and BigQuery location settings")
        store = BigQueryRemediationStore(
            project_id=project_id,
            governance_dataset=governance_dataset,
            location=location,
        )
        transition_report = store.transition(
            remediation_case_id=args.case_id,
            to_status=RemediationStatus(args.to_status),
            actor=args.actor,
            note=args.note,
            resolution_action=args.resolution_action,
            replay_id=args.replay_id,
            verification_result_id=args.verification_result_id,
        )
        print(transition_report.model_dump_json(indent=2))
    else:
        _fail(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
