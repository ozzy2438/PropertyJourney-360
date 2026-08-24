"""Command-line entry points for local validation and operator workflows."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import NoReturn

import uvicorn

from propertyjourney360.config import Settings
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

    serve = commands.add_parser("serve", help="run the internal API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    seed_postgres = commands.add_parser(
        "seed-postgres", help="seed local synthetic operational-source tables"
    )
    seed_postgres.add_argument("--seed", type=int, default=20260824)
    seed_postgres.add_argument("--customers", type=int, default=100)
    return parser


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
    else:
        _fail(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
