from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from propertyjourney360 import cli


class JsonResult:
    def model_dump_json(self, *, indent: int) -> str:
        assert indent == 2
        return '{"status":"ok"}'


def _invoke(monkeypatch: pytest.MonkeyPatch, arguments: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["pj360", *arguments])
    cli.main()


def test_run_local_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(**kwargs: Any) -> JsonResult:
        calls.append(kwargs)
        return JsonResult()

    monkeypatch.setattr(cli, "run_local_pipeline", fake_run)
    _invoke(monkeypatch, ["run-local", "--customers", "12", "--seed", "42"])
    assert calls[0]["customer_count"] == 12
    assert calls[0]["seed"] == 42
    assert '"status":"ok"' in capsys.readouterr().out


def test_validate_sources_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _invoke(monkeypatch, ["validate-sources", "--catalog", "config/sources.yml"])
    assert "source catalogue valid" in capsys.readouterr().out


def test_fetch_file_command(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SimpleNamespace(access_method="https_file")
    catalog = SimpleNamespace(require_enabled=lambda _name: source)
    calls: list[str] = []

    class FakeFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> FakeFetcher:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def fetch_file(self, source_name: str) -> JsonResult:
            calls.append(source_name)
            return JsonResult()

    monkeypatch.setattr(cli, "load_catalog", lambda _path: catalog)
    monkeypatch.setattr(cli, "PublicDataFetcher", FakeFetcher)
    _invoke(monkeypatch, ["fetch-public", "--source", "rba"])
    assert calls == ["rba"]


def test_arcgis_fetch_requires_explicit_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SimpleNamespace(access_method="arcgis_rest")
    catalog = SimpleNamespace(require_enabled=lambda _name: source)

    class FakeFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> FakeFetcher:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

    monkeypatch.setattr(cli, "load_catalog", lambda _path: catalog)
    monkeypatch.setattr(cli, "PublicDataFetcher", FakeFetcher)
    with pytest.raises(SystemExit, match="requires --where"):
        _invoke(monkeypatch, ["fetch-public", "--source", "vicmap"])


def test_serve_command(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, int]] = []
    monkeypatch.setattr(
        cli.uvicorn,
        "run",
        lambda app, host, port: calls.append((app, host, port)),
    )
    _invoke(monkeypatch, ["serve", "--host", "127.0.0.2", "--port", "9000"])
    assert calls == [("propertyjourney360.api.main:app", "127.0.0.2", 9000)]


def test_seed_postgres_requires_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "Settings", lambda: SimpleNamespace(postgres_dsn=None))
    with pytest.raises(SystemExit, match="PJ360_POSTGRES_DSN"):
        _invoke(monkeypatch, ["seed-postgres"])


def test_seed_postgres_uses_secret_without_printing_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = SecretStr("postgresql://secret-value")
    monkeypatch.setattr(cli, "Settings", lambda: SimpleNamespace(postgres_dsn=secret))
    calls: list[dict[str, Any]] = []

    def fake_seed(**kwargs: Any) -> JsonResult:
        calls.append(kwargs)
        return JsonResult()

    monkeypatch.setattr(cli, "seed_operational_postgres", fake_seed)
    _invoke(monkeypatch, ["seed-postgres", "--seed", "7", "--customers", "8"])
    assert calls == [{"dsn": "postgresql://secret-value", "seed": 7, "customer_count": 8}]
    assert "secret-value" not in capsys.readouterr().out


def test_default_output_path_is_relative_and_scoped() -> None:
    args = cli._parser().parse_args(["run-local"])
    assert args.output == Path("data/processed/local_run")


def test_operator_cli_rejects_unknown_or_cross_pipeline_modes() -> None:
    common = [
        "--window-start",
        "2026-08-23T00:00:00Z",
        "--window-end",
        "2026-08-24T00:00:00Z",
        "--window-id",
        "a" * 64,
    ]
    with pytest.raises(SystemExit):
        cli._parser().parse_args(
            ["ingest-batch", "extract", "--source", "legacy_crm", *common, "--mode", "LIVE"]
        )
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["ingest-events", "consume", *common, "--mode", "INCREMENTL"])


@pytest.mark.parametrize(
    ("arguments", "expected_method"),
    [
        (
            [
                "ingest-batch",
                "extract",
                "--source",
                "legacy_crm",
                "--window-start",
                "2026-08-23T00:00:00Z",
                "--window-end",
                "2026-08-24T00:00:00Z",
                "--window-id",
                "a" * 64,
                "--mode",
                "INCREMENTAL",
            ],
            "extract",
        ),
        (
            [
                "ingest-batch",
                "load-raw",
                "--source",
                "legacy_crm",
                "--window-start",
                "2026-08-23T00:00:00Z",
                "--window-end",
                "2026-08-24T00:00:00Z",
                "--window-id",
                "a" * 64,
                "--mode",
                "INCREMENTAL",
            ],
            "load_raw",
        ),
        (
            [
                "ingest-events",
                "consume",
                "--window-start",
                "2026-08-23T00:00:00Z",
                "--window-end",
                "2026-08-24T00:00:00Z",
                "--window-id",
                "a" * 64,
                "--mode",
                "LIVE",
                "--max-messages",
                "7",
            ],
            "consume",
        ),
        (
            [
                "ingest-events",
                "load-validated",
                "--window-start",
                "2026-08-23T00:00:00Z",
                "--window-end",
                "2026-08-24T00:00:00Z",
                "--window-id",
                "a" * 64,
                "--mode",
                "LIVE",
            ],
            "load_validated",
        ),
        (
            [
                "ingest-events",
                "commit-checkpoint",
                "--window-start",
                "2026-08-23T00:00:00Z",
                "--window-end",
                "2026-08-24T00:00:00Z",
                "--window-id",
                "a" * 64,
                "--mode",
                "LIVE",
            ],
            "commit_checkpoint",
        ),
    ],
)
def test_operator_commands_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
    expected_method: str,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeOperator:
        def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
            def invoke(**kwargs: Any) -> JsonResult:
                calls.append((name, kwargs))
                return JsonResult()

            return invoke

    operator = FakeOperator()
    monkeypatch.setattr(cli, "build_operator_from_environment", lambda: operator)
    monkeypatch.setattr(cli, "build_event_operator_from_environment", lambda: operator)
    _invoke(monkeypatch, arguments)
    assert calls[0][0] == expected_method
    if expected_method == "consume":
        assert calls[0][1]["max_messages"] == 7
    assert '"status":"ok"' in capsys.readouterr().out


def test_batch_commit_validates_and_dispatches_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    class FakeOperator:
        def commit_watermarks(self, *, sources: list[str], window: Any) -> JsonResult:
            calls.append(sources)
            assert window.window_id == "a" * 64
            return JsonResult()

    monkeypatch.setattr(cli, "build_operator_from_environment", FakeOperator)
    common = [
        "ingest-batch",
        "commit-watermarks",
        "--window-start",
        "2026-08-23T00:00:00Z",
        "--window-end",
        "2026-08-24T00:00:00Z",
        "--window-id",
        "a" * 64,
        "--mode",
        "INCREMENTAL",
    ]
    _invoke(monkeypatch, [*common, "--sources-json", '["legacy_crm","support"]'])
    assert calls == [["legacy_crm", "support"]]
    with pytest.raises(SystemExit, match="JSON array"):
        _invoke(monkeypatch, [*common, "--sources-json", '{"source":"legacy_crm"}'])


def test_resolved_public_fetch_and_remediation_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = SimpleNamespace(access_method="sdmx_rest")
    catalog = SimpleNamespace(require_enabled=lambda _name: source)
    fetch_calls: list[tuple[str, str, str | None]] = []

    class FakeFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> FakeFetcher:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def fetch_resolved(
            self, source_name: str, *, resolved_url: str, expected_sha256: str | None
        ) -> JsonResult:
            fetch_calls.append((source_name, resolved_url, expected_sha256))
            return JsonResult()

    monkeypatch.setattr(cli, "load_catalog", lambda _path: catalog)
    monkeypatch.setattr(cli, "PublicDataFetcher", FakeFetcher)
    _invoke(
        monkeypatch,
        [
            "fetch-public",
            "--source",
            "abs",
            "--resolved-url",
            "https://data.api.abs.gov.au/rest/data/ABS/example",
        ],
    )
    assert fetch_calls[0][0] == "abs"

    transition_calls: list[dict[str, Any]] = []

    class FakeStore:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["project_id"] == "pj360-project"

        def transition(self, **kwargs: Any) -> JsonResult:
            transition_calls.append(kwargs)
            return JsonResult()

    monkeypatch.setattr(cli, "BigQueryRemediationStore", FakeStore)
    monkeypatch.setenv("PJ360_PROJECT_ID", "pj360-project")
    monkeypatch.setenv("PJ360_GOVERNANCE_DATASET", "governance")
    monkeypatch.setenv("PJ360_LOCATION", "australia-southeast1")
    _invoke(
        monkeypatch,
        [
            "remediation",
            "transition",
            "--case-id",
            "b" * 64,
            "--to-status",
            "IN_REVIEW",
            "--actor",
            "operator",
            "--note",
            "accepted for review",
        ],
    )
    assert transition_calls[0]["to_status"] is cli.RemediationStatus.IN_REVIEW


def test_resolved_public_fetch_and_remediation_require_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = SimpleNamespace(access_method="metadata_resolved_file")
    catalog = SimpleNamespace(require_enabled=lambda _name: source)

    class FakeFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> FakeFetcher:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

    monkeypatch.setattr(cli, "load_catalog", lambda _path: catalog)
    monkeypatch.setattr(cli, "PublicDataFetcher", FakeFetcher)
    with pytest.raises(SystemExit, match="requires --resolved-url"):
        _invoke(monkeypatch, ["fetch-public", "--source", "vgv"])

    for name in (
        "PJ360_PROJECT_ID",
        "GCP_PROJECT_ID",
        "PJ360_GOVERNANCE_DATASET",
        "DBT_BIGQUERY_GOVERNANCE_DATASET",
        "PJ360_LOCATION",
        "BIGQUERY_LOCATION",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit, match="remediation requires"):
        _invoke(
            monkeypatch,
            [
                "remediation",
                "transition",
                "--case-id",
                "b" * 64,
                "--to-status",
                "IN_REVIEW",
                "--actor",
                "operator",
                "--note",
                "review",
            ],
        )
