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
