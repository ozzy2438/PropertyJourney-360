"""Credential-free structural checks for the Airflow 3 DAG sources."""

from __future__ import annotations

import ast
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
BATCH_DAG = DAGS_DIR / "propertyjourney_customer_360.py"
EVENT_DAG = DAGS_DIR / "propertyjourney_events.py"
SHARED = DAGS_DIR / "pj360" / "shared.py"


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_all_dag_python_is_syntactically_valid() -> None:
    for path in sorted(DAGS_DIR.rglob("*.py")):
        ast.parse(_source(path), filename=str(path))


def test_dags_have_bounded_concurrency_and_no_implicit_catchup() -> None:
    for path in (BATCH_DAG, EVENT_DAG):
        source = _source(path)
        assert "catchup=False" in source
        assert "max_active_runs=1" in source
        assert "on_failure_callback" in source


def test_batch_dag_has_explicit_backfill_and_reconciliation_controls() -> None:
    source = _source(BATCH_DAG)
    for token in (
        'DAG_ID = "propertyjourney_customer_360"',
        '"BACKFILL"',
        '"REPLAY"',
        '"window_start"',
        '"window_end"',
        '"replay_id"',
        'group_id="reconciliation_gate"',
        "commit_watermarks",
    ):
        assert token in source


def test_event_dag_has_replay_limits_and_checkpoint_after_gate() -> None:
    source = _source(EVENT_DAG)
    for token in (
        'DAG_ID = "propertyjourney_events"',
        '"max_messages"',
        '"BACKFILL"',
        '"REPLAY"',
        'group_id="event_reconciliation_gate"',
        "commit_event_checkpoint",
        "consumed >> loaded >> transformed >> reconciled >> committed",
    ):
        assert token in source


def test_window_is_frozen_once_and_commands_never_use_a_shell() -> None:
    source = _source(SHARED)
    assert "window_id" in source
    assert "data_interval_start" in source
    assert "logical_date" in source
    assert "shell=False" in source
    assert "shell=True" not in source


def test_business_rules_are_not_embedded_in_dags() -> None:
    combined = _source(BATCH_DAG) + _source(EVENT_DAG)
    forbidden = (
        "normalize_email",
        "normalize_phone",
        "AUTO_MATCH",
        "match_score",
        "select * from",
        "insert into",
        "merge into",
    )
    lowered = combined.lower()
    for token in forbidden:
        assert token.lower() not in lowered
