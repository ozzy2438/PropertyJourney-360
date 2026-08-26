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
    assert "At least one batch source must be selected" in source
    for token in (
        'DAG_ID = "propertyjourney_customer_360"',
        '"BACKFILL"',
        '"REPLAY"',
        '"window_start"',
        '"window_end"',
        '"replay_id"',
        'group_id="reconciliation_gate"',
        'group_id="publish_curated"',
        '"pipeline_run_id"',
        "commit_watermarks",
        "transformed >> reconciled >> published >> committed",
    ):
        assert token in source
    assert '"orchestration_window_id": source_window_id' in source
    assert '"pipeline_run_id": f"{DAG_ID}:{frozen_window[\'window_id\']}"' in source


def test_event_dag_has_replay_limits_and_checkpoint_after_gate() -> None:
    source = _source(EVENT_DAG)
    for token in (
        'DAG_ID = "propertyjourney_events"',
        '"max_messages"',
        '"BACKFILL"',
        '"REPLAY"',
        'group_id="event_reconciliation_gate"',
        'group_id="publish_event_curated"',
        '"pipeline_run_id"',
        "commit_event_checkpoint",
        "transformed >> reconciled >> published >> committed",
    ):
        assert token in source
    assert '"orchestration_window_id": source_window_id' in source
    assert '"pipeline_run_id": f"{DAG_ID}:{frozen_window[\'window_id\']}"' in source
    transform = source.split('@task_group(group_id="event_transform")', maxsplit=1)[1].split(
        '@task_group(group_id="event_reconciliation_gate")', maxsplit=1
    )[0]
    for governance_model in ('"dq_issue"', '"dq_quarantine"', '"remediation_case"'):
        assert governance_model in transform


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


def test_curated_models_are_not_selected_before_reconciliation_gate() -> None:
    for path, publish_group in (
        (BATCH_DAG, '@task_group(group_id="publish_curated")'),
        (EVENT_DAG, '@task_group(group_id="publish_event_curated")'),
    ):
        source = _source(path)
        pre_publish, publish = source.split(publish_group, maxsplit=1)
        assert "path:models/core" not in pre_publish
        assert "path:models/marts" not in pre_publish
        assert "fact_customer_property_interaction" not in pre_publish
        assert '"build"' in publish


def test_runtime_gates_execute_critical_singular_tests_before_publish() -> None:
    batch = _source(BATCH_DAG).split('@task_group(group_id="publish_curated")', maxsplit=1)[0]
    event = _source(EVENT_DAG).split('@task_group(group_id="publish_event_curated")', maxsplit=1)[0]

    for test_name in (
        "valid_staging_business_keys_not_null",
        "staging_business_keys_unique_per_window",
        "valid_staging_rows_have_single_payload_variant",
        "divergent_staging_payloads_are_fully_quarantined",
        "orphan_cross_entity_references_are_quarantined",
        "orphan_support_cases_are_quarantined",
        "valid_events_have_unambiguous_subject",
        "valid_immutable_event_keys_have_one_payload",
        "immutable_event_first_write_wins",
        "collision_issues_have_remediation_cases",
    ):
        assert f'"{test_name}"' in batch
    for test_name in (
        "valid_events_have_unambiguous_subject",
        "valid_immutable_event_keys_have_one_payload",
        "immutable_event_first_write_wins",
        "collision_issues_have_remediation_cases",
    ):
        assert f'"{test_name}"' in event
