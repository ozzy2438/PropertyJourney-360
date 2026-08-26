"""Five-minute event ingestion plus parameterised backfill and replay orchestration."""

from __future__ import annotations

import json
from typing import Any

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task, task_group
from pj360.shared import freeze_data_window, run_configured_command, run_dbt, task_failure_callback

DAG_ID = "propertyjourney_events"


def dbt_vars(frozen_window: dict[str, str], params: dict[str, Any]) -> str:
    source_window_id = (
        params["replay_id"] if params["run_mode"] == "REPLAY" else frozen_window["window_id"]
    )
    return json.dumps(
        {
            "orchestration_window_id": source_window_id,
            "orchestration_window_start": frozen_window["window_start"],
            "orchestration_window_end": frozen_window["window_end"],
            "pipeline_run_id": f"{DAG_ID}:{frozen_window['window_id']}",
        },
        separators=(",", ":"),
        sort_keys=True,
    )


@task
def freeze_event_window() -> dict[str, str]:
    return freeze_data_window(
        get_current_context(),
        dag_id=DAG_ID,
        default_interval_seconds=300,
    )


@task_group(group_id="consume_and_validate")
def consume_group(window: dict[str, str]) -> Any:
    @task
    def consume(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        arguments = [
            "consume",
            "--window-start",
            frozen_window["window_start"],
            "--window-end",
            frozen_window["window_end"],
            "--window-id",
            frozen_window["window_id"],
            "--mode",
            params["run_mode"],
            "--max-messages",
            str(params["max_messages"]),
        ]
        if params.get("replay_id"):
            arguments.extend(["--replay-id", params["replay_id"]])
        run_configured_command(
            "PJ360_EVENT_INGEST_COMMAND", arguments, default_command="pj360 ingest-events"
        )

    return consume(window)


@task_group(group_id="load_event_raw")
def load_group(window: dict[str, str]) -> Any:
    @task
    def load(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        arguments = [
            "load-validated",
            "--window-start",
            frozen_window["window_start"],
            "--window-end",
            frozen_window["window_end"],
            "--window-id",
            frozen_window["window_id"],
            "--mode",
            params["run_mode"],
        ]
        if params.get("replay_id"):
            arguments.extend(["--replay-id", params["replay_id"]])
        run_configured_command(
            "PJ360_EVENT_INGEST_COMMAND", arguments, default_command="pj360 ingest-events"
        )

    return load(window)


@task_group(group_id="event_transform")
def transform_group(window: dict[str, str]) -> Any:
    @task
    def build_pre_publish_models(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        run_dbt(
            [
                "run",
                "--target",
                params["dbt_target"],
                "--select",
                "stg_property_events",
                "int_event_identity_attribution",
                "reconciliation_result",
                "pipeline_run_audit",
                "dq_issue",
                "dq_quarantine",
                "remediation_case",
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return build_pre_publish_models(window)


@task_group(group_id="event_reconciliation_gate")
def gate_group(window: dict[str, str]) -> Any:
    @task
    def assert_event_balance(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        run_dbt(
            [
                "test",
                "--target",
                params["dbt_target"],
                "--select",
                "reconciliation_result",
                "reconciliation_balances",
                "valid_events_have_unambiguous_subject",
                "valid_immutable_event_keys_have_one_payload",
                "immutable_event_first_write_wins",
                "collision_issues_have_remediation_cases",
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return assert_event_balance(window)


@task_group(group_id="publish_event_curated")
def publish_group(window: dict[str, str]) -> Any:
    @task
    def publish_features(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        run_dbt(
            [
                "build",
                "--target",
                params["dbt_target"],
                "--select",
                "fact_customer_property_interaction",
                "mart_customer_engagement",
                "customer_360_current",
                "mart_personalisation_features",
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return publish_features(window)


@task
def commit_event_checkpoint(window: dict[str, str]) -> None:
    params = get_current_context()["params"]
    arguments = [
        "commit-checkpoint",
        "--window-start",
        window["window_start"],
        "--window-end",
        window["window_end"],
        "--window-id",
        window["window_id"],
        "--mode",
        params["run_mode"],
    ]
    if params.get("replay_id"):
        arguments.extend(["--replay-id", params["replay_id"]])
    run_configured_command(
        "PJ360_EVENT_INGEST_COMMAND", arguments, default_command="pj360 ingest-events"
    )


with DAG(
    dag_id=DAG_ID,
    description="Validated event ingestion, replay, backfill and feature refresh",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Australia/Melbourne"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),
        "on_failure_callback": task_failure_callback,
    },
    params={
        "window_start": Param(
            "", type="string", description="Optional exclusive ISO-8601 start; pair with window_end"
        ),
        "window_end": Param(
            "", type="string", description="Optional inclusive ISO-8601 end; pair with window_start"
        ),
        "run_mode": Param("LIVE", enum=["LIVE", "BACKFILL", "REPLAY"]),
        "replay_id": Param(
            "", type="string", description="Stable replay identifier for idempotent replay"
        ),
        "max_messages": Param(5000, type="integer", minimum=1, maximum=50000),
        "dbt_target": Param("dev", type="string", pattern="^[A-Za-z0-9_-]+$"),
    },
    render_template_as_native_obj=True,
    tags=["propertyjourney", "events", "replay"],
) as dag:
    frozen = freeze_event_window()
    consumed = consume_group(frozen)
    loaded = load_group(frozen)
    transformed = transform_group(frozen)
    reconciled = gate_group(frozen)
    published = publish_group(frozen)
    committed = commit_event_checkpoint(frozen)

    consumed >> loaded >> transformed >> reconciled >> published >> committed
