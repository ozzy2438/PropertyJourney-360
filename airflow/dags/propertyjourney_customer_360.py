"""Daily and parameterised-backfill orchestration for the trusted Customer 360."""

from __future__ import annotations

import json
from typing import Any

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task, task_group
from pj360.shared import freeze_data_window, run_configured_command, run_dbt, task_failure_callback

DAG_ID = "propertyjourney_customer_360"
ALLOWED_SOURCES = ["salesforce", "legacy_crm", "support", "consent", "property", "economic"]


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
def freeze_window() -> dict[str, str]:
    return freeze_data_window(
        get_current_context(),
        dag_id=DAG_ID,
        default_interval_seconds=86_400,
    )


@task
def selected_sources() -> list[str]:
    params = get_current_context()["params"]
    sources = list(dict.fromkeys(params["sources"]))
    if not sources:
        raise ValueError("At least one batch source must be selected")
    unknown = sorted(set(sources) - set(ALLOWED_SOURCES))
    if unknown:
        raise ValueError(f"Unsupported source selections: {unknown}")
    return sources


@task_group(group_id="extract")
def extract_group(window: dict[str, str], sources: list[str]) -> Any:
    @task
    def extract_one(source: str, frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        arguments = [
            "extract",
            "--source",
            source,
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
            "PJ360_BATCH_INGEST_COMMAND", arguments, default_command="pj360 ingest-batch"
        )

    return extract_one.partial(frozen_window=window).expand(source=sources)


@task_group(group_id="load_raw")
def load_raw_group(window: dict[str, str], sources: list[str]) -> Any:
    @task
    def load_one(source: str, frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        arguments = [
            "load-raw",
            "--source",
            source,
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
            "PJ360_BATCH_INGEST_COMMAND", arguments, default_command="pj360 ingest-batch"
        )

    return load_one.partial(frozen_window=window).expand(source=sources)


@task_group(group_id="transform")
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
                "path:models/staging",
                "path:models/intermediate",
                "reconciliation_result",
                "dq_volume_anomaly",
                "dq_issue",
                "dq_quarantine",
                "remediation_case",
                "pipeline_run_audit",
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return build_pre_publish_models(window)


@task_group(group_id="reconciliation_gate")
def reconciliation_gate_group(window: dict[str, str]) -> Any:
    @task
    def assert_balanced(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        run_dbt(
            [
                "test",
                "--target",
                params["dbt_target"],
                "--select",
                "reconciliation_result",
                "reconciliation_balances",
                "identity_cluster_has_no_verified_email_conflict",
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
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return assert_balanced(window)


@task_group(group_id="publish_curated")
def publish_group(window: dict[str, str]) -> Any:
    @task
    def publish_customer_360(frozen_window: dict[str, str]) -> None:
        params = get_current_context()["params"]
        run_dbt(
            [
                "build",
                "--target",
                params["dbt_target"],
                "--select",
                "path:models/core",
                "path:models/marts",
                "--vars",
                dbt_vars(frozen_window, params),
            ]
        )

    return publish_customer_360(window)


@task
def commit_watermarks(window: dict[str, str], sources: list[str]) -> None:
    params = get_current_context()["params"]
    arguments = [
        "commit-watermarks",
        "--sources-json",
        json.dumps(sources, separators=(",", ":")),
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
        "PJ360_BATCH_INGEST_COMMAND",
        arguments,
        default_command="pj360 ingest-batch",
    )


with DAG(
    dag_id=DAG_ID,
    description="Incremental and backfill-safe Customer 360 orchestration",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Australia/Melbourne"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": task_failure_callback,
    },
    params={
        "window_start": Param(
            "", type="string", description="Optional exclusive ISO-8601 start; pair with window_end"
        ),
        "window_end": Param(
            "", type="string", description="Optional inclusive ISO-8601 end; pair with window_start"
        ),
        "run_mode": Param("INCREMENTAL", enum=["INCREMENTAL", "BACKFILL", "REPLAY"]),
        "replay_id": Param(
            "", type="string", description="Required by the ingestion adapter for REPLAY mode"
        ),
        "sources": Param(ALLOWED_SOURCES, type="array", items={"type": "string"}),
        "dbt_target": Param("dev", type="string", pattern="^[A-Za-z0-9_-]+$"),
    },
    render_template_as_native_obj=True,
    tags=["propertyjourney", "customer-360", "batch"],
) as dag:
    frozen = freeze_window()
    sources = selected_sources()
    extracted = extract_group(frozen, sources)
    loaded = load_raw_group(frozen, sources)
    transformed = transform_group(frozen)
    reconciled = reconciliation_gate_group(frozen)
    published = publish_group(frozen)
    committed = commit_watermarks(frozen, sources)

    extracted >> loaded >> transformed >> reconciled >> published >> committed
