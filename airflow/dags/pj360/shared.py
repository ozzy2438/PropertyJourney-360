"""Infrastructure-only helpers shared by the thin Airflow DAGs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shlex
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

import pendulum
from airflow.exceptions import AirflowException

LOGGER = logging.getLogger(__name__)


def _parse_timestamp(value: str, *, parameter_name: str) -> pendulum.DateTime:
    """Parse one timezone-aware ISO-8601 orchestration parameter."""
    try:
        parsed = pendulum.parse(value, strict=True)
    except (TypeError, ValueError) as exc:
        raise AirflowException(f"{parameter_name} must be an ISO-8601 timestamp") from exc
    if not isinstance(parsed, pendulum.DateTime) or parsed.tzinfo is None:
        raise AirflowException(f"{parameter_name} must include a timezone offset")
    return parsed.in_timezone("UTC")


def freeze_data_window(
    context: Mapping[str, Any],
    *,
    dag_id: str,
    default_interval_seconds: int,
) -> dict[str, str]:
    """Resolve an immutable window once; all downstream tasks consume this XCom."""
    params = context["params"]
    requested_start = str(params.get("window_start") or "").strip()
    requested_end = str(params.get("window_end") or "").strip()
    if bool(requested_start) != bool(requested_end):
        raise AirflowException("window_start and window_end must be supplied together")

    if requested_start:
        start = _parse_timestamp(requested_start, parameter_name="window_start")
        end = _parse_timestamp(requested_end, parameter_name="window_end")
        window_source = "PARAMETER"
    else:
        interval_start = context.get("data_interval_start")
        interval_end = context.get("data_interval_end")
        if (
            interval_start is not None
            and interval_end is not None
            and interval_end > interval_start
        ):
            start = interval_start.in_timezone("UTC")
            end = interval_end.in_timezone("UTC")
            window_source = "DATA_INTERVAL"
        else:
            end = context["logical_date"].in_timezone("UTC")
            start = end.subtract(seconds=default_interval_seconds)
            window_source = "LOGICAL_DATE_FALLBACK"

    if end <= start:
        raise AirflowException("window_end must be later than window_start")

    start_iso = start.to_iso8601_string()
    end_iso = end.to_iso8601_string()
    run_id = str(context["run_id"])
    window_id = hashlib.sha256(f"{dag_id}|{run_id}|{start_iso}|{end_iso}".encode()).hexdigest()
    return {
        "window_id": window_id,
        "window_start": start_iso,
        "window_end": end_iso,
        "window_source": window_source,
        "airflow_run_id": run_id,
    }


def run_configured_command(
    command_environment_variable: str,
    arguments: Sequence[str],
    *,
    default_command: str | None = None,
) -> None:
    """Run a configured executable without a shell or interpolated command string."""
    configured = os.getenv(command_environment_variable, default_command or "").strip()
    if not configured:
        raise AirflowException(
            f"Required command environment variable {command_environment_variable} is not set"
        )
    argv = [*shlex.split(configured), *[str(argument) for argument in arguments]]
    timeout_seconds = int(os.getenv("PJ360_TASK_TIMEOUT_SECONDS", "3600"))
    LOGGER.info("Running configured command %s with %d arguments", argv[0], len(argv) - 1)
    try:
        subprocess.run(  # noqa: S603 - executable allow-list is deployment configuration.
            argv,
            check=True,
            shell=False,
            timeout=timeout_seconds,
            env=os.environ.copy(),
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise AirflowException(f"Configured command {argv[0]} failed") from exc


def run_dbt(arguments: Sequence[str]) -> None:
    """Invoke dbt with deployment-provided project/profile paths."""
    if not arguments:
        raise AirflowException("dbt arguments must include a subcommand")
    project_dir = os.getenv("PJ360_DBT_PROJECT_DIR", "/opt/airflow/dbt")
    profiles_dir = os.getenv("PJ360_DBT_PROFILES_DIR", "/opt/airflow/dbt")
    log_path = os.getenv("PJ360_DBT_LOG_PATH", "/opt/airflow/logs/dbt")
    target_path = os.getenv("PJ360_DBT_TARGET_PATH", "/opt/airflow/logs/dbt-target")
    run_configured_command(
        "PJ360_DBT_COMMAND",
        [
            arguments[0],
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
            "--log-path",
            log_path,
            "--target-path",
            target_path,
            *arguments[1:],
        ],
        default_command="dbt",
    )


def task_failure_callback(context: Mapping[str, Any]) -> None:
    """Emit a non-sensitive failure event and optionally invoke an alert adapter."""
    task_instance = context.get("task_instance")
    payload = {
        "dag_id": getattr(task_instance, "dag_id", context.get("dag", "unknown")),
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "run_id": context.get("run_id", "unknown"),
        "try_number": getattr(task_instance, "try_number", None),
        "exception_type": type(context.get("exception")).__name__,
    }
    LOGGER.error("PropertyJourney task failure: %s", json.dumps(payload, sort_keys=True))
    callback_command = os.getenv("PJ360_FAILURE_CALLBACK_COMMAND", "").strip()
    if callback_command:
        run_configured_command(
            "PJ360_FAILURE_CALLBACK_COMMAND",
            [json.dumps(payload, separators=(",", ":"), sort_keys=True)],
        )
