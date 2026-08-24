# PropertyJourney 360 Airflow

These Airflow 3.3 DAGs are intentionally thin. They freeze one UTC half-open
window, invoke deployment-provided ingestion adapters, run dbt selections, gate
on reconciliation, and only then commit watermarks or event checkpoints.

Configure these executable command prefixes in the scheduler/worker environment:

- `PJ360_BATCH_INGEST_COMMAND`
- `PJ360_EVENT_INGEST_COMMAND`
- `PJ360_DBT_COMMAND` (defaults to `dbt`)
- `PJ360_FAILURE_CALLBACK_COMMAND` (optional alert adapter)

Commands are tokenised with `shlex` and executed without a shell. The ingestion
adapters own extraction, landing, validation, replay and idempotency logic; the
DAGs do not contain business transformations.

The container keeps dbt in `/opt/dbt-venv`, isolated from Airflow's own Python
dependency set. This avoids allowing one tool to silently upgrade or downgrade
the other's Google client libraries.

For a backfill or replay, provide both `window_start` and `window_end` as
timezone-aware ISO-8601 values. The frozen window ID is deterministic for the
Airflow run and is passed unchanged to every task. `replay_id` is also forwarded
to adapters so they can enforce idempotency.
