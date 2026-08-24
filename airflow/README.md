# PropertyJourney 360 Airflow

These Airflow 3.3 DAGs are intentionally thin. They freeze one UTC half-open
window, invoke the repository's ingestion operators, materialise staging/intermediate governance,
gate the exact pipeline run on reconciliation, publish curated core/mart models, and only then
commit watermarks or event checkpoints.

The default executable command prefixes are `pj360 ingest-batch` and
`pj360 ingest-events`. They may be overridden in the scheduler/worker environment:

- `PJ360_BATCH_INGEST_COMMAND`
- `PJ360_EVENT_INGEST_COMMAND`
- `PJ360_DBT_COMMAND` (defaults to `dbt`)
- `PJ360_DBT_LOG_PATH` and `PJ360_DBT_TARGET_PATH` (must be writable; Compose uses the shared Airflow logs volume)
- `PJ360_FAILURE_CALLBACK_COMMAND` (optional alert adapter)

Commands are tokenised with `shlex` and executed without a shell. The repository
operators own extraction, landing, validation, replay and idempotency logic; the
DAGs do not contain business transformations.

The container keeps dbt in `/opt/dbt-venv`, isolated from Airflow's own Python
dependency set. This avoids allowing one tool to silently upgrade or downgrade
the other's Google client libraries.

## GCP identity wiring

Default local Compose deliberately mounts no GCP credential. For an approved cloud validation,
set `PJ360_ADC_FILE` to an existing user or workload-federated Application Default Credentials
file; set `PJ360_INGEST_SERVICE_ACCOUNT`, `PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT` and
`DBT_BIGQUERY_IMPERSONATE_SERVICE_ACCOUNT` to the corresponding Terraform outputs; then use:

```bash
docker compose -f compose.yaml -f compose.gcp.yaml config
docker compose -f compose.yaml -f compose.gcp.yaml up --detach
```

The override mounts the broker credential read-only. Python batch clients impersonate only the
ingest account, Python event clients impersonate only the event-consumer account, and dbt's
BigQuery adapter impersonates only the transform account. The broker principal must be granted
`roles/iam.serviceAccountTokenCreator` on each target service account by an owner. Do not create a
service-account key or store ADC content in `.env`, Git, images or Airflow metadata.

For a backfill or replay, provide both `window_start` and `window_end` as
timezone-aware ISO-8601 values. Extractors apply `(window_start, window_end]`, so adjacent windows
neither overlap nor omit boundary records. The frozen window ID is deterministic for the
Airflow run and is passed unchanged to every task. `replay_id` is also forwarded
to adapters so they can enforce source idempotency. Batch replays read the immutable extract
report and landing manifest produced by the referenced original run; they never call the source
extractor again. The audit `pipeline_run_id` still uses the real Airflow window ID so separate
replay runs never overwrite one another.

LIVE event ACK recovery treats the immutable, content-addressed landing manifest as the durable
disposition boundary. After a crash, the operator re-ACKs matching redeliveries that are visible and
then continues from the already-landed payload; an ACK whose outcome cannot be observed may
redeliver in a later run, where immutable event first-write-wins rules collapse an exact duplicate
or quarantine a divergent collision. Recovery therefore does not require Pub/Sub to redeliver every
previously landed event before the current run can load its durable manifest.
