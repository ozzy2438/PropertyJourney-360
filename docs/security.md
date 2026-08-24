# Security

## Data classification

| Class | Examples | Handling |
|---|---|---|
| Public licensed | Vicmap, aggregate VGV/ABS/CLUE, permitted RBA/ABN fields | Licence/attribution manifest; raw bulk out of Git |
| Synthetic confidential | generated people, phone/email, consent, support, behaviour | Treat operationally as PII; least privilege and masking |
| Secret | OAuth client secret, API token, GCP credentials | Secret Manager/local keychain; never logs, chat, Git or Terraform state |
| Governance | match evidence, quarantine, remediation | Restricted steward/operator access; audit retained |

## Identity and access

- GitHub Actions authenticates to GCP with repository/ref/environment-restricted Workload Identity
  Federation and short-lived credentials. Mapped `workflow_ref` values also bind Terraform and
  image publication to separate service accounts; neither workflow can impersonate the other's
  account.
- Separate service accounts prevent the API, event consumer, ingestion and transformations from
  inheriting each other's privileges.
- Cloud-enabled Airflow uses an owner-supplied ADC broker solely to mint one-hour impersonated
  credentials: batch clients target `ingest`, event clients target `event_consumer`, and dbt targets
  `transform`. The Compose override mounts ADC read-only; default local Compose mounts nothing.
- Cloud Run services are private. The API refuses to start in `cloud` with local auth bypass.
- `/remediation` and customer endpoints require authentication; identity email/phone values are
  masked by default.
- GCS enforces public-access prevention; BigQuery uses explicit dataset IAM. The cloud API has
  dataset-viewer access only to core/marts/governance plus project-level job execution.
- Cloud mode instantiates a read-only BigQuery repository; local SQLite fallback cannot be selected
  because `PJ360_ENVIRONMENT=cloud` also requires auth and all dataset coordinates.

## Secrets

Terraform creates Secret Manager containers and IAM bindings, not secret payloads. Owners enter
secrets via an authenticated terminal. Structured logging filters credentials/tokens and tests check
that connector exceptions do not expose secret values.

The ADC broker principal requires target-scoped `roles/iam.serviceAccountTokenCreator` on the three
runtime accounts before a cloud run. This is an explicit owner bootstrap action and is intentionally
not broadened to a project-wide token-creation grant. User-managed service-account JSON keys are not
an accepted runtime bootstrap mechanism.

## Supply chain and CI

- Runtime images, GitHub Actions and critical providers are pinned; automated updates are reviewed.
- CI runs lint, strict typing, tests/coverage, dependency audit, secret scan, dbt parse, Terraform
  validation and container build.
- Workflow permissions default to read-only. `id-token: write` exists only in protected GCP jobs.
- Image publication impersonates a dedicated repository-scoped Artifact Registry writer; it never
  receives the Terraform deployment account's project-administration roles.
- Pull requests cannot automatically apply infrastructure or deploy production.

## Threat controls

| Threat | Control |
|---|---|
| False identity merge | high thresholds, verified-conflict block, cluster constraint, audit/review |
| Object overwrite/tamper | content hash, application append-only paths, bucket versioning |
| Replay duplicates | stable ingestion/event keys and MERGE/unique tests |
| Forged remediation state | append-only events, legal predecessor validation and atomic BigQuery ASSERT |
| Public data licence breach | executable source catalogue and disabled unverified sources |
| Query/cost abuse | parameterised endpoints, pagination, maximum bytes billed, private service |
| Cross-project credential theft | WIF conditions, no long-lived JSON key, minimum workflow permission |
