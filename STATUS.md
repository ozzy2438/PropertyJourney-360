# PropertyJourney 360 status

Last updated: 2026-08-24 (Australia/Melbourne)

## Current verdict

The production-shaped foundation is implemented and locally verified. It is ready for review and
owner-gated Salesforce/GCP validation; it is not represented as a live production deployment.

## Locally verified

- 77 tests pass; branch coverage is 86.27% against an 85% gate.
- Ruff lint/format and strict mypy pass across 35 source files.
- Held-out identity evaluation meets the synthetic targets: 100% cluster precision, 98.1555%
  recall, zero false-positive merges.
- The local 325-record identity slice reconciles exactly: 280 accepted + 45 quarantined.
- RBA, City of Melbourne CLUE and a bounded Vicmap Address response were acquired and hashed.
- PostgreSQL 17, API and minimal Airflow 3.3.1 stack run healthy; both DAGs are discovered and paused.
- dbt parses 36 models without credentials; Terraform 1.15.9 validates with Google provider 7.45.0.
- Production API image builds with its cloud BigQuery dependencies; locked audit and Gitleaks pass.

Machine-readable reports live in [`evidence/`](evidence/).

## Implemented but not externally verified

- Salesforce Developer Edition seeding and live incremental extraction.
- GCS/BigQuery/Pub/Sub/Secret Manager/Cloud Run runtime behaviour and IAM effectiveness.
- BigQuery dbt build/test/replay, query bytes, event-to-curated latency and real cloud cost.
- GitHub-to-GCP WIF bootstrap and protected deploy workflow.

## Owner gates remaining

1. Create/approve Salesforce Developer Edition OAuth credentials outside Git, seed synthetic
   contacts/cases and execute a bounded frozen-window extraction.
2. Select the GCP project/billing controls, bootstrap WIF, review a saved Terraform plan and apply.
3. Load bounded sources, execute `dbt build`/tests/replay in BigQuery and inspect exact job/run IDs.
4. Deploy the API by immutable image digest, verify private invocation and capture cost/bytes.
5. If approved, run the paused schedules for 7-14 days before reporting sustained reliability.

No merge, production apply, secret creation or sustained paid run is performed automatically.
