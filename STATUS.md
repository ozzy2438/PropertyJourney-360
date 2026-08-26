# PropertyJourney 360 status

Last updated: 2026-08-26 (Australia/Melbourne)

## Current verdict

The production-shaped foundation is implemented and locally verified. The repository is a
completion candidate for independent final control and owner-gated Salesforce/GCP validation; it
is not represented as a live production deployment.

## Locally verified

- 176 tests pass; branch coverage is 86.09% against an 85% gate.
- Ruff lint/format and strict mypy pass across 40 source files.
- The Python reference identity resolver meets the held-out synthetic targets: 100% cluster
  precision, 98.1555% recall, zero false-positive merges. The documented dbt subset remains
  cloud-unexecuted; it does not implement the Python-only trusted cross-system-ID signal and does
  not inherit that evidence claim.
- The local 325-record identity slice reconciles exactly: 280 accepted + 45 quarantined.
- RBA, City of Melbourne CLUE, a bounded Vicmap Address response and three pinned ABS Census
  Victorian-SA2 responses were acquired and hashed; the ABS adapter produced 524 joined rows.
- Three pinned VGV 2025 annual workbooks were browser-acquired and locally verified by exact
  filename, byte count, SHA-256 and XLSX schema. They produced 16,241 house/unit/land suburb-year
  median rows with source quality markers and composite lineage; absent sales volumes were not
  invented and preliminary 2026 values were excluded.
- PostgreSQL 17, API and minimal Airflow 3.3.1 stack run healthy; both DAGs are discovered and paused.
- The packaged Airflow operator extracted and immutably landed 125 real local PostgreSQL rows for a
  frozen window; its dbt 1.12.3 CLI parsed the mounted project from writable run directories.
- dbt parses 36 models without credentials; Terraform 1.15.9 validates with Google provider 7.45.0.
- BigQuery Sandbox ADC connectivity was owner-authorised and verified against project
  `propertyjourney-360` in `australia-southeast1` with a guarded literal `SELECT 1`:
  `total_bytes_processed=0`. This verifies authentication and zero-byte query execution only; it
  does not verify datasets, dbt models, deployed infrastructure, billing configuration or cost.
- Salesforce Developer Edition Client Credentials Flow was owner-authorised and verified against
  the org's My Domain. The configured Run As User resolved as active and an authenticated
  `User/describe` REST call succeeded with the `api` scope. Tokens, credentials and raw user
  identifiers are not persisted in repository evidence. Synthetic seeding and incremental
  extraction remain unexecuted.
- Production API image builds with its cloud BigQuery dependencies; locked audit and Gitleaks pass.

Machine-readable reports live in [`evidence/`](evidence/).

## Implemented but not externally verified

- Salesforce Developer Edition seeding and live incremental extraction.
- GCS/BigQuery/Pub/Sub/Secret Manager/Cloud Run runtime behaviour and IAM effectiveness.
- BigQuery dbt build/test/replay, query bytes, event-to-curated latency and real cloud cost.
- BigQuery quarantine-to-raw-manifest URI/SHA drill-through and live remediation concurrency.
- GitHub-to-GCP WIF bootstrap and protected deploy workflow.
- A privacy-filtered ABN organisation extract; catalogue/fetch gates exist, but no absent payload is
  claimed as acquired or normalised.

## Owner gates remaining

1. Seed synthetic Salesforce contacts/cases and execute a bounded frozen-window extraction using
   the verified Developer Edition OAuth connection.
2. Select the GCP project/billing controls, bootstrap WIF, review a saved Terraform plan and apply.
3. Load bounded sources, execute `dbt build`/tests/replay in BigQuery and inspect exact job/run IDs.
4. Deploy the API by immutable image digest, verify private invocation and capture cost/bytes.
5. If approved, run the paused schedules for 7-14 days before reporting sustained reliability.

## ABN integration: optional, deferred, currently out of scope

The owner has deferred the ABN organisation-extract source. `abn_lookup_bulk_extract` is set to
`enabled: false` in `config/sources.yml`, so `pj360 fetch-public` and `pj360 validate-sources`
treat it as unavailable and no download, GUID request or normalisation runs against it. The
catalogue entry and the downstream Salesforce/dbt ABN-field wiring are left in place unmodified;
re-enabling the source, choosing an access path (the two public bulk ZIP resources are roughly
496 MiB each, or the bounded web service which requires an owner-issued authentication GUID) and
acquiring the data all remain a future, separately-approved step.

No merge, production apply, secret creation or sustained paid run is performed automatically.
