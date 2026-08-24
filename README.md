# PropertyJourney 360

PropertyJourney 360 is a company-neutral, production-oriented Customer 360 platform for Australian
property journeys. It combines authoritative public Australian property/business/economic data
with synthetic private identities and behaviour, then produces an explainable canonical customer,
auditable remediation records and personalisation-ready features.

The design is precision-first: uncertain identities are never silently merged, original source IDs
are retained, invalid inputs are quarantined, and every successful run must reconcile:

```text
source = accepted + quarantined + rejected + explained duplicates
```

## What is implemented

- Deterministic defective synthetic identities, PostgreSQL operational sources and held-out ground
  truth evaluation.
- Salesforce OAuth/SOQL/Bulk API 2.0 connector with frozen windows, pagination, schema inspection,
  retry/backoff and rate-limit handling.
- Licensed-source catalogue plus bounded live acquisition for RBA, City of Melbourne CLUE and
  Vicmap Address; raw source payloads remain outside Git.
- Checksummed local/GCS landing, deterministic BigQuery load jobs and explicit watermark controls.
- 36 dbt models across staging, identity, core, marts and governance, including SCD2, quarantine,
  remediation and reconciliation contracts.
- Airflow 3.3 batch/event DAGs with parameterised backfill/replay and post-reconciliation watermark
  commit gates.
- Versioned `IDENTIFY` event contract, idempotent Pub/Sub processing and bounded anonymous-session
  linkage.
- Small authenticated FastAPI data product. Local mode reads SQLite; Cloud Run mode uses
  parameterised, byte-capped BigQuery reads and refuses insecure cloud configuration.
- Terraform for GCS, BigQuery, Pub/Sub/DLQ, Secret Manager, private Cloud Run, observability, budgets,
  least-privilege service accounts and GitHub Workload Identity Federation.
- Locked dependencies, pinned images/actions/providers, CI, secret scanning and operational docs.

## Architecture

```text
Salesforce / PostgreSQL / support / consent / licensed public data
                              |
                    local Docker Airflow
                              |
                 checksummed GCS raw landing
                              |
                    BigQuery RAW + dbt
                              |
         identity resolution + quarantine/remediation
                              |
          Customer 360 + feature marts -> private FastAPI

Web events -> Pub/Sub -> validated at-least-once consumer -> BigQuery -> dbt
```

## Locally verified evidence

- 77 tests passed with 86.27% branch coverage.
- Held-out synthetic identity evaluation: 100% cluster precision, 98.1555% cluster recall and zero
  false-positive merges (200 held-out customers; 653 source identities).
- Local vertical slice: 325 source identities fully reconciled; 280 accepted, 45 quarantined and
  zero unexplained records. Anonymous view/save activity was linked only after `IDENTIFY`.
- Three authoritative public-source responses were fetched and content-hashed, including one
  explicitly filtered 25-record Vicmap Address page; this is not a statewide completeness claim.
- PostgreSQL 17, private local API and the minimal Airflow components ran healthy; two DAGs were
  discovered but remain paused by default.
- dbt parse, Terraform 1.15.9 validation, production-image build, locked dependency audit and
  Gitleaks worktree scan passed locally.

These are local observations, not Salesforce or GCP runtime claims. See [STATUS.md](STATUS.md) and
[evidence/](evidence/) for exact scope and hashes.

## Quick start

Prerequisites: Python 3.12, Docker with Compose and `make`.

```bash
python3.12 -m pip install --disable-pip-version-check --no-deps uv==0.12.5
uv sync --locked --all-extras
make lint format-check typecheck test audit
uv run pj360 run-local
docker compose up --build --detach
curl http://127.0.0.1:8000/health
```

Airflow is then available only on loopback at `http://127.0.0.1:8080`. The local DAGs are paused and
cloud extraction commands are intentionally unset. Use [.env.example](.env.example) as a names-only
configuration guide; never put real secrets in Git.

## Safety and remaining owner gates

- Private people, support cases, consent and events are synthetic.
- Proprietary property portals are not scraped; the unverified VIPS transaction layer is disabled.
- Salesforce Developer Edition credentials/seed execution and a real incremental extraction remain
  owner-gated and unobserved.
- GCP project selection, WIF bootstrap, Terraform plan/apply, dbt execution against BigQuery and
  private Cloud Run verification remain owner-gated and unobserved.
- The 7-14 day scheduled reliability run has not occurred; latency, success-rate and cloud-cost
  targets remain targets, not actuals.

## Documentation

Start with [architecture](docs/architecture.md), [data model](docs/data_model.md),
[identity resolution](docs/identity_resolution.md), [data quality](docs/data_quality.md),
[security](docs/security.md), [runbook](docs/runbook.md) and the
[reliability scorecard](docs/reliability_report.md).

## Licence

Project code is MIT licensed. Source datasets retain their own terms and are not redistributed
unless the captured source policy permits it.
