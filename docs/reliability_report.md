# Reliability report

Generated evidence must identify the command, source-tree/commit identity, environment, input hashes
and timestamp. This document never converts targets into actuals.

## Acceptance scorecard

| Claim | Target | Actual | Evidence state |
|---|---:|---:|---|
| Identity precision | >= 99% | 100% cluster; 100% pair | LOCALLY_VERIFIED Python reference, synthetic held-out |
| Identity recall | >= 95% | 98.1555% cluster; 95.6522% pair | LOCALLY_VERIFIED Python reference, synthetic held-out |
| Critical invalid rows in curated | 0 | 0 in local 325-record slice | LOCALLY_VERIFIED synthetic |
| Duplicate curated rows after replay | 0 | 0 in local idempotency tests | LOCALLY_VERIFIED; warehouse pending |
| Reconciliation explained | 100% | 100% (325/325) local slice | LOCALLY_VERIFIED synthetic |
| P95 event-to-curated | < 5 minutes | Not observed | Requires real Pub/Sub/BigQuery run |
| Scheduled success | >= 99.5% | Not observed | Requires owner-approved sustained run |
| Committed secrets | 0 | 0 findings in pre-commit worktree scan | LOCALLY_VERIFIED; history rescan in CI |

## Failure matrix

| Failure | Expected behaviour | Proof gate |
|---|---|---|
| HTTP 429/5xx | bounded backoff/jitter; no watermark advance | passing connector failure tests; live rate-limit pending |
| Interrupted extraction | existing raw object reused; run resumes/restarts safely | passing content-addressed/create-only landing tests |
| Malformed record | quarantined with reason; count reconciles | contract/DQ test |
| Duplicate input/event | one curated row; duplicate disposition retained | passing event and landing idempotency tests |
| Late arrival | affected business partitions recomputed/merged | dbt integration test in BigQuery |
| Schema v2 | v1/v2 accepted; v1 rejects the v2-only field | passing event contract tests |
| Reconciliation mismatch | curated publish and watermark blocked | unit + DAG gate test |
| Conflicting cluster identifiers | merge rejected for review | cluster-conflict test |

## Validation levels

- Local unit/contract results prove deterministic code behaviour only.
- `dbt parse` proves project structure, not BigQuery execution.
- Terraform validation proves configuration shape, not created resources/IAM effectiveness.
- Cloud verification requires exact resource IDs, run IDs, checks, reconciliation and rerun evidence.
- A sustained reliability claim requires an owner-approved 7-14 day scheduled run and teardown/cost
  evidence.

## Identity evaluation evidence

The Python reference implementation of frozen ruleset `identity-v1.1.0` was evaluated with seed
`20260825`, 1,000 synthetic customers and
an entity-isolated 200-customer held-out split (653 source identities; 212,878 evaluated pairs).
Final canonical clusters produced zero false-positive merges, 100% precision and 98.1555% recall.
The 95% Wilson intervals were 99.4870%-100% for precision and 96.9278%-98.8981% for recall. Direct
pair decisions had 100% precision and 95.6522% recall; transitive cluster links explain the higher
final recall. The exact inputs and hashes are in
[`evidence/identity_evaluation.json`](../evidence/identity_evaluation.json). dbt uses the documented
email/phone/name/address/postcode subset with the same versioned weights and thresholds; trusted
cross-system identifiers remain Python-reference-only. Parse evidence is not warehouse execution or
a transfer of the Python evaluator's performance claim.

This is representative deterministic synthetic regression evidence, not a real-customer or cloud
performance claim.

## Local platform evidence

The current local gate contains 169 passing tests and 86.00% branch coverage. The Docker API image,
Compose model, healthy loopback API/Airflow services, two parsed DAGs, a packaged 125-row ingestion
task, 36-model dbt parse, Terraform validation, PostgreSQL source counts, locked dependency audit
and Gitleaks scan were executed on 2026-08-24. Exact command tails, output hashes, timestamps and
limitations are recorded in
[`local_validation.json`](../evidence/local_validation.json),
[`local_pipeline_validation.json`](../evidence/local_pipeline_validation.json) and
[`platform_validation.json`](../evidence/platform_validation.json).
