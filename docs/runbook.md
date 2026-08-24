# Operations runbook

## First response

1. Record environment, UTC and Australia/Melbourne time, alert/run ID, commit/image digest, DAG/task
   and affected source/window.
2. Stop promotion/publication if reconciliation, contract, identity or security gates failed. Do not
   advance the watermark manually.
3. Preserve raw object URI/checksum, logs and query/job IDs before retry or teardown.
4. Classify customer impact: delayed data, incomplete data, incorrect data, privacy/security or cost.
5. Prefer idempotent rerun/replay; never edit curated rows to make a dashboard green.

## Salesforce failure or rate limiting

- Check token endpoint versus data API failure without printing request credentials.
- Inspect remaining-usage headers and structured retry metadata.
- For 429/5xx, allow bounded jittered backoff; do not run parallel catch-up extracts that amplify
  throttling.
- Confirm the frozen high watermark and last successful low watermark.
- On schema failure, fetch `sObject/describe`, compare the recorded contract version and quarantine
  incompatible payloads.
- Resume the same extraction window. Advance state only after load, dbt gates and reconciliation.

## Failed dbt transformation

- Capture invocation ID, compiled SQL, model/test name and target dataset.
- Determine whether failure is compile, permission, cost limit, data contract or SQL/runtime.
- Do not label `dbt parse` as a successful warehouse build.
- Fix forward, rerun only affected models/partitions, then run downstream tests and reconciliation.

## Schema change

- Freeze the affected source watermark and preserve the first incompatible raw payload.
- Classify additive-compatible, additive-requiring-mapping, breaking or semantic.
- Add a versioned contract/parser; do not overwrite the prior version.
- Replay old and new fixtures, then a bounded real window. Record schema hash and migration ADR.

## Reconciliation mismatch

- Publication and watermark advancement remain blocked.
- Drill from aggregate counts to the disposition table and record hashes.
- Check duplicate classification, late rows, partial object load and cross-run overlap.
- Correct the pipeline, rerun the exact window and require a new `PASSED` result. Never change source
  counts to force equality.

## Pub/Sub backlog or poison event

- Inspect oldest unacked age, delivery attempts, consumer errors and DLQ volume.
- Scale consumer only after identifying whether the bottleneck is validation, BigQuery quota or a
  poison payload.
- Invalid events retain event ID/schema/reason in quarantine or DLQ. Ack only after durable routing.
- Replay through the same validator; `event_id` deduplication must prevent duplicate curated facts.

## Corrupted/tampered raw object

- Compare object bytes to the landing manifest SHA-256 and bucket version metadata.
- Stop downstream use, preserve the suspect object/version and audit access logs.
- Re-extract from the authority into a new content-addressed object; never replace evidence in place.
- Treat unexplained modification as a security incident.

## Remediation replay

1. Case must be `RESOLVED` with a documented correction.
2. Launch a parameterised replay against the original object/source key and create `replay_run_id`.
3. Run standard validation, idempotency, dbt and reconciliation controls.
4. Transition to `REPLAYED`, then an independent verifier records `verification_result_id`.
5. Close only from `VERIFIED`; skipped lifecycle states are rejected.

## Historical backfill

- Require source, start/end, reason, expected size/cost and idempotency key.
- Backfill bounded windows; do not alter the live incremental watermark until reconciliation passes.
- Constrain BigQuery target partitions and watch bytes processed/quarantine rate.
- Compare pre/post counts and feature windows; record run/query IDs and actual cost.

## Rollback and communication

Curated views or Cloud Run revisions can roll back to a known digest, but raw/audit history is never
deleted as rollback. Report observed facts, unknowns, customer impact, containment, next evidence
gate and owner action. Targets or expected behaviour are not incident evidence.
