# Data quality, quarantine and remediation

## Contract and disposition

Validation occurs before curated publication. Each input receives exactly one mutually exclusive
disposition, so the conservation equation is auditable:

```text
source = accepted + quarantined + rejected + explained_duplicates
```

A non-zero difference blocks publication and watermark advancement. Reconciliation filters both
RAW and staging by the frozen Airflow `_window_id` and covers all 11 declared RAW assets. Explained
duplicates are measured by stable source key inside that same window; a missing row is never
inferred to be a duplicate merely because RAW exceeds staging. Zero-row runs still produce a
manifest and reconciliation result.

## Control families

| Control | Examples | Failure route |
|---|---|---|
| Contract/schema | required IDs, types, version compatibility | quarantine or hard source stop |
| Domain | email/phone/postcode/event type/consent values | quarantine with reason |
| Entity | uniqueness and stable ingestion key | explained duplicate |
| Relationship | customer/account/property/source foreign keys | quarantine/DQ issue |
| Temporal | source freshness, late/clock-invalid events, SCD overlap | issue or quarantine |
| Volume | bounded change from recent successful baseline | pipeline gate |
| Reconciliation | window-scoped counts plus issue/ingestion lineage keys | publication gate |

The synthetic generator deliberately includes duplicate identities/events, malformed emails,
format differences, stale addresses, orphans, consent conflict, late arrivals and schema v1/v2.

## Quarantine

Warehouse quarantine rows currently retain record/source type, nullable source ID, deterministic
lineage ingestion key, issue code, severity and detection time. Null-key rows receive a window-local
lineage key so they remain distinct. Immutable raw object URI/SHA, manifest version and first/last
seen metadata remain in operator reports but are not yet joined into the BigQuery quarantine model;
that drill-through is an explicit cloud implementation gate. Quarantined rows do not enter trusted
marts.

## Remediation lifecycle

```text
OPEN -> IN_REVIEW -> RESOLVED -> REPLAYED -> VERIFIED -> CLOSED
```

Transitions are append-only BigQuery events and cannot skip or reopen states. A deterministic job ID
and idempotency-first BigQuery transaction recover a lost response; transactional `ASSERT` checks
reject missing, stale or illegal predecessors. `REPLAYED` requires a real replay run ID. `VERIFIED`
requires an independent verification result ID. Correcting a UI status cannot impersonate pipeline
execution. Replay reads the original versioned/checksummed payload, writes a new run and is subject
to the same idempotency, DQ and reconciliation gates. Real BigQuery concurrency remains an external
cloud verification gate.

## Controlled schema evolution

Property event v2 adds optional `device_type`. The consumer accepts v1 and v2, raw preserves the
version and staging produces null for v1. Compatibility is proven by replay/backfill tests; removing
or changing existing semantics requires a new contract and migration ADR.
