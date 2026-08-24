# Data quality, quarantine and remediation

## Contract and disposition

Validation occurs before curated publication. Each input receives exactly one mutually exclusive
disposition, so the conservation equation is auditable:

```text
source = accepted + quarantined + rejected + explained_duplicates
```

A non-zero difference blocks publication and watermark advancement. Zero-row runs still produce a
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
| Reconciliation | counts plus drillable record hashes | publication gate |

The synthetic generator deliberately includes duplicate identities/events, malformed emails,
format differences, stale addresses, orphans, consent conflict, late arrivals and schema v1/v2.

## Quarantine

Quarantined rows retain raw object URI, record hash, source ID, contract/rule version, issue code,
first/last seen timestamps and remediation case. They do not vanish and do not enter trusted marts.

## Remediation lifecycle

```text
OPEN -> IN_REVIEW -> RESOLVED -> REPLAYED -> VERIFIED -> CLOSED
```

Transitions cannot skip or reopen states. `REPLAYED` requires a real replay run ID. `VERIFIED`
requires an independent verification result ID. Correcting a UI status cannot impersonate pipeline
execution. Replay reads the original versioned/checksummed payload, writes a new run and is subject
to the same idempotency, DQ and reconciliation gates.

## Controlled schema evolution

Property event v2 adds optional `device_type`. The consumer accepts v1 and v2, raw preserves the
version and staging produces null for v1. Compatibility is proven by replay/backfill tests; removing
or changing existing semantics requires a new contract and migration ADR.
