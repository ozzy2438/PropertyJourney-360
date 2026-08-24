# Data model

## Common RAW metadata

Every explicit RAW table has a versioned typed schema. The ingestion adapter adds these common
columns before landing and load:

| Field | Meaning |
|---|---|
| `_schema_version` | Version of the source contract/parser |
| `_window_id` | Frozen Airflow window shared across extract/load/dbt/reconciliation |
| `_source_run_id` | Original window or explicit replay identity |
| `_ingested_at` | Pipeline ingestion timestamp and partition column |

Original source IDs and source update times remain typed asset columns. Object URI, extracted time,
record count and payload SHA-256 live in the create-only landing manifest rather than being copied
onto every row.

`RecordDisposition` assigns each input exactly one of `ACCEPTED`, `QUARANTINED`, `REJECTED` or
`DUPLICATE`, with a machine-readable reason and record hash.

## Warehouse layers

| Layer | Models | Grain / intent |
|---|---|---|
| RAW | Salesforce account/contact/case, legacy customer, support, consent, events, property, market, ABS, RBA | Append-only source versions; manifests remain operator evidence |
| STAGING | Source-specific `stg_*` models | Typed, renamed, conservatively normalised source records |
| INTERMEDIATE | identity candidates/features/decisions, event links | Explainable pipeline working sets |
| CORE | `dim_customer`, history, account, property, bridges, interaction/support/consent facts | Trusted dimensional product |
| MARTS | Customer 360 current, engagement, personalisation features | Consumer-ready, documented metrics |
| GOVERNANCE | run audit, watermark, DQ issue/quarantine, remediation, reconciliation, contract version | Operability and evidence |

The executable dbt project currently contains 36 models. Local application storage additionally
contains the event/session identity links needed to prove the explicit `IDENTIFY` journey. Future
registry/event-history extensions are not represented as existing warehouse tables.

## Customer keys and history

- `customer_id` is initially derived from the connected source-identity component, then persisted in
  the incremental identity registry; it is never directly derived from email, phone, name or address.
- Invalidated identities remain inactive in the registry for ID continuity but are excluded from
  curated dimensions/bridges. If previously co-clustered identities later split into conflicting
  verified emails, every affected mapping remains visible in the registry under its issued ID while
  the whole conflicting customer is held out of trusted dimensions/bridges.
- `bridge_customer_source_identity` maps each original source key to a canonical customer with rule
  version, decision and effective history.
- A component containing conflicting verified emails or multiple previously issued customer IDs is
  excluded from the trusted bridge and routed to remediation rather than silently merged.
- `dim_customer` contains current state. `dim_customer_history` is SCD Type 2 with non-overlapping
  effective intervals and exactly one current row per customer.
- Consent is a fact/history stream, never overwritten by a convenient “current” flag alone.

## BigQuery physical design

| Table class | Partition | Cluster | Reason |
|---|---|---|---|
| Raw/event manifests | ingestion date | source/entity | Bounded replay and source scans |
| Property interactions | event date | customer, property | Time-window feature workloads |
| Consent/support facts | effective/created date | customer | Lifecycle and customer queries |
| Small dimensions | none by default | only if measured useful | Avoid tiny partition overhead |

Incremental inputs are constrained to the frozen run window, while business-key MERGE operations
remain target-table scans unless BigQuery execution evidence justifies adding safe target partition
predicates. Late arrivals are selected by ingestion time; API query adapters use parameters and a
maximum-bytes-billed cap.

## Personalisation features

The mart exposes data features, not a recommendation model: `views_7d`, `views_30d`,
`saved_properties_30d`, `enquiries_30d`, preferred suburbs/type, observed price band,
`last_activity_at` and engagement frequency. Feature timestamps, windows and null semantics are part
of the contract.

## Geography

Suburb/postcode names are not equated to SA2. A future enrichment must use a versioned official
correspondence and preserve fractional allocations or unmatched records; no geography bridge is
claimed in the current 36-model build.
