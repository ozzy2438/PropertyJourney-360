# Data model

## Common raw envelope

Every operational record is wrapped before raw load:

| Field | Meaning |
|---|---|
| `schema_version` | Version of the source contract/parser |
| `source_system`, `entity_type` | Provenance and source object |
| `source_record_id` | Original, never discarded |
| `source_updated_at` | Source business/system change time |
| `extracted_at`, `ingested_at` | Pipeline event times |
| `run_id`, `object_uri`, `payload_sha256` | Audit and replay identity |
| `operation` | `UPSERT` or `DELETE`/tombstone |
| `payload` | Original source representation |

`RecordDisposition` assigns each input exactly one of `ACCEPTED`, `QUARANTINED`, `REJECTED` or
`DUPLICATE`, with a machine-readable reason and record hash.

## Warehouse layers

| Layer | Models | Grain / intent |
|---|---|---|
| RAW | Salesforce account/contact/case, legacy customer, support, consent, events, property, market, ABS, RBA, manifests | Append-only source versions |
| STAGING | Source-specific `stg_*` models | Typed, renamed, conservatively normalised source records |
| INTERMEDIATE | identity candidates/features/decisions, event links | Explainable pipeline working sets |
| CORE | `dim_customer`, history, account, property, bridges, interaction/support/consent facts | Trusted dimensional product |
| MARTS | Customer 360 current, engagement, personalisation features | Consumer-ready, documented metrics |
| GOVERNANCE | run audit, watermark, DQ issue/quarantine, remediation, reconciliation, contract version | Operability and evidence |

The executable dbt project currently contains 36 models. Local application storage additionally
contains the event/session identity links needed to prove the explicit `IDENTIFY` journey. Future
registry/event-history extensions are not represented as existing warehouse tables.

## Customer keys and history

- `customer_id` is a stable key derived from the connected source-identity component, never directly
  from email, phone, name or address values.
- `bridge_customer_source_identity` maps each original source key to a canonical customer with rule
  version, decision and effective history.
- Cluster merges retain a survivor and redirects; cluster splits are remediation events, not silent
  rewrites.
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

Incremental MERGE statements constrain both source and target to affected partitions. Late arrivals
are selected by ingestion time and rewrite/merge only affected business-date partitions. API query
adapters use parameters and a maximum-bytes-billed cap.

## Personalisation features

The mart exposes data features, not a recommendation model: `views_7d`, `views_30d`,
`saved_properties_30d`, `enquiries_30d`, preferred suburbs/type, observed price band,
`last_activity_at` and engagement frequency. Feature timestamps, windows and null semantics are part
of the contract.

## Geography

Suburb/postcode names are not equated to SA2. A future enrichment must use a versioned official
correspondence and preserve fractional allocations or unmatched records; no geography bridge is
claimed in the current 36-model build.
