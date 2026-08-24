# ADR 0001: BigQuery as the primary analytical warehouse

- Status: Accepted
- Date: 2026-08-24

## Context

The project targets GCP/BigQuery capability, append-heavy multi-source data, dimensional marts and
bounded feature computation while keeping portfolio operations inexpensive.

## Decision

Use BigQuery datasets for RAW, staging, core, marts and governance. Partition event/fact workloads
by justified dates, cluster common bounded filters and use dbt incremental MERGE with affected-target
partition predicates. Small dimensions are not partitioned by default.

## Consequences

Local SQLite/PostgreSQL tests do not prove BigQuery behaviour. Cloud evidence requires an ephemeral
real dataset, `dbt build`, query/job IDs and bytes processed. Query APIs must be parameterised and
set maximum bytes billed.
