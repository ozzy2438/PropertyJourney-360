# ADR 0002: Local Apache Airflow as required orchestration

- Status: Accepted
- Date: 2026-08-24

## Context

Scheduling, retry, backfill and dependency gates matter, but a continuously running managed
orchestrator is disproportionate for a portfolio project.

## Decision

Use pinned Apache Airflow 3 locally through Compose. DAGs stay thin and invoke typed application
services. Cloud Composer is an optional, owner-approved validation milestone, not a baseline.

## Consequences

CI validates DAG structure/imports and commands. Local Airflow evidence is not relabelled as managed
GCP evidence. No Celery/Redis/Kubernetes is introduced without a measured requirement.
