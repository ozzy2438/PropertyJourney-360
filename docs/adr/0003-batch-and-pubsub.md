# ADR 0003: Batch ingestion plus Pub/Sub behavioural events

- Status: Accepted
- Date: 2026-08-24

## Context

Operational/public sources change on bounded schedules, while customer property activity benefits
from lower-latency event processing.

## Decision

Airflow orchestrates batch/incremental sources. Pub/Sub delivers versioned behavioural events to a
validated private consumer. Delivery is treated as at least once and deduplicated by `event_id`.

## Consequences

The platform does not claim end-to-end exactly-once. Late/out-of-order events are retained and marts
recompute affected windows. Spark, Kafka, Flink and Kubernetes are unnecessary.
