# ADR 0007: Workload Identity Federation and separated service identities

- Status: Accepted
- Date: 2026-08-24

## Context

Long-lived service-account JSON keys and shared broad roles create avoidable credential and blast
radius risk.

## Decision

GitHub Actions uses repository/ref/environment-restricted GCP Workload Identity Federation.
Ingestion, transformation, API, event consumer and CI deploy use distinct service accounts. Secret
Manager holds secret payloads; Terraform manages containers/bindings only.

## Consequences

Bootstrap and protected deploy approval are owner gates. Workflow permissions default read-only and
only authenticated jobs receive `id-token: write`. IAM effectiveness must be smoke-tested after real
apply; Terraform validation alone is insufficient.
