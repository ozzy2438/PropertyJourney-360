# ADR 0004: Supported Salesforce APIs and frozen watermarks

- Status: Accepted
- Date: 2026-08-24

## Context

Salesforce must be a real integration with OAuth, pagination, incremental extraction, rate limits,
schema inspection and larger-load support.

## Decision

Use an owner-configured Salesforce External Client App/compatible supported OAuth flow. REST SOQL
handles bounded extracts and follows `nextRecordsUrl`; Bulk API 2.0 handles justified larger queries.
Incremental windows use `SystemModstamp > low AND <= frozen_high`. Secret material stays outside Git.

## Consequences

Mock tests prove connector behaviour only. `CLOUD_VERIFIED` requires a real Developer org, schema
describe, two incremental runs, delete/tombstone handling, a controlled 429/failure and exact run
evidence. Client app creation/authentication is an owner gate.
