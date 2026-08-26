# ADR 0006: Checksummed, versioned raw landing and replay

- Status: Accepted
- Date: 2026-08-24

## Context

Recovery, audit and late corrections require original inputs. Overwriting or silently dropping bad
records prevents proof.

## Decision

Land payloads under content-addressed paths with manifests, SHA-256, source window, schema version
and licence metadata. Configure GCS versioning and application-level append-only writes. Replay reads
the original object and creates a new run subject to the same controls.

## Consequences

GCS is not described as legally immutable without a separately approved retention lock. Lifecycle
and cost controls apply to dev objects. Corrupt/tampered hashes stop publication.
