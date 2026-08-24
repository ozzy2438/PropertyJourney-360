# ADR 0005: Precision-first explainable identity resolution

- Status: Accepted
- Date: 2026-08-24

## Context

False customer merges can disclose or corrupt data. Aggressive recall is not worth silent
cross-person linkage.

## Decision

Apply conservative normalisation, deterministic high-confidence rules, bounded candidate blocking,
weighted explainable scoring and three outcomes: `AUTO_MATCH`, `REVIEW_REQUIRED`, `NO_MATCH`.
Cluster joins reject conflicting verified identifiers. Issued canonical IDs live in a registry and
ground truth remains unavailable to matching logic.

## Consequences

Manual review is expected. Every decision/evidence contribution and rule version is retained.
Threshold targets require held-out evidence; a small synthetic point estimate is not a production
precision claim.
