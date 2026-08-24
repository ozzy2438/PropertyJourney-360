# Identity resolution

## Safety objective

The engine optimises for very high precision. `REVIEW_REQUIRED` is a valid product outcome; it is
safer than a false merge. Synthetic ground truth is held separately and never passed into matching.

## Normalisation

- Email: trim and case-fold; malformed values become unusable signals. Provider-specific dot or
  plus-alias rewriting is deliberately absent.
- Australian phone: plausible national numbers become E.164; short/foreign/implausible values are
  not guessed.
- Name/address: Unicode decomposition, punctuation/whitespace normalisation and conservative street
  tokens. Originals remain in raw.
- Postcode: four-digit Australian range validation.

## Pair decisions

Version `identity-v1.1.0` applies deterministic rules first:

1. Exact trusted cross-system identifier.
2. Exact email when both source records mark it verified.
3. Exact email plus a strongly agreeing name; a verified source raises confidence.
4. Exact normalised phone plus a strongly agreeing name; postcode agreement raises confidence.

Otherwise, comparable email/phone/name/address/postcode signals carry weights 0.35/0.30/0.20/0.10/
0.05. The mean similarity is reduced when evidence coverage is sparse. Defaults are:

- `AUTO_MATCH`: score at least 0.94 and coverage at least 0.70;
- `REVIEW_REQUIRED`: score at least 0.70;
- otherwise `NO_MATCH`.

Conflicting verified emails prohibit automatic merge even when another field agrees. Every result
stores normalised comparison values, comparator, similarity, weight, contribution, threshold/rule
version and decision timestamp.

## Candidate generation and clustering

The Python fixture evaluator can compare all pairs only because its datasets are small. Warehouse
candidate generation blocks on exact normalised identifiers and bounded name/geography keys; an
unbounded all-pairs join is prohibited.

`AUTO_MATCH` edges do not automatically imply an unconstrained union. Before joining clusters, the
engine checks accumulated verified emails and trusted identifiers. A contradiction rejects the
cluster edge for remediation. Persisted registry assignments are supplied on rerun so newly arrived
sources cannot silently replace issued `customer_id` values.

## Evaluation

The deterministic generator provides labelled source identities and train/calibration versus
held-out seeds. Reports include:

- candidate-blocking recall;
- pair precision/recall/F1;
- false-positive merges and false negatives;
- cluster-level false merges;
- manual-review rate;
- dataset size, seed, defect distribution and rule version.

A small fixture point estimate is local regression evidence, not proof that production precision is
at least 99%. The target remains separate until an adequately sized held-out evaluation is executed.

## Anonymous-to-known journey

Only an explicit authenticated `IDENTIFY` event links prior activity. Linking is confined to one
session and a configured history window. Duplicate events remain idempotent. A second customer ID on
the session generates `REVIEW_REQUIRED`; device, IP, email similarity or other fingerprints never
silently resolve it.
