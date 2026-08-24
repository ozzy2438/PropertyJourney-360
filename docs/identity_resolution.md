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

Version `identity-v1.1.0` applies deterministic rules first. The Python reference uses
`SequenceMatcher`; dbt uses BigQuery `EDIT_DISTANCE` for the shared email/phone/name/address/postcode
feature and threshold subset, so their performance evidence is deliberately kept separate. The
current RAW warehouse contract has no trusted cross-system identifier field; that exact-match rule
is explicitly Python-reference-only rather than silently claimed as warehouse parity:

`config/identity_rules.yml` is the reviewed declarative record of this frozen ruleset, not a runtime
configuration loader. The evidence report hashes that file together with both implementations, and
contract tests assert their shared rule ordering and deterministic confidence values to expose drift.

1. Python only: exact trusted cross-system identifier.
2. Python and dbt: exact email when both source records mark it verified.
3. Python and dbt: exact email plus a strongly agreeing name; a verified source raises confidence.
4. Python and dbt: exact normalised phone plus a strongly agreeing name; postcode agreement raises
   confidence.

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

`AUTO_MATCH` edges do not automatically imply an unconstrained union. Before joining clusters, both
implementations check accumulated verified emails; the Python reference additionally checks trusted
identifiers because only its input contract currently carries that field. A contradiction rejects
the cluster edge for remediation. Persisted registry assignments are supplied on rerun so newly
arrived sources cannot silently replace issued `customer_id` values.

## Evaluation

The deterministic generator provides labelled source identities and train/calibration versus
held-out seeds. Reports include:

- candidate-blocking recall;
- pair precision/recall/F1;
- false-positive merges and false negatives;
- cluster-level false merges;
- manual-review rate;
- dataset size, seed, defect distribution and rule version.

The held-out report evaluates the Python reference resolver only. It is local synthetic regression
evidence, not proof of production precision and not evidence that the dbt implementation achieves
the same metrics; dbt requires a labelled BigQuery run before such a claim can be made.

## Anonymous-to-known journey

Only an explicit authenticated `IDENTIFY` event links prior activity. Linking is confined to one
session and a configured history window. Duplicate events remain idempotent. A second customer ID on
the session generates `REVIEW_REQUIRED`; device, IP, email similarity or other fingerprints never
silently resolve it.
