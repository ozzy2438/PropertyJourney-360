#!/usr/bin/env python3
"""Run the frozen held-out synthetic identity evaluation and persist measured evidence."""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from math import sqrt
from pathlib import Path

from propertyjourney360.evidence import (
    ConfidenceInterval,
    EvidenceLevel,
    IdentityEvaluationReport,
)
from propertyjourney360.identity.evaluation import evaluate_clusters, evaluate_matches
from propertyjourney360.identity.matcher import IdentityResolver
from propertyjourney360.synthetic import (
    DatasetSplit,
    SyntheticIdentityConfig,
    generate_identity_dataset,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "evidence" / "identity_evaluation.json"
HASHED_INPUTS = (
    Path("config/identity_rules.yml"),
    Path("scripts/evaluate_identity.py"),
    Path("src/propertyjourney360/identity/matcher.py"),
    Path("src/propertyjourney360/identity/normalization.py"),
    Path("src/propertyjourney360/synthetic/identities.py"),
)


def _wilson(successes: int, total: int, *, z: float = 1.959963984540054) -> ConfidenceInterval:
    if total <= 0:
        raise ValueError("Wilson interval requires a positive denominator")
    proportion = successes / total
    denominator = 1 + z**2 / total
    centre = proportion + z**2 / (2 * total)
    margin = z * sqrt((proportion * (1 - proportion) + z**2 / (4 * total)) / total)
    return ConfidenceInterval(
        confidence=0.95,
        lower=round((centre - margin) / denominator, 6),
        upper=round((centre + margin) / denominator, 6),
        method="Wilson score",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=20260825)
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    dataset = generate_identity_dataset(
        SyntheticIdentityConfig(seed=args.seed, customer_count=args.customers)
    )
    records = dataset.records_for_split(DatasetSplit.HELD_OUT)
    resolver = IdentityResolver()
    matches = resolver.compare_all(records)
    resolution = resolver.resolve_clusters(records, matches)
    pair_metrics = evaluate_matches(records, matches)
    cluster_metrics = evaluate_clusters(records, resolution.assignments)
    precision_denominator = cluster_metrics.true_positives + cluster_metrics.false_positive_merges
    recall_denominator = cluster_metrics.true_positives + cluster_metrics.false_negatives
    target_precision = 0.99
    target_recall = 0.95
    passed = (
        cluster_metrics.precision >= target_precision
        and cluster_metrics.recall >= target_recall
        and cluster_metrics.false_positive_merges == 0
    )
    report = IdentityEvaluationReport(
        generated_at=datetime.now(UTC),
        ruleset_version=resolver.config.rule_version,
        evaluation_seed=args.seed,
        total_synthetic_customers=args.customers,
        held_out_customer_count=len(dataset.held_out_truth_customer_ids),
        held_out_source_identity_count=len(records),
        pair_metrics=pair_metrics,
        cluster_metrics=cluster_metrics,
        cluster_conflict_count=len(resolution.conflicts),
        cluster_precision_interval_95=_wilson(
            cluster_metrics.true_positives,
            precision_denominator,
        ),
        cluster_recall_interval_95=_wilson(
            cluster_metrics.true_positives,
            recall_denominator,
        ),
        source_hashes={str(path): _sha256(ROOT / path) for path in HASHED_INPUTS},
        target_precision=target_precision,
        target_recall=target_recall,
        target_status="PASSED" if passed else "FAILED",
        evidence_level=EvidenceLevel.LOCALLY_VERIFIED,
        scope_limitation=(
            "Deterministic synthetic held-out evidence; does not establish real-world or cloud "
            "identity performance."
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report.model_dump_json(indent=2))
    print(report.model_dump_json(indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
