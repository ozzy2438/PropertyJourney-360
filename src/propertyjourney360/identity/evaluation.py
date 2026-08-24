"""Ground-truth evaluation for synthetic identity-resolution datasets."""

from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.domain.models import IdentityRecord, MatchDecision, MatchResult


class IdentityMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluated_pairs: int = Field(ge=0)
    true_positives: int = Field(ge=0)
    false_positive_merges: int = Field(ge=0)
    false_negatives: int = Field(ge=0)
    true_negatives: int = Field(ge=0)
    manual_review_pairs: int = Field(ge=0)
    precision: float = Field(ge=0, le=1)
    recall: float = Field(ge=0, le=1)
    f1: float = Field(ge=0, le=1)
    manual_review_rate: float = Field(ge=0, le=1)


class ClusterMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluated_pairs: int = Field(ge=0)
    true_positives: int = Field(ge=0)
    false_positive_merges: int = Field(ge=0)
    false_negatives: int = Field(ge=0)
    true_negatives: int = Field(ge=0)
    precision: float = Field(ge=0, le=1)
    recall: float = Field(ge=0, le=1)
    f1: float = Field(ge=0, le=1)


def evaluate_matches(
    records: Sequence[IdentityRecord],
    matches: Sequence[MatchResult],
) -> IdentityMetrics:
    """Evaluate automatic merges; REVIEW_REQUIRED intentionally counts as not merged."""

    truth_by_key = {record.source_key: record.truth_customer_id for record in records}
    if any(value is None for value in truth_by_key.values()):
        raise ValueError("all evaluation records require truth_customer_id")

    tp = fp = fn = tn = reviews = 0
    for match in matches:
        left_truth = truth_by_key[match.left_source_key]
        right_truth = truth_by_key[match.right_source_key]
        actual_same = left_truth == right_truth
        predicted_same = match.decision is MatchDecision.AUTO_MATCH
        reviews += match.decision is MatchDecision.REVIEW_REQUIRED
        if actual_same and predicted_same:
            tp += 1
        elif not actual_same and predicted_same:
            fp += 1
        elif actual_same and not predicted_same:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    pair_count = len(matches)
    return IdentityMetrics(
        evaluated_pairs=pair_count,
        true_positives=tp,
        false_positive_merges=fp,
        false_negatives=fn,
        true_negatives=tn,
        manual_review_pairs=reviews,
        precision=round(precision, 6),
        recall=round(recall, 6),
        f1=round(f1, 6),
        manual_review_rate=round(reviews / pair_count, 6) if pair_count else 0.0,
    )


def evaluate_clusters(
    records: Sequence[IdentityRecord],
    assignments: dict[str, str],
) -> ClusterMetrics:
    """Evaluate final canonical co-membership, including safe transitive links."""

    if any(record.truth_customer_id is None for record in records):
        raise ValueError("all evaluation records require truth_customer_id")
    missing = {record.source_key for record in records} - assignments.keys()
    if missing:
        raise ValueError(f"canonical assignments missing {len(missing)} source identities")

    tp = fp = fn = tn = 0
    for left_index, left in enumerate(records):
        for right in records[left_index + 1 :]:
            actual_same = left.truth_customer_id == right.truth_customer_id
            predicted_same = assignments[left.source_key] == assignments[right.source_key]
            if actual_same and predicted_same:
                tp += 1
            elif not actual_same and predicted_same:
                fp += 1
            elif actual_same and not predicted_same:
                fn += 1
            else:
                tn += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return ClusterMetrics(
        evaluated_pairs=tp + fp + fn + tn,
        true_positives=tp,
        false_positive_merges=fp,
        false_negatives=fn,
        true_negatives=tn,
        precision=round(precision, 6),
        recall=round(recall, 6),
        f1=round(f1, 6),
    )
