"""Explainable identity matching with conservative automatic-merge gates."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from difflib import SequenceMatcher
from itertools import combinations
from uuid import NAMESPACE_URL, uuid5

from propertyjourney360.domain.models import (
    IdentityRecord,
    MatchDecision,
    MatchEvidence,
    MatchResult,
)
from propertyjourney360.identity.normalization import NormalizedIdentity, normalize_identity


@dataclass(frozen=True, slots=True)
class ResolverConfig:
    rule_version: str = "identity-v1.1.0"
    automatic_threshold: float = 0.94
    review_threshold: float = 0.70
    minimum_automatic_coverage: float = 0.70

    def __post_init__(self) -> None:
        if not 0 <= self.review_threshold < self.automatic_threshold <= 1:
            raise ValueError("thresholds must satisfy 0 <= review < automatic <= 1")
        if not 0 <= self.minimum_automatic_coverage <= 1:
            raise ValueError("minimum automatic coverage must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class ClusterConflict:
    left_source_key: str
    right_source_key: str
    reason: str


@dataclass(frozen=True, slots=True)
class ClusterResolution:
    assignments: dict[str, str]
    conflicts: tuple[ClusterConflict, ...]


_WEIGHTS = {
    "email": 0.35,
    "phone": 0.30,
    "name": 0.20,
    "address": 0.10,
    "postcode": 0.05,
}


def _similarity(left: str, right: str, *, exact_only: bool) -> float:
    if left == right:
        return 1.0
    if exact_only:
        return 0.0
    return SequenceMatcher(a=left, b=right, autojunk=False).ratio()


class IdentityResolver:
    """Match source identities while reserving uncertain candidates for human review."""

    def __init__(self, config: ResolverConfig | None = None) -> None:
        self.config = config or ResolverConfig()

    def compare(self, left: IdentityRecord, right: IdentityRecord) -> MatchResult:
        left_normalized = normalize_identity(left)
        right_normalized = normalize_identity(right)
        evidence, weighted_sum, comparable_weight = self._score_fields(
            left_normalized,
            right_normalized,
        )

        deterministic_decision = self._deterministic_decision(
            left,
            right,
            left_normalized,
            right_normalized,
        )
        coverage = comparable_weight
        mean_similarity = weighted_sum / comparable_weight if comparable_weight else 0.0
        calibrated_score = mean_similarity * (0.55 + 0.45 * coverage)

        verified_email_conflict = bool(
            left.email_verified
            and right.email_verified
            and left_normalized.email
            and right_normalized.email
            and left_normalized.email != right_normalized.email
        )
        if verified_email_conflict:
            calibrated_score = min(calibrated_score, self.config.automatic_threshold - 0.01)
            evidence.append(
                MatchEvidence(
                    field="email",
                    comparator="verified_conflict",
                    left_normalized=left_normalized.email,
                    right_normalized=right_normalized.email,
                    similarity=0,
                    weight=0,
                    contribution=0,
                    note="Conflicting verified emails prohibit automatic merge.",
                )
            )

        if deterministic_decision and not verified_email_conflict:
            decision, score = deterministic_decision
        elif (
            calibrated_score >= self.config.automatic_threshold
            and coverage >= self.config.minimum_automatic_coverage
            and not verified_email_conflict
        ):
            decision, score = MatchDecision.AUTO_MATCH, calibrated_score
        elif calibrated_score >= self.config.review_threshold:
            decision, score = MatchDecision.REVIEW_REQUIRED, calibrated_score
        else:
            decision, score = MatchDecision.NO_MATCH, calibrated_score

        return MatchResult(
            left_source_key=left.source_key,
            right_source_key=right.source_key,
            score=round(score, 6),
            decision=decision,
            rule_version=self.config.rule_version,
            evidence=tuple(evidence),
        )

    def compare_all(self, records: Sequence[IdentityRecord]) -> list[MatchResult]:
        """Compare each candidate once. Warehouse-scale blocking occurs upstream in dbt."""

        return [self.compare(left, right) for left, right in combinations(records, 2)]

    def canonical_assignments(
        self,
        records: Sequence[IdentityRecord],
        matches: Iterable[MatchResult],
        *,
        existing_assignments: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Return conflict-aware canonical assignments for compatibility with existing callers."""

        return self.resolve_clusters(
            records,
            matches,
            existing_assignments=existing_assignments,
        ).assignments

    def resolve_clusters(
        self,
        records: Sequence[IdentityRecord],
        matches: Iterable[MatchResult],
        *,
        existing_assignments: dict[str, str] | None = None,
    ) -> ClusterResolution:
        """Build AUTO_MATCH components without joining contradictory verified identifiers.

        In a warehouse, an identity registry supplies ``existing_assignments`` so a later source
        arrival cannot replace an already issued canonical ID.
        """

        parent = {record.source_key: record.source_key for record in records}
        members = {record.source_key: {record.source_key} for record in records}
        normalized = {record.source_key: normalize_identity(record) for record in records}
        verified_emails = {
            record.source_key: (
                {normalized[record.source_key].email}
                if record.email_verified and normalized[record.source_key].email
                else set()
            )
            for record in records
        }
        trusted_ids = {
            record.source_key: (
                {normalized[record.source_key].trusted_cross_system_id}
                if normalized[record.source_key].trusted_cross_system_id
                else set()
            )
            for record in records
        }
        conflicts: list[ClusterConflict] = []

        def find(node: str) -> str:
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node

        def union(left: str, right: str) -> bool:
            left_root, right_root = find(left), find(right)
            if left_root == right_root:
                return True
            combined_verified_emails = verified_emails[left_root] | verified_emails[right_root]
            combined_trusted_ids = trusted_ids[left_root] | trusted_ids[right_root]
            if len(combined_verified_emails) > 1:
                conflicts.append(
                    ClusterConflict(left, right, "conflicting verified emails across clusters")
                )
                return False
            if len(combined_trusted_ids) > 1:
                conflicts.append(
                    ClusterConflict(left, right, "conflicting trusted identifiers across clusters")
                )
                return False
            keep, attach = sorted((left_root, right_root))
            parent[attach] = keep
            members[keep] |= members.pop(attach)
            verified_emails[keep] = combined_verified_emails
            trusted_ids[keep] = combined_trusted_ids
            return True

        for match in matches:
            if match.decision is MatchDecision.AUTO_MATCH:
                union(match.left_source_key, match.right_source_key)

        components: defaultdict[str, list[str]] = defaultdict(list)
        for source_key in sorted(parent):
            components[find(source_key)].append(source_key)

        assignments: dict[str, str] = {}
        for source_keys in components.values():
            previous_ids = {
                customer_id
                for source_key in source_keys
                if (customer_id := (existing_assignments or {}).get(source_key))
            }
            if len(previous_ids) > 1:
                raise ValueError("component contains multiple previously issued canonical IDs")
            stable_anchor = min(source_keys)
            customer_id = next(iter(previous_ids), None) or str(
                uuid5(NAMESPACE_URL, f"propertyjourney360:{stable_anchor}")
            )
            assignments.update(dict.fromkeys(source_keys, customer_id))
        return ClusterResolution(assignments=assignments, conflicts=tuple(conflicts))

    @staticmethod
    def _deterministic_decision(
        left: IdentityRecord,
        right: IdentityRecord,
        left_normalized: NormalizedIdentity,
        right_normalized: NormalizedIdentity,
    ) -> tuple[MatchDecision, float] | None:
        if (
            left_normalized.trusted_cross_system_id
            and left_normalized.trusted_cross_system_id == right_normalized.trusted_cross_system_id
        ):
            return MatchDecision.AUTO_MATCH, 1.0
        if (
            left.email_verified
            and right.email_verified
            and left_normalized.email
            and left_normalized.email == right_normalized.email
        ):
            return MatchDecision.AUTO_MATCH, 0.995
        name_similarity = (
            _similarity(left_normalized.name, right_normalized.name, exact_only=False)
            if left_normalized.name and right_normalized.name
            else 0.0
        )
        if (
            left_normalized.email
            and left_normalized.email == right_normalized.email
            and name_similarity >= 0.90
        ):
            score = 0.99 if left.email_verified or right.email_verified else 0.985
            return MatchDecision.AUTO_MATCH, score
        if (
            left_normalized.phone
            and left_normalized.phone == right_normalized.phone
            and name_similarity >= 0.96
            and left_normalized.postcode
            and left_normalized.postcode == right_normalized.postcode
        ):
            return MatchDecision.AUTO_MATCH, 0.975
        if (
            left_normalized.phone
            and left_normalized.phone == right_normalized.phone
            and name_similarity >= 0.94
        ):
            return MatchDecision.AUTO_MATCH, 0.97
        return None

    @staticmethod
    def _score_fields(
        left: NormalizedIdentity,
        right: NormalizedIdentity,
    ) -> tuple[list[MatchEvidence], float, float]:
        evidence: list[MatchEvidence] = []
        weighted_sum = 0.0
        comparable_weight = 0.0
        exact_fields = {"phone", "postcode"}

        for field, weight in _WEIGHTS.items():
            left_value = getattr(left, field)
            right_value = getattr(right, field)
            if not left_value or not right_value:
                continue
            similarity = _similarity(
                left_value,
                right_value,
                exact_only=field in exact_fields,
            )
            contribution = similarity * weight
            weighted_sum += contribution
            comparable_weight += weight
            evidence.append(
                MatchEvidence(
                    field=field,
                    comparator="exact" if field in exact_fields else "sequence_similarity",
                    left_normalized=left_value,
                    right_normalized=right_value,
                    similarity=round(similarity, 6),
                    weight=weight,
                    contribution=round(contribution, 6),
                )
            )
        return evidence, weighted_sum, comparable_weight
