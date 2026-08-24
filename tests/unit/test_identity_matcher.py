from datetime import UTC, datetime

from propertyjourney360.domain.models import IdentityRecord, MatchDecision, SourceSystem
from propertyjourney360.identity.evaluation import evaluate_clusters, evaluate_matches
from propertyjourney360.identity.matcher import IdentityResolver

NOW = datetime(2026, 8, 24, tzinfo=UTC)


def _record(
    source: SourceSystem,
    record_id: str,
    *,
    email: str | None = None,
    verified: bool = False,
    phone: str | None = None,
    first_name: str | None = "Alex",
    last_name: str | None = "Morgan",
    postcode: str | None = "3000",
    truth: str = "truth-1",
) -> IdentityRecord:
    return IdentityRecord(
        source_system=source,
        source_record_id=record_id,
        source_updated_at=NOW,
        email=email,
        email_verified=verified,
        phone=phone,
        first_name=first_name,
        last_name=last_name,
        postcode=postcode,
        truth_customer_id=truth,
    )


def test_verified_exact_email_is_an_automatic_match() -> None:
    resolver = IdentityResolver()
    left = _record(SourceSystem.SALESFORCE, "1", email="A@EXAMPLE.COM", verified=True)
    right = _record(SourceSystem.LEGACY_CRM, "2", email="a@example.com", verified=True)

    result = resolver.compare(left, right)

    assert result.decision is MatchDecision.AUTO_MATCH
    assert result.score == 0.995


def test_conflicting_verified_emails_never_auto_merge() -> None:
    resolver = IdentityResolver()
    left = _record(
        SourceSystem.SALESFORCE,
        "1",
        email="left@example.com",
        verified=True,
        phone="0412 345 678",
    )
    right = _record(
        SourceSystem.LEGACY_CRM,
        "2",
        email="right@example.com",
        verified=True,
        phone="0412 345 678",
    )

    result = resolver.compare(left, right)

    assert result.decision is not MatchDecision.AUTO_MATCH
    assert any(item.comparator == "verified_conflict" for item in result.evidence)


def test_sparse_name_only_signal_cannot_auto_merge() -> None:
    resolver = IdentityResolver()
    left = _record(SourceSystem.SALESFORCE, "1", postcode=None)
    right = _record(SourceSystem.LEGACY_CRM, "2", postcode=None)

    result = resolver.compare(left, right)

    assert result.decision is MatchDecision.NO_MATCH


def test_phone_name_and_postcode_can_auto_match() -> None:
    resolver = IdentityResolver()
    left = _record(SourceSystem.SALESFORCE, "1", phone="0412 345 678")
    right = _record(SourceSystem.SUPPORT, "2", phone="+61 412 345 678")

    assert resolver.compare(left, right).decision is MatchDecision.AUTO_MATCH


def test_canonical_assignments_are_deterministic_and_review_edges_do_not_merge() -> None:
    resolver = IdentityResolver()
    records = [
        _record(SourceSystem.SALESFORCE, "1", email="same@example.com", verified=True),
        _record(SourceSystem.LEGACY_CRM, "2", email="same@example.com", verified=True),
        _record(
            SourceSystem.SUPPORT,
            "3",
            email="different@example.com",
            verified=True,
            truth="truth-2",
        ),
    ]
    matches = resolver.compare_all(records)

    first = resolver.canonical_assignments(records, matches)
    second = resolver.canonical_assignments(records, reversed(matches))

    assert first == second
    assert first[records[0].source_key] == first[records[1].source_key]
    assert first[records[0].source_key] != first[records[2].source_key]


def test_ground_truth_metrics_distinguish_false_merges_and_misses() -> None:
    resolver = IdentityResolver()
    records = [
        _record(SourceSystem.SALESFORCE, "1", email="same@example.com", verified=True),
        _record(SourceSystem.LEGACY_CRM, "2", email="same@example.com", verified=True),
        _record(
            SourceSystem.SUPPORT,
            "3",
            email="other@example.com",
            verified=True,
            truth="truth-2",
        ),
    ]

    metrics = evaluate_matches(records, resolver.compare_all(records))

    assert metrics.precision == 1
    assert metrics.recall == 1
    assert metrics.false_positive_merges == 0


def test_transitive_cluster_join_is_blocked_on_verified_email_conflict() -> None:
    resolver = IdentityResolver()
    records = [
        _record(
            SourceSystem.SALESFORCE,
            "a",
            email="first@example.com",
            verified=True,
            phone="0412 345 678",
        ),
        _record(
            SourceSystem.LEGACY_CRM,
            "b",
            email=None,
            phone="0412 345 678",
        ),
        _record(
            SourceSystem.SUPPORT,
            "c",
            email=None,
            phone="0499 999 999",
        ),
        _record(
            SourceSystem.WEB,
            "d",
            email="second@example.com",
            verified=True,
            phone="0499 999 999",
        ),
    ]
    # Supply a trusted bridge edge between the two records with missing emails.
    bridge_b = records[1].model_copy(update={"trusted_cross_system_id": "bridge-1"})
    bridge_c = records[2].model_copy(update={"trusted_cross_system_id": "bridge-1"})
    records = [records[0], bridge_b, bridge_c, records[3]]
    matches = resolver.compare_all(records)

    resolution = resolver.resolve_clusters(records, matches)

    assert resolution.conflicts
    assert (
        resolution.assignments[records[0].source_key]
        != resolution.assignments[records[3].source_key]
    )


def test_existing_canonical_id_is_preserved_when_a_source_joins() -> None:
    resolver = IdentityResolver()
    records = [
        _record(SourceSystem.SALESFORCE, "z", email="same@example.com", verified=True),
        _record(SourceSystem.LEGACY_CRM, "a", email="same@example.com", verified=True),
    ]
    existing = {records[0].source_key: "customer-issued-earlier"}

    assignments = resolver.canonical_assignments(
        records,
        resolver.compare_all(records),
        existing_assignments=existing,
    )

    assert set(assignments.values()) == {"customer-issued-earlier"}


def test_cluster_metrics_measure_final_co_membership() -> None:
    resolver = IdentityResolver()
    records = [
        _record(SourceSystem.SALESFORCE, "1", email="same@example.com", verified=True),
        _record(SourceSystem.LEGACY_CRM, "2", email="same@example.com"),
        _record(
            SourceSystem.SUPPORT,
            "3",
            email="other@example.com",
            truth="truth-2",
        ),
    ]
    matches = resolver.compare_all(records)
    assignments = resolver.resolve_clusters(records, matches).assignments

    metrics = evaluate_clusters(records, assignments)

    assert metrics.precision == 1
    assert metrics.recall == 1
