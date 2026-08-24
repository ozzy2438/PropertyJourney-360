from datetime import UTC, datetime

from propertyjourney360.evidence import EvidenceClaim, EvidenceLevel, LocalEvidenceReport


def test_target_can_remain_unobserved_without_fake_actual() -> None:
    claim = EvidenceClaim(
        claim_id="event_latency",
        description="Cloud event-to-curated P95 latency.",
        level=EvidenceLevel.IMPLEMENTED,
        target="< 5 minutes",
        actual=None,
        evidence_status="NOT_OBSERVED",
        limitation="No cloud run configured.",
    )
    report = LocalEvidenceReport(
        generated_at=datetime(2026, 8, 24, tzinfo=UTC),
        environment="local",
        python_version="3.12.14",
        source_tree_sha256="a" * 64,
        checks=(),
        claims=(claim,),
    )

    assert report.claims[0].target == "< 5 minutes"
    assert report.claims[0].actual is None
