from pathlib import Path

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.pipeline import run_local_pipeline


def test_local_pipeline_is_reconcilable_and_idempotent(tmp_path: Path) -> None:
    output = tmp_path / "output"
    database = tmp_path / "local.db"

    first = run_local_pipeline(
        output_root=output,
        database_path=database,
        seed=42,
        customer_count=12,
    )
    second = run_local_pipeline(
        output_root=output,
        database_path=database,
        seed=42,
        customer_count=12,
    )

    assert first.reconciliation.status.value == "PASSED"
    assert first.reconciliation.counts.difference == 0
    assert first.landing_manifest.sha256 == second.landing_manifest.sha256
    assert first.source_identity_count == second.source_identity_count
    assert first.canonical_customer_count == second.canonical_customer_count
    assert first.customer_journey_event_count == second.customer_journey_event_count == 3
    assert first.held_out_pair_metrics.false_positive_merges == 0
    assert first.held_out_cluster_metrics.false_positive_merges == 0

    repository = CustomerRepository(database)
    quarantined = repository.list_remediation(limit=1000, offset=0)
    assert len(quarantined) == first.reconciliation.counts.quarantined
    for item in quarantined:
        assert (
            repository.customer_for_source(
                source_system=item["source_system"],
                source_record_id=item["source_record_id"],
            )
            is None
        )
    with repository.connect() as connection:
        curated_identity_count = connection.execute(
            "SELECT count(*) FROM customer_identity_source"
        ).fetchone()[0]
    assert curated_identity_count == first.reconciliation.counts.accepted
