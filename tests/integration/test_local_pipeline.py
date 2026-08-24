from pathlib import Path

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
