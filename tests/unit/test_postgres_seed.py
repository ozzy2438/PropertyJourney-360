from propertyjourney360.synthetic.postgres import build_operational_seed_rows


def test_operational_rows_are_deterministic_defective_and_ground_truth_free() -> None:
    first = build_operational_seed_rows(seed=20260824, customer_count=20)
    second = build_operational_seed_rows(seed=20260824, customer_count=20)

    assert first == second
    # Five intentional duplicate-source rows exercise idempotent source-key handling.
    assert len(first.legacy_customers) == 25
    assert len(first.support_customers) == 20
    assert len(first.support_cases) == 21
    assert len(first.consent_events) == 23
    assert first.support_cases[-1]["customer_source_record_id"] == "MISSING-SUPPORT-CUSTOMER"
    assert any(row["consent_status"] == "WITHDRAWN" for row in first.consent_events)
    assert all("truth_customer_id" not in row["source_payload"] for row in first.legacy_customers)
