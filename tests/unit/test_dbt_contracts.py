from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

RECONCILED_STAGING_MODELS = (
    "stg_salesforce_contacts.sql",
    "stg_salesforce_accounts.sql",
    "stg_salesforce_cases.sql",
    "stg_legacy_customers.sql",
    "stg_support_customers.sql",
    "stg_support_cases.sql",
    "stg_property_events.sql",
    "stg_marketing_consents.sql",
    "stg_property_master.sql",
    "stg_market_data.sql",
    "stg_abs_demographics.sql",
    "stg_rba_indicators.sql",
)


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_reconciled_staging_deduplicates_inside_each_window_and_retains_bad_keys() -> None:
    staging = ROOT / "dbt" / "models" / "staging"
    for model_name in RECONCILED_STAGING_MODELS:
        source = (staging / model_name).read_text(encoding="utf-8")
        row_number_clause = source.rsplit("row_number() over (", maxsplit=1)[1].split(
            ") as row_num", maxsplit=1
        )[0]
        fingerprint_fields = source.split("to_json_string(struct(", maxsplit=1)[1].split(
            ")))) as payload_fingerprint", maxsplit=1
        )[0]
        assert "orchestration_window_id" in row_number_clause, model_name
        assert "payload_fingerprint" in row_number_clause, model_name
        assert "where row_num = 1 or" in source, model_name
        assert "explained_duplicate_count" in source, model_name
        assert "payload_variant_count" in source, model_name
        assert "payload_duplicate_row_count" in source, model_name
        assert "count(distinct payload_fingerprint) over (" in source, model_name
        assert "when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'" in source, model_name
        assert "ingested_at" not in fingerprint_fields, model_name
        assert "orchestration_window_id" not in fingerprint_fields, model_name
        assert "_source_run_id" not in fingerprint_fields, model_name
        assert "_schema_version" not in fingerprint_fields, model_name


def test_valid_staging_rows_cannot_hide_conflicting_payload_variants() -> None:
    singular_test = _read("dbt/tests/valid_staging_rows_have_single_payload_variant.sql")
    for model_name in RECONCILED_STAGING_MODELS:
        assert f"ref('{model_name.removesuffix('.sql')}')" in singular_test, model_name
    assert singular_test.count("dq_status = 'VALID' and payload_variant_count > 1") == len(
        RECONCILED_STAGING_MODELS
    )


def test_divergent_payload_variants_are_each_retained_and_quarantined() -> None:
    singular_test = _read("dbt/tests/divergent_staging_payloads_are_fully_quarantined.sql")
    uniqueness_test = _read("dbt/tests/staging_business_keys_unique_per_window.sql")
    for model_name in RECONCILED_STAGING_MODELS:
        model = _read(f"dbt/models/staging/{model_name}")
        row_number_clause = model.rsplit("row_number() over (", maxsplit=1)[1].split(
            ") as row_num", maxsplit=1
        )[0]
        assert "payload_fingerprint" in row_number_clause, model_name
        assert "when payload_variant_count > 1" in model, model_name
        assert f"ref('{model_name.removesuffix('.sql')}')" in singular_test, model_name
    assert "count(*) != max(payload_variant_count)" in singular_test
    assert "countif(dq_status != 'CONFLICTING_DUPLICATE') > 0" in singular_test
    assert "sum(payload_duplicate_row_count - 1)" in singular_test
    assert uniqueness_test.count("payload_fingerprint having count(*) > 1") == len(
        RECONCILED_STAGING_MODELS
    )


def test_event_and_consent_ids_are_immutable_across_windows() -> None:
    events = _read("dbt/models/staging/stg_property_events.sql")
    consents = _read("dbt/models/staging/stg_marketing_consents.sql")
    immutable_test = _read("dbt/tests/valid_immutable_event_keys_have_one_payload.sql")
    first_write_test = _read("dbt/tests/immutable_event_first_write_wins.sql")
    reconciliation = _read("dbt/models/governance/reconciliation_result.sql")
    dq_issues = _read("dbt/models/governance/dq_issue.sql")

    assert "historical_payloads as (" in events
    assert "event_payload_stats as (" in events
    assert "canonical_payload_fingerprint" in events
    assert "immutable_history as (" in consents
    for source in (events, consents):
        assert "then 'EXACT_DUPLICATE'" in source
        assert "then 'EVENT_ID_COLLISION'" in source
    assert "count(distinct payload_fingerprint) > 1" in immutable_test
    assert "ref('stg_property_events')" in immutable_test
    assert "ref('stg_marketing_consents')" in immutable_test
    assert "expected_canonical_payload_fingerprint" in first_write_test
    assert "dq_status != 'EXACT_DUPLICATE'" in first_write_test
    assert "dq_status != 'EVENT_ID_COLLISION'" in first_write_test
    assert reconciliation.count("dq_status not in ('VALID', 'EXACT_DUPLICATE')") == 2
    assert dq_issues.count("dq_status not in ('VALID', 'EXACT_DUPLICATE')") == 2


def test_warehouse_event_contract_rejects_missing_identity_and_v1_device_type() -> None:
    events = _read("dbt/models/staging/stg_property_events.sql")
    assert "then 'MISSING_EVENT_IDENTITY'" in events
    assert "when schema_version = 1 and device_type is not null" in events
    assert "then 'V1_DEVICE_TYPE_NOT_ALLOWED'" in events


def test_identity_phone_rules_match_the_python_reference_order() -> None:
    sql = _read("dbt/models/intermediate/int_identity_candidates.sql")
    strict = "phone_exact and name_similarity >= 0.96 and postcode_exact"
    broad = "phone_exact and name_similarity >= 0.94"
    assert sql.index(strict) < sql.index(broad)
    assert sql.count(strict) == 2
    assert sql.count(broad) == 2


def test_identity_verified_email_scores_match_the_python_reference() -> None:
    sql = _read("dbt/models/intermediate/int_identity_candidates.sql")
    assert "when verified_email_exact then 0.995" in sql
    assert "when one_verified_email_exact and name_similarity >= 0.90 then 0.99" in sql
    assert "when email_exact and name_similarity >= 0.90 then 0.985" in sql
    verified_both = "when verified_email_exact then 0.995"
    verified_one = "when one_verified_email_exact and name_similarity >= 0.90 then 0.99"
    unverified = "when email_exact and name_similarity >= 0.90 then 0.985"
    assert sql.index(verified_both) < sql.index(verified_one) < sql.index(unverified)


def test_split_registry_customer_conflicts_are_not_curated_and_gate_publish() -> None:
    membership = _read("dbt/models/intermediate/int_identity_membership.sql")
    bridge = _read("dbt/models/core/bridge_customer_source_identity.sql")
    customer = _read("dbt/models/core/dim_customer.sql")
    dag = _read("airflow/dags/propertyjourney_customer_360.py")
    singular = _read("dbt/tests/conflicted_registry_mappings_are_not_curated.sql")
    safety_test = _read("dbt/tests/identity_cluster_has_no_verified_email_conflict.sql")

    assert "registry_customer_safety as (" in membership
    assert "group by membership.customer_id" in membership
    assert "registry_customer_safety.verified_email_count > 1" in membership
    assert "and not membership.cluster_conflict" in bridge
    assert "and not membership.cluster_conflict" in customer
    assert "identity_cluster_has_no_verified_email_conflict" in dag
    assert "inner join {{ ref('bridge_customer_source_identity') }}" in singular
    assert "countif(not membership.cluster_conflict) > 0" in safety_test


def test_anonymous_attribution_can_republish_bounded_prior_window_events() -> None:
    attribution = _read("dbt/models/intermediate/int_event_identity_attribution.sql")
    fact = _read("dbt/models/core/fact_customer_property_interaction.sql")
    assert "with event_history as" in attribution
    assert "target_events as" in attribution
    assert "events.session_id = identified.session_id" in attribution
    assert "identified.orchestration_window_id = '{{ run_window_id }}'" in attribution
    assert "anonymous_link_window_hours" in attribution
    assert "is_incremental() and not run_window_id" in fact


def test_reconciliation_gate_is_scoped_to_one_deterministic_pipeline_run() -> None:
    result = _read("dbt/models/governance/reconciliation_result.sql")
    test = _read("dbt/tests/reconciliation_balances.sql")
    assert "var('pipeline_run_id', invocation_id)" in result
    assert "pipeline_run_id = '{{ pipeline_run_id }}'" in test


def test_governance_watermarks_cover_every_reconciled_asset() -> None:
    watermarks = _read("dbt/models/governance/ingestion_watermark.sql")
    for asset in (
        "salesforce_accounts",
        "salesforce_contacts",
        "salesforce_cases",
        "legacy_customers",
        "support_cases",
        "marketing_consents",
        "property_events",
        "property_master",
        "property_market",
        "abs_demographics",
        "rba_indicators",
    ):
        assert f"'{asset}'" in watermarks
    assert watermarks.count("source_retrieved_at") == 3


def test_public_source_provenance_survives_raw_staging_and_property_core() -> None:
    raw_contracts = _read("src/propertyjourney360/ingestion/raw_contracts.py")
    terraform = _read("infra/terraform/bigquery.tf")
    core = _read("dbt/models/core/dim_property.sql")
    for field in (
        "source_retrieved_at",
        "source_payload_sha256",
        "source_payload_object",
    ):
        assert raw_contracts.count(f'"{field}"') >= 4
        assert field in terraform
        assert field in core
        for model in (
            "stg_property_master.sql",
            "stg_market_data.sql",
            "stg_abs_demographics.sql",
            "stg_rba_indicators.sql",
        ):
            assert field in _read(f"dbt/models/staging/{model}"), model


def test_duplicate_collisions_open_durable_remediation_cases() -> None:
    issues = _read("dbt/models/governance/dq_issue.sql")
    remediation = _read("dbt/models/governance/remediation_case.sql")
    singular = _read("dbt/tests/collision_issues_have_remediation_cases.sql")

    assert "'EVENT_ID_COLLISION'" in issues.split("then 'HIGH'", maxsplit=1)[0]
    medium = issues.split("then 'HIGH'", maxsplit=1)[1].split("then 'MEDIUM'", maxsplit=1)[0]
    assert "'CONFLICTING_DUPLICATE'" in medium
    assert "then 'DATA_INTEGRITY_COLLISION'" in issues
    assert "severity in ('HIGH', 'MEDIUM')" in remediation
    assert "left join {{ ref('remediation_case') }}" in singular


def test_cross_entity_references_require_trusted_current_parents() -> None:
    cases = _read("dbt/models/staging/stg_salesforce_cases.sql")
    events = _read("dbt/models/staging/stg_property_events.sql")
    assert "membership.is_active" in cases
    assert "customer_source_id is not null and not customer_exists" in cases
    assert "account_source_id is not null and not account_exists" in cases
    assert "customer_id is not null and not canonical_customer_exists" in events
    assert "customer_source_id is not null and not source_subject_exists" in events
    assert "customer_id != source_subject_customer_id" in events


def test_raw_contract_tests_allow_business_defects_to_reach_quarantine() -> None:
    sources = _read("dbt/models/sources.yml")
    governance = _read("dbt/models/governance/_governance.yml")
    raw_section = sources.split("- name: governance_input", maxsplit=1)[0]
    assert "- name: id\n" not in raw_section
    assert "- {name: source_record_id, data_type: string}" in governance


def test_closed_customer_history_does_not_require_a_current_dimension_row() -> None:
    core_contracts = _read("dbt/models/core/_core.yml")
    history_contract = core_contracts.split("- name: dim_customer_history", maxsplit=1)[1].split(
        "- name: dim_account", maxsplit=1
    )[0]
    one_current = _read("dbt/tests/customer_history_one_current.sql")
    assert "relationships:" not in history_contract
    assert "having countif(is_current) > 1" in one_current

    for model_name, next_model in (
        ("fact_customer_property_interaction", "fact_support_case"),
        ("fact_support_case", "fact_consent_change"),
    ):
        fact_contract = core_contracts.split(f"- name: {model_name}", maxsplit=1)[1].split(
            f"- name: {next_model}", maxsplit=1
        )[0]
        assert "relationships:" not in fact_contract
    consent_contract = core_contracts.split("- name: fact_consent_change", maxsplit=1)[1]
    assert "relationships:" not in consent_contract
