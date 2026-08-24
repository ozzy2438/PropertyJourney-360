from __future__ import annotations

from typing import Any

import pytest

from propertyjourney360.api.bigquery_repository import BigQueryCustomerRepository


class FakeRow(dict[str, Any]):
    pass


class FakeQueryJob:
    def __init__(self, rows: list[FakeRow]) -> None:
        self.rows = rows

    def result(self) -> list[FakeRow]:
        return self.rows


class FakeBigQueryClient:
    def __init__(self, responses: list[list[FakeRow] | Exception]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def query(self, sql: str, *, job_config: Any, location: str) -> FakeQueryJob:
        self.calls.append({"sql": sql, "job_config": job_config, "location": location})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return FakeQueryJob(response)


def _repository(client: FakeBigQueryClient) -> BigQueryCustomerRepository:
    return BigQueryCustomerRepository(
        project_id="pj360-test-project",
        location="australia-southeast1",
        core_dataset="pj360_core_dev",
        marts_dataset="pj360_marts_dev",
        governance_dataset="pj360_governance_dev",
        maximum_bytes_billed=1_000_000,
        client=client,
    )


def test_customer_query_is_parameterised_and_byte_capped() -> None:
    client = FakeBigQueryClient(
        [
            [
                FakeRow(
                    customer_id="customer-1",
                    display_name="Synthetic Customer",
                    preferred_suburbs=["Carlton"],
                    observed_price_band="700000-900000",
                    identity_rule_version="identity-v1.1.0",
                    as_of="2026-08-24T00:00:00Z",
                )
            ]
        ]
    )
    result = _repository(client).get_customer("customer-1")

    assert result is not None
    assert result["preferred_suburbs"] == ["Carlton"]
    call = client.calls[0]
    assert "@customer_id" in call["sql"]
    assert "customer-1" not in call["sql"]
    assert call["job_config"].maximum_bytes_billed == 1_000_000
    assert call["job_config"].query_parameters[0].value == "customer-1"


def test_identity_reads_do_not_expose_raw_contact_fields() -> None:
    client = FakeBigQueryClient(
        [
            [
                FakeRow(
                    source_system="salesforce",
                    source_record_id="003-test",
                    normalized_email=None,
                    normalized_phone=None,
                    match_score=0.99,
                    match_decision="AUTO_MATCH",
                    rule_version="identity-v1.1.0",
                )
            ]
        ]
    )
    row = _repository(client).list_identity_sources("customer-1")[0]
    assert row["normalized_email"] is None
    assert row["normalized_phone"] is None


def test_repository_rejects_identifier_injection() -> None:
    with pytest.raises(ValueError, match="dataset"):
        BigQueryCustomerRepository(
            project_id="pj360-test-project",
            location="australia-southeast1",
            core_dataset="core`; DROP TABLE x; --",
            marts_dataset="pj360_marts_dev",
            governance_dataset="pj360_governance_dev",
            maximum_bytes_billed=1,
            client=FakeBigQueryClient([]),
        )


def test_readiness_probes_every_relation_used_by_api() -> None:
    client = FakeBigQueryClient([[], [], [], [], []])
    assert _repository(client).ready()

    sql = "\n".join(call["sql"] for call in client.calls)
    for relation in (
        "customer_360_current",
        "mart_personalisation_features",
        "bridge_customer_source_identity",
        "fact_customer_property_interaction",
        "remediation_case",
    ):
        assert relation in sql
    assert len(client.calls) == 5
    assert all("LIMIT 0" in call["sql"] for call in client.calls)


def test_readiness_fails_when_relation_is_not_readable() -> None:
    client = FakeBigQueryClient([[], PermissionError("table denied")])
    assert not _repository(client).ready()
    assert len(client.calls) == 2
