"""Read-only BigQuery repository for the private cloud API runtime."""

# Table references cannot be query parameters; every interpolated identifier is allowlisted.
# ruff: noqa: S608

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_DATASET_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class BigQueryCustomerRepository:
    """Execute parameterised, byte-capped reads against governed dbt relations."""

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        core_dataset: str,
        marts_dataset: str,
        governance_dataset: str,
        maximum_bytes_billed: int,
        client: Any | None = None,
    ) -> None:
        if not _PROJECT_ID.fullmatch(project_id):
            raise ValueError("invalid GCP project ID")
        if not location:
            raise ValueError("BigQuery location is required")
        for dataset in (core_dataset, marts_dataset, governance_dataset):
            if not _DATASET_ID.fullmatch(dataset):
                raise ValueError("invalid BigQuery dataset ID")
        if maximum_bytes_billed < 1:
            raise ValueError("maximum_bytes_billed must be positive")
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=project_id, location=location)
        self.client = client
        self.project_id = project_id
        self.location = location
        self.core_dataset = core_dataset
        self.marts_dataset = marts_dataset
        self.governance_dataset = governance_dataset
        self.maximum_bytes_billed = maximum_bytes_billed

    def _table(self, dataset: str, table: str) -> str:
        if not _DATASET_ID.fullmatch(table):
            raise ValueError("invalid BigQuery table ID")
        return f"`{self.project_id}.{dataset}.{table}`"

    def _query(
        self,
        sql: str,
        parameters: list[tuple[str, str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self.maximum_bytes_billed,
            use_query_cache=True,
            query_parameters=[
                bigquery.ScalarQueryParameter(name, parameter_type, value)
                for name, parameter_type, value in (parameters or [])
            ],
        )
        rows: Iterable[Any] = self.client.query(
            sql,
            job_config=config,
            location=self.location,
        ).result()
        return [dict(row.items()) for row in rows]

    def ready(self) -> bool:
        try:
            return bool(self._query("SELECT 1 AS ready")[0]["ready"] == 1)
        except Exception:  # Readiness must degrade on any client/query failure.
            return False

    def get_customer(self, customer_id: str) -> dict[str, Any] | None:
        customer_360 = self._table(self.marts_dataset, "customer_360_current")
        features = self._table(self.marts_dataset, "mart_personalisation_features")
        identity_bridge = self._table(self.core_dataset, "bridge_customer_source_identity")
        rows = self._query(
            f"""
            SELECT
              customer.customer_id,
              customer.full_name AS display_name,
              COALESCE(features.preferred_suburbs, CAST([] AS ARRAY<STRING>))
                AS preferred_suburbs,
              CASE
                WHEN features.observed_price_min IS NULL THEN NULL
                ELSE CONCAT(
                  CAST(features.observed_price_min AS STRING), '-',
                  CAST(features.observed_price_max AS STRING)
                )
              END AS observed_price_band,
              (
                SELECT ANY_VALUE(rule_version)
                FROM {identity_bridge} AS identity
                WHERE identity.customer_id = customer.customer_id
              ) AS identity_rule_version,
              customer.profile_as_of_at AS as_of
            FROM {customer_360} AS customer
            LEFT JOIN {features} AS features USING (customer_id)
            WHERE customer.customer_id = @customer_id
            LIMIT 1
            """,
            [("customer_id", "STRING", customer_id)],
        )
        return rows[0] if rows else None

    def list_identity_sources(self, customer_id: str) -> list[dict[str, Any]]:
        identity_bridge = self._table(self.core_dataset, "bridge_customer_source_identity")
        return self._query(
            f"""
            SELECT
              source_system,
              source_record_id,
              CAST(NULL AS STRING) AS normalized_email,
              CAST(NULL AS STRING) AS normalized_phone,
              highest_match_score AS match_score,
              resolution_decision AS match_decision,
              rule_version
            FROM {identity_bridge}
            WHERE customer_id = @customer_id
            ORDER BY source_system, source_record_id
            """,
            [("customer_id", "STRING", customer_id)],
        )

    def list_journey(self, customer_id: str, *, limit: int, offset: int) -> list[dict[str, Any]]:
        interactions = self._table(self.core_dataset, "fact_customer_property_interaction")
        return self._query(
            f"""
            SELECT
              interaction_id AS event_id,
              event_type,
              occurred_at,
              session_id,
              property_id,
              attribution_method AS link_reason,
              schema_version
            FROM {interactions}
            WHERE customer_id = @customer_id
            ORDER BY occurred_at DESC, interaction_id
            LIMIT @limit OFFSET @offset
            """,
            [
                ("customer_id", "STRING", customer_id),
                ("limit", "INT64", limit),
                ("offset", "INT64", offset),
            ],
        )

    def list_remediation(self, *, limit: int, offset: int) -> list[dict[str, Any]]:
        remediation = self._table(self.governance_dataset, "remediation_case")
        return self._query(
            f"""
            SELECT
              remediation_case_id AS case_id,
              source_system,
              source_record_id,
              issue_type,
              issue_code,
              severity,
              status,
              opened_at,
              updated_at
            FROM {remediation}
            ORDER BY opened_at DESC, remediation_case_id
            LIMIT @limit OFFSET @offset
            """,
            [("limit", "INT64", limit), ("offset", "INT64", offset)],
        )
