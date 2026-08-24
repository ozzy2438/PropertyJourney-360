"""Versioned RAW asset contracts shared by ingestion operators and warehouse bootstrap."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RawAssetContract:
    table_name: str
    fields: tuple[str, ...]
    schema_version: str = "1"

    def prepare(self, record: dict[str, Any], *, metadata: dict[str, Any]) -> dict[str, Any]:
        unknown = set(record) - set(self.fields)
        if unknown:
            raise ValueError(f"{self.table_name} record has unknown fields: {sorted(unknown)}")
        return {field: record.get(field) for field in self.fields} | metadata


METADATA_FIELDS = ("_ingested_at", "_window_id", "_source_run_id", "_schema_version")


RAW_CONTRACTS: dict[str, RawAssetContract] = {
    "raw_salesforce_accounts": RawAssetContract(
        "raw_salesforce_accounts",
        (
            "id",
            "name",
            "abn",
            "industry",
            "billing_street",
            "billing_city",
            "billing_state",
            "billing_postal_code",
            "system_modstamp",
        ),
    ),
    "raw_salesforce_contacts": RawAssetContract(
        "raw_salesforce_contacts",
        (
            "id",
            "account_id",
            "first_name",
            "last_name",
            "email",
            "email_verified",
            "mobile_phone",
            "mailing_street",
            "mailing_city",
            "mailing_state",
            "mailing_postal_code",
            "system_modstamp",
        ),
    ),
    "raw_salesforce_cases": RawAssetContract(
        "raw_salesforce_cases",
        (
            "id",
            "contact_id",
            "account_id",
            "status",
            "priority",
            "subject",
            "created_date",
            "closed_date",
            "system_modstamp",
        ),
    ),
    "raw_legacy_customers": RawAssetContract(
        "raw_legacy_customers",
        (
            "customer_id",
            "account_id",
            "first_name",
            "last_name",
            "email",
            "email_verified",
            "phone",
            "address_line_1",
            "suburb",
            "state",
            "postcode",
            "updated_at",
        ),
    ),
    "raw_support_cases": RawAssetContract(
        "raw_support_cases",
        (
            "case_id",
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_email_verified",
            "customer_phone",
            "status",
            "priority",
            "subject",
            "opened_at",
            "closed_at",
            "updated_at",
        ),
    ),
    "raw_marketing_consents": RawAssetContract(
        "raw_marketing_consents",
        (
            "consent_event_id",
            "subject_source_system",
            "subject_source_id",
            "channel",
            "consent_status",
            "policy_version",
            "capture_source",
            "occurred_at",
        ),
    ),
    "raw_property_events": RawAssetContract(
        "raw_property_events",
        (
            "event_id",
            "event_type",
            "session_id",
            "anonymous_id",
            "customer_id",
            "customer_source_system",
            "customer_source_id",
            "property_id",
            "price_observed",
            "device_type",
            "attributes",
            "schema_version",
            "occurred_at",
            "received_at",
        ),
    ),
    "raw_property_master": RawAssetContract(
        "raw_property_master",
        (
            "property_id",
            "address_id",
            "address_line_1",
            "suburb",
            "state",
            "postcode",
            "property_type",
            "latitude",
            "longitude",
            "geometry_wkt",
            "source_dataset",
            "source_updated_at",
            "source_retrieved_at",
            "source_payload_sha256",
            "source_payload_object",
        ),
    ),
    "raw_property_market": RawAssetContract(
        "raw_property_market",
        (
            "geography_code",
            "geography_type",
            "period_start",
            "property_type",
            "median_price",
            "sales_volume",
            "source_dataset",
            "source_retrieved_at",
            "source_payload_sha256",
            "source_payload_object",
        ),
    ),
    "raw_abs_demographics": RawAssetContract(
        "raw_abs_demographics",
        (
            "geography_code",
            "geography_type",
            "reference_period",
            "population",
            "median_household_income_weekly",
            "owner_occupied_dwellings",
            "rented_dwellings",
            "source_dataset",
            "source_retrieved_at",
            "source_payload_sha256",
            "source_payload_object",
        ),
    ),
    "raw_rba_indicators": RawAssetContract(
        "raw_rba_indicators",
        (
            "series_id",
            "series_name",
            "observation_date",
            "observation_value",
            "unit",
            "source_retrieved_at",
            "source_payload_sha256",
            "source_payload_object",
        ),
    ),
}


SOURCE_ASSETS: dict[str, tuple[str, ...]] = {
    "salesforce": (
        "raw_salesforce_accounts",
        "raw_salesforce_contacts",
        "raw_salesforce_cases",
    ),
    "legacy_crm": ("raw_legacy_customers",),
    "support": ("raw_support_cases",),
    "consent": ("raw_marketing_consents",),
    "property": ("raw_property_master", "raw_property_market"),
    "economic": ("raw_abs_demographics", "raw_rba_indicators"),
}
