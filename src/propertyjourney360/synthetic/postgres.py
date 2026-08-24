"""Idempotent seeding of local PostgreSQL operational-source simulations."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.domain.models import IdentityRecord, SourceSystem
from propertyjourney360.synthetic.identities import (
    SyntheticIdentityConfig,
    generate_identity_dataset,
)


class OperationalSeedRows(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    legacy_customers: tuple[dict[str, Any], ...]
    support_customers: tuple[dict[str, Any], ...]
    support_cases: tuple[dict[str, Any], ...]
    consent_events: tuple[dict[str, Any], ...]


class PostgresSeedReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    seed: int
    requested_customers: int = Field(ge=1)
    legacy_customer_rows: int = Field(ge=0)
    support_customer_rows: int = Field(ge=0)
    support_case_rows: int = Field(ge=0)
    consent_event_rows: int = Field(ge=0)
    orphan_case_rows: int = Field(ge=0)
    conflicting_consent_customers: int = Field(ge=0)
    evidence_scope: str


def _identity_row(record: IdentityRecord, *, run_id: str) -> dict[str, Any]:
    return {
        "source_record_id": record.source_record_id,
        "source_updated_at": record.source_updated_at,
        "email": record.email,
        "phone": record.phone,
        "first_name": record.first_name,
        "last_name": record.last_name,
        "address_line": record.address_line,
        "suburb": record.suburb,
        "postcode": record.postcode,
        "source_payload": json.dumps(record.attributes, sort_keys=True),
        "seed_run_id": run_id,
    }


def build_operational_seed_rows(*, seed: int, customer_count: int) -> OperationalSeedRows:
    """Build source rows without copying evaluation-only truth IDs into operational tables."""
    dataset = generate_identity_dataset(
        SyntheticIdentityConfig(seed=seed, customer_count=customer_count)
    )
    run_id = f"operational-{seed}-{customer_count}"
    legacy = tuple(
        _identity_row(record, run_id=run_id)
        for record in dataset.records
        if record.source_system is SourceSystem.LEGACY_CRM
    )
    support = tuple(
        _identity_row(record, run_id=run_id)
        for record in dataset.records
        if record.source_system is SourceSystem.SUPPORT
    )
    cases: list[dict[str, Any]] = []
    consents: list[dict[str, Any]] = []
    for index, record in enumerate(support):
        updated_at = record["source_updated_at"]
        cases.append(
            {
                "case_id": f"SUP-{index + 1:08d}",
                "customer_source_record_id": record["source_record_id"],
                "opened_at": updated_at - timedelta(days=index % 5),
                "status": ("CLOSED" if index % 3 == 0 else "IN_PROGRESS"),
                "subject": "Synthetic property journey support request",
                "description": "Synthetic-only case used to exercise support ingestion.",
                "source_updated_at": updated_at,
                "seed_run_id": run_id,
            }
        )
        legacy_record = legacy[index]
        effective_at = legacy_record["source_updated_at"] - timedelta(days=7)
        consents.append(
            {
                "consent_event_id": f"CONSENT-{index + 1:08d}-A",
                "customer_source_record_id": legacy_record["source_record_id"],
                "channel": "EMAIL",
                "consent_status": "GRANTED",
                "effective_at": effective_at,
                "source_updated_at": effective_at,
                "seed_run_id": run_id,
            }
        )
        if index % 9 == 0:
            consents.append(
                {
                    "consent_event_id": f"CONSENT-{index + 1:08d}-B",
                    "customer_source_record_id": legacy_record["source_record_id"],
                    "channel": "EMAIL",
                    "consent_status": "WITHDRAWN",
                    "effective_at": effective_at,
                    "source_updated_at": effective_at + timedelta(minutes=1),
                    "seed_run_id": run_id,
                }
            )
    cases.append(
        {
            "case_id": "SUP-ORPHAN-0001",
            "customer_source_record_id": "MISSING-SUPPORT-CUSTOMER",
            "opened_at": datetime(2026, 1, 2, tzinfo=UTC),
            "status": "NEW",
            "subject": "Synthetic orphan relationship",
            "description": "Intentional DQ defect; no customer row exists.",
            "source_updated_at": datetime(2026, 1, 2, tzinfo=UTC),
            "seed_run_id": run_id,
        }
    )
    return OperationalSeedRows(
        legacy_customers=legacy,
        support_customers=support,
        support_cases=tuple(cases),
        consent_events=tuple(consents),
    )


def seed_operational_postgres(
    *, dsn: str, seed: int = 20260824, customer_count: int = 100
) -> PostgresSeedReport:
    """Upsert deterministic source rows in one transaction and report observed table counts."""
    import psycopg

    rows = build_operational_seed_rows(seed=seed, customer_count=customer_count)
    run_id = f"operational-{seed}-{customer_count}"
    identity_sql = """
        INSERT INTO {table} (
            source_record_id, source_updated_at, email, phone, first_name, last_name,
            address_line, suburb, postcode, source_payload, seed_run_id
        ) VALUES (
            %(source_record_id)s, %(source_updated_at)s, %(email)s, %(phone)s, %(first_name)s,
            %(last_name)s, %(address_line)s, %(suburb)s, %(postcode)s,
            %(source_payload)s::jsonb, %(seed_run_id)s
        )
        ON CONFLICT (source_record_id) DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            address_line = EXCLUDED.address_line,
            suburb = EXCLUDED.suburb,
            postcode = EXCLUDED.postcode,
            source_payload = EXCLUDED.source_payload,
            seed_run_id = EXCLUDED.seed_run_id
    """
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.executemany(identity_sql.format(table="legacy.customer"), rows.legacy_customers)
        cursor.executemany(identity_sql.format(table="support.customer"), rows.support_customers)
        cursor.executemany(
            """
            INSERT INTO support.case_record (
                case_id, customer_source_record_id, opened_at, status, subject, description,
                source_updated_at, seed_run_id
            ) VALUES (
                %(case_id)s, %(customer_source_record_id)s, %(opened_at)s, %(status)s,
                %(subject)s, %(description)s, %(source_updated_at)s, %(seed_run_id)s
            )
            ON CONFLICT (case_id) DO UPDATE SET
                customer_source_record_id = EXCLUDED.customer_source_record_id,
                status = EXCLUDED.status,
                description = EXCLUDED.description,
                source_updated_at = EXCLUDED.source_updated_at,
                seed_run_id = EXCLUDED.seed_run_id
            """,
            rows.support_cases,
        )
        cursor.executemany(
            """
            INSERT INTO marketing.consent_history (
                consent_event_id, customer_source_record_id, channel, consent_status,
                effective_at, source_updated_at, seed_run_id
            ) VALUES (
                %(consent_event_id)s, %(customer_source_record_id)s, %(channel)s,
                %(consent_status)s, %(effective_at)s, %(source_updated_at)s, %(seed_run_id)s
            )
            ON CONFLICT (consent_event_id) DO UPDATE SET
                consent_status = EXCLUDED.consent_status,
                effective_at = EXCLUDED.effective_at,
                source_updated_at = EXCLUDED.source_updated_at,
                seed_run_id = EXCLUDED.seed_run_id
            """,
            rows.consent_events,
        )
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM legacy.customer WHERE seed_run_id = %(run_id)s),
              (SELECT count(*) FROM support.customer WHERE seed_run_id = %(run_id)s),
              (SELECT count(*) FROM support.case_record WHERE seed_run_id = %(run_id)s),
              (SELECT count(*) FROM marketing.consent_history WHERE seed_run_id = %(run_id)s)
            """,
            {"run_id": run_id},
        )
        observed = cursor.fetchone()
    if observed is None:
        raise RuntimeError("PostgreSQL seed reconciliation query returned no row")
    return PostgresSeedReport(
        run_id=run_id,
        seed=seed,
        requested_customers=customer_count,
        legacy_customer_rows=observed[0],
        support_customer_rows=observed[1],
        support_case_rows=observed[2],
        consent_event_rows=observed[3],
        orphan_case_rows=1,
        conflicting_consent_customers=(customer_count + 8) // 9,
        evidence_scope="Local PostgreSQL operational-source simulation; synthetic identities only.",
    )
