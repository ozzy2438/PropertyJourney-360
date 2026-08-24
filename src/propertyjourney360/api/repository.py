"""SQLite repository used by the local vertical slice and API contract tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol


class CustomerReadRepository(Protocol):
    """Read contract shared by the local SQLite and cloud BigQuery adapters."""

    def ready(self) -> bool: ...

    def get_customer(self, customer_id: str) -> dict[str, Any] | None: ...

    def list_identity_sources(self, customer_id: str) -> list[dict[str, Any]]: ...

    def list_journey(
        self, customer_id: str, *, limit: int, offset: int
    ) -> list[dict[str, Any]]: ...

    def list_remediation(self, *, limit: int, offset: int) -> list[dict[str, Any]]: ...


class CustomerRepository:
    """Small persistence adapter; production deployments substitute BigQuery repositories."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection, connection:
            connection.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS customer (
                    customer_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    preferred_suburbs TEXT NOT NULL DEFAULT '[]',
                    observed_price_band TEXT,
                    identity_rule_version TEXT NOT NULL,
                    as_of TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS customer_identity_source (
                    customer_id TEXT NOT NULL REFERENCES customer(customer_id),
                    source_system TEXT NOT NULL,
                    source_record_id TEXT NOT NULL,
                    normalized_email TEXT,
                    normalized_phone TEXT,
                    match_score REAL,
                    match_decision TEXT NOT NULL,
                    PRIMARY KEY (source_system, source_record_id)
                );
                CREATE TABLE IF NOT EXISTS property_event (
                    event_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    anonymous_id TEXT,
                    customer_id TEXT,
                    customer_source_system TEXT,
                    customer_source_id TEXT,
                    property_id TEXT,
                    price_observed REAL,
                    device_type TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_identity_link (
                    event_id TEXT PRIMARY KEY REFERENCES property_event(event_id),
                    customer_id TEXT NOT NULL,
                    link_reason TEXT NOT NULL,
                    linked_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS session_identity (
                    session_id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    identified_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS remediation_case (
                    case_id TEXT PRIMARY KEY,
                    source_system TEXT NOT NULL,
                    source_record_id TEXT NOT NULL,
                    issue_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """
            )
            existing_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(property_event)").fetchall()
            }
            migration_columns = {
                "customer_source_system": "TEXT",
                "customer_source_id": "TEXT",
                "price_observed": "REAL",
            }
            for column, data_type in migration_columns.items():
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE property_event ADD COLUMN {column} {data_type}"
                    )

    def ready(self) -> bool:
        try:
            with self.connect() as connection:
                return bool(connection.execute("SELECT 1").fetchone()[0] == 1)
        except sqlite3.Error:
            return False

    def upsert_customer(self, record: Mapping[str, Any]) -> None:
        with self.connect() as connection, connection:
            connection.execute(
                """
                INSERT INTO customer (
                    customer_id, display_name, preferred_suburbs, observed_price_band,
                    identity_rule_version, as_of
                ) VALUES (:customer_id, :display_name, :preferred_suburbs, :observed_price_band,
                          :identity_rule_version, :as_of)
                ON CONFLICT(customer_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    preferred_suburbs = excluded.preferred_suburbs,
                    observed_price_band = excluded.observed_price_band,
                    identity_rule_version = excluded.identity_rule_version,
                    as_of = excluded.as_of
                """,
                record,
            )

    def add_identity_source(self, record: Mapping[str, Any]) -> None:
        with self.connect() as connection, connection:
            connection.execute(
                """
                INSERT INTO customer_identity_source (
                    customer_id, source_system, source_record_id, normalized_email,
                    normalized_phone, match_score, match_decision
                ) VALUES (:customer_id, :source_system, :source_record_id, :normalized_email,
                          :normalized_phone, :match_score, :match_decision)
                ON CONFLICT(source_system, source_record_id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    normalized_email = excluded.normalized_email,
                    normalized_phone = excluded.normalized_phone,
                    match_score = excluded.match_score,
                    match_decision = excluded.match_decision
                """,
                record,
            )

    def get_customer(self, customer_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM customer WHERE customer_id = ?",
                (customer_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_identity_sources(self, customer_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT source_system, source_record_id, normalized_email, normalized_phone,
                       match_score, match_decision
                FROM customer_identity_source
                WHERE customer_id = ? ORDER BY source_system, source_record_id
                """,
                (customer_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_journey(self, customer_id: str, *, limit: int, offset: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT e.event_id, e.event_type, e.occurred_at, e.received_at, e.session_id,
                       e.property_id, e.price_observed, e.device_type, l.link_reason
                FROM property_event AS e
                JOIN event_identity_link AS l ON l.event_id = e.event_id
                WHERE l.customer_id = ?
                ORDER BY e.occurred_at DESC, e.event_id
                LIMIT ? OFFSET ?
                """,
                (customer_id, limit, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_remediation(self, *, limit: int, offset: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT case_id, source_system, source_record_id, issue_type, status, opened_at
                FROM remediation_case
                ORDER BY opened_at DESC, case_id
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_remediation(self, record: Mapping[str, Any]) -> None:
        with self.connect() as connection, connection:
            connection.execute(
                """
                INSERT INTO remediation_case (
                    case_id, source_system, source_record_id, issue_type, status, opened_at,
                    payload_json
                ) VALUES (:case_id, :source_system, :source_record_id, :issue_type, :status,
                          :opened_at, :payload_json)
                ON CONFLICT(case_id) DO UPDATE SET
                    status = excluded.status,
                    payload_json = excluded.payload_json
                """,
                record,
            )

    def insert_event(
        self, record: Mapping[str, Any]
    ) -> Literal["INSERTED", "DUPLICATE", "COLLISION"]:
        with self.connect() as connection, connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO property_event (
                    event_id, schema_version, event_type, occurred_at, received_at, session_id,
                    anonymous_id, customer_id, customer_source_system, customer_source_id,
                    property_id, price_observed, device_type, payload_json
                ) VALUES (:event_id, :schema_version, :event_type, :occurred_at, :received_at,
                          :session_id, :anonymous_id, :customer_id, :customer_source_system,
                          :customer_source_id, :property_id, :price_observed, :device_type,
                          :payload_json)
                """,
                record,
            )
            if cursor.rowcount == 1:
                return "INSERTED"
            existing = connection.execute(
                """
                SELECT event_id, schema_version, event_type, occurred_at, received_at, session_id,
                       anonymous_id, customer_id, customer_source_system, customer_source_id,
                       property_id, price_observed, device_type, payload_json
                FROM property_event
                WHERE event_id = ?
                """,
                (record["event_id"],),
            ).fetchone()
            if existing is None:
                raise RuntimeError("event insert conflict did not resolve to a durable row")
            expected = {key: record.get(key) for key in dict(existing)}
            return "DUPLICATE" if dict(existing) == expected else "COLLISION"

    def session_customer(self, session_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT customer_id FROM session_identity WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return str(row["customer_id"]) if row else None

    def customer_for_source(self, *, source_system: str, source_record_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT customer_id
                FROM customer_identity_source
                WHERE source_system = ? AND source_record_id = ?
                """,
                (source_system, source_record_id),
            ).fetchone()
        return str(row["customer_id"]) if row else None

    def identify_session(
        self,
        *,
        session_id: str,
        customer_id: str,
        identified_at: datetime,
    ) -> None:
        with self.connect() as connection, connection:
            connection.execute(
                """
                INSERT INTO session_identity (session_id, customer_id, identified_at)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    identified_at = excluded.identified_at
                """,
                (session_id, customer_id, identified_at.astimezone(UTC).isoformat()),
            )

    def link_event_identity(
        self,
        *,
        event_id: str,
        customer_id: str,
        link_reason: str,
        linked_at: datetime,
    ) -> int:
        with self.connect() as connection, connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO event_identity_link (
                    event_id, customer_id, link_reason, linked_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    event_id,
                    customer_id,
                    link_reason,
                    linked_at.astimezone(UTC).isoformat(),
                ),
            )
            return cursor.rowcount

    def link_session_events(
        self,
        *,
        session_id: str,
        customer_id: str,
        occurred_after: datetime,
        occurred_before: datetime,
        linked_at: datetime,
    ) -> int:
        with self.connect() as connection, connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO event_identity_link (
                    event_id, customer_id, link_reason, linked_at
                )
                SELECT event_id, ?, 'EXPLICIT_SESSION_IDENTIFY', ?
                FROM property_event
                WHERE session_id = ? AND occurred_at >= ? AND occurred_at <= ?
                """,
                (
                    customer_id,
                    linked_at.astimezone(UTC).isoformat(),
                    session_id,
                    occurred_after.astimezone(UTC).isoformat(),
                    occurred_before.astimezone(UTC).isoformat(),
                ),
            )
            return cursor.rowcount
