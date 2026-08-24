"""Transactional SQLite watermark state for incremental extractors."""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict


class Watermark(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    stream: str
    value: str
    updated_at: datetime
    run_id: str


class WatermarkStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_watermark (
                    source TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    PRIMARY KEY (source, stream)
                )
                """
            )

    def get(self, *, source: str, stream: str) -> Watermark | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT source, stream, value, updated_at, run_id "
                "FROM ingestion_watermark WHERE source = ? AND stream = ?",
                (source, stream),
            ).fetchone()
        return Watermark.model_validate(dict(row)) if row else None

    def advance(self, *, source: str, stream: str, value: str, run_id: str) -> Watermark:
        """Advance lexicographically sortable watermarks; reject regressions."""

        return self.advance_many(((source, stream, value, run_id),))[0]

    def advance_many(self, updates: Sequence[tuple[str, str, str, str]]) -> tuple[Watermark, ...]:
        """Atomically advance a complete source/asset set or leave every row unchanged."""

        now = datetime.now(UTC)
        with closing(self._connect()) as connection, connection:
            for source, stream, value, _run_id in updates:
                row = connection.execute(
                    "SELECT value FROM ingestion_watermark WHERE source = ? AND stream = ?",
                    (source, stream),
                ).fetchone()
                if row and value < row["value"]:
                    raise ValueError(f"watermark regression for {source}.{stream}")
            for source, stream, value, run_id in updates:
                connection.execute(
                    """
                    INSERT INTO ingestion_watermark (source, stream, value, updated_at, run_id)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(source, stream) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at,
                        run_id = excluded.run_id
                    """,
                    (source, stream, value, now.isoformat(), run_id),
                )
        return tuple(
            Watermark(
                source=source,
                stream=stream,
                value=value,
                updated_at=now,
                run_id=run_id,
            )
            for source, stream, value, run_id in updates
        )
