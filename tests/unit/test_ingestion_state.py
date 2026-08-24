import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from propertyjourney360.ingestion.landing import LandingManifest, LocalRawLanding
from propertyjourney360.ingestion.watermarks import WatermarkStore


def test_content_addressed_landing_is_idempotent(tmp_path: Path) -> None:
    landing = LocalRawLanding(tmp_path)
    extracted_at = datetime(2026, 8, 24, tzinfo=UTC)
    records = [{"id": "2", "value": "b"}, {"id": "1", "value": "a"}]

    first = landing.land(
        source="legacy_customers",
        run_id="run-1",
        schema_version="1",
        records=records,
        extracted_at=extracted_at,
    )
    second = landing.land(
        source="legacy_customers",
        run_id="run-1",
        schema_version="1",
        records=records,
        extracted_at=extracted_at,
    )

    assert first.sha256 == second.sha256
    assert first.object_path == second.object_path
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1


def test_local_landing_rejects_existing_manifest_with_conflicting_envelope(
    tmp_path: Path,
) -> None:
    landing = LocalRawLanding(tmp_path)
    arguments = {
        "source": "legacy_customers",
        "run_id": "run-1",
        "schema_version": "1",
        "records": [{"id": "1"}],
        "extracted_at": datetime(2026, 8, 24, tzinfo=UTC),
    }
    landing.land(**arguments)  # type: ignore[arg-type]
    manifest_path = next(tmp_path.rglob("*.manifest.json"))
    manifest = LandingManifest.model_validate_json(manifest_path.read_text())
    manifest_path.write_text(
        manifest.model_copy(update={"watermark": "forged"}).model_dump_json(indent=2)
    )

    with pytest.raises(RuntimeError, match="watermark"):
        landing.land(**arguments)  # type: ignore[arg-type]


def test_watermark_store_rejects_regression(tmp_path: Path) -> None:
    store = WatermarkStore(tmp_path / "state.db")
    store.advance(source="salesforce", stream="Contact", value="2026-08-24T01:00:00Z", run_id="1")

    with pytest.raises(ValueError, match="regression"):
        store.advance(
            source="salesforce",
            stream="Contact",
            value="2026-08-23T23:00:00Z",
            run_id="2",
        )

    assert store.get(source="salesforce", stream="Contact").run_id == "1"  # type: ignore[union-attr]


def test_watermark_multi_asset_advance_rolls_back_and_retries_atomically(tmp_path: Path) -> None:
    database = tmp_path / "state.db"
    store = WatermarkStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_second_asset
            BEFORE INSERT ON ingestion_watermark
            WHEN NEW.stream = 'raw_property_market'
            BEGIN
                SELECT RAISE(ABORT, 'simulated interruption');
            END
            """
        )
    updates = (
        ("property", "raw_property_master", "2026-08-24T00:00:00+00:00", "run-1"),
        ("property", "raw_property_market", "2026-08-24T00:00:00+00:00", "run-1"),
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated interruption"):
        store.advance_many(updates)
    assert store.get(source="property", stream="raw_property_master") is None
    assert store.get(source="property", stream="raw_property_market") is None

    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER fail_second_asset")
    committed = store.advance_many(updates)
    assert {watermark.stream for watermark in committed} == {
        "raw_property_master",
        "raw_property_market",
    }
