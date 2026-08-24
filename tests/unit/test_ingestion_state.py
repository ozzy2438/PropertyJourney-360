from datetime import UTC, datetime
from pathlib import Path

import pytest

from propertyjourney360.ingestion.landing import LocalRawLanding
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
