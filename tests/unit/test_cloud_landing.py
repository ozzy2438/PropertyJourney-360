from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from google.api_core.exceptions import PreconditionFailed

from propertyjourney360.ingestion.bigquery import BigQueryJsonLoader
from propertyjourney360.ingestion.gcs import GCSRawLanding
from propertyjourney360.ingestion.landing import LandingManifest


class FakeBlob:
    def __init__(self, name: str, objects: dict[str, dict[str, Any]]) -> None:
        self.name = name
        self.objects = objects
        self.metadata: dict[str, str] | None = None
        self.size: int | None = None

    def upload_from_string(self, payload: bytes, **kwargs: Any) -> None:
        assert kwargs["if_generation_match"] == 0
        assert kwargs["checksum"] == "crc32c"
        if self.name in self.objects:
            raise PreconditionFailed("already exists")
        self.objects[self.name] = {
            "payload": payload,
            "metadata": dict(self.metadata or {}),
        }

    def reload(self) -> None:
        stored = self.objects[self.name]
        self.metadata = stored["metadata"]
        self.size = len(stored["payload"])

    def download_as_bytes(self) -> bytes:
        return self.objects[self.name]["payload"]


class FakeBucket:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(name, self.objects)


class FakeStorageClient:
    def __init__(self) -> None:
        self.value = FakeBucket()

    def bucket(self, name: str) -> FakeBucket:
        assert name == "pj360-raw-test"
        return self.value


def test_gcs_landing_is_create_only_and_replay_safe() -> None:
    client = FakeStorageClient()
    landing = GCSRawLanding(
        bucket_name="pj360-raw-test",
        client=client,
        now=lambda: datetime(2026, 8, 24, 2, tzinfo=UTC),
    )
    arguments = {
        "source": "salesforce_contacts",
        "run_id": "run-1",
        "schema_version": "contacts-v1",
        "records": [{"Id": "003-example", "_ingested_at": "2026-08-24T01:00:00Z"}],
        "extracted_at": datetime(2026, 8, 24, 1, tzinfo=UTC),
        "watermark": "2026-08-24T01:00:00Z",
    }

    first = landing.land(**arguments)  # type: ignore[arg-type]
    second = landing.land(**arguments)  # type: ignore[arg-type]

    assert first == second
    assert first.object_path.startswith("gs://pj360-raw-test/")
    assert len(client.value.objects) == 2


class FakeLoadJob:
    def __init__(self) -> None:
        self.errors: list[dict[str, str]] = []
        self.output_rows = 2

    def result(self) -> None:
        return None


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def load_table_from_uri(self, uri: str, table: str, **kwargs: Any) -> FakeLoadJob:
        self.calls.append({"uri": uri, "table": table, **kwargs})
        return FakeLoadJob()


def test_bigquery_loader_uses_deterministic_job_id() -> None:
    manifest = LandingManifest(
        source="events",
        run_id="run-1",
        schema_version="event-v2",
        extracted_at=datetime(2026, 8, 24, 1, tzinfo=UTC),
        landed_at=datetime(2026, 8, 24, 2, tzinfo=UTC),
        watermark=None,
        record_count=2,
        sha256="a" * 64,
        object_path="gs://pj360-raw-test/events/a.jsonl",
    )
    client = FakeBigQueryClient()
    loader = BigQueryJsonLoader(client=client, location="australia-southeast1")

    first = loader.load(manifest, table="valid-project.pj360_raw.events")
    second = loader.load(manifest, table="valid-project.pj360_raw.events")

    assert first.job_id == second.job_id
    assert first.output_rows == 2
    assert client.calls[0]["job_config"].max_bad_records == 0


def test_bigquery_loader_rejects_local_or_malformed_targets() -> None:
    manifest = LandingManifest(
        source="events",
        run_id="run-1",
        schema_version="event-v2",
        extracted_at=datetime(2026, 8, 24, 1, tzinfo=UTC),
        landed_at=datetime(2026, 8, 24, 2, tzinfo=UTC),
        watermark=None,
        record_count=0,
        sha256="b" * 64,
        object_path="events/b.jsonl",
    )
    loader = BigQueryJsonLoader(client=FakeBigQueryClient(), location="australia-southeast1")
    with pytest.raises(ValueError, match="gs://"):
        loader.load(manifest, table="valid-project.dataset.table")
