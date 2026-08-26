from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from google.api_core.exceptions import Conflict, PreconditionFailed

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
            raise PreconditionFailed("already exists")  # type: ignore[no-untyped-call]
        self.objects[self.name] = {
            "payload": payload,
            "metadata": dict(self.metadata or {}),
        }

    def reload(self) -> None:
        stored = self.objects[self.name]
        self.metadata = stored["metadata"]
        self.size = len(stored["payload"])

    def download_as_bytes(self) -> bytes:
        return cast(bytes, self.objects[self.name]["payload"])


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


def test_gcs_landing_rejects_existing_manifest_with_conflicting_envelope() -> None:
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
        "records": [{"Id": "003-example"}],
        "extracted_at": datetime(2026, 8, 24, 1, tzinfo=UTC),
        "watermark": "2026-08-24T01:00:00Z",
    }
    first = landing.land(**arguments)  # type: ignore[arg-type]
    manifest_name = next(name for name in client.value.objects if name.endswith(".manifest.json"))
    client.value.objects[manifest_name]["payload"] = (
        first.model_copy(update={"schema_version": "forged-v2"}).model_dump_json(indent=2).encode()
    )

    with pytest.raises(RuntimeError, match="schema_version"):
        landing.land(**arguments)  # type: ignore[arg-type]


class FakeLoadJob:
    def __init__(
        self,
        *,
        uri: str,
        table: str,
        job_id: str,
        job_config: Any,
        location: str,
        interrupt_once: bool = False,
    ) -> None:
        self.errors: list[dict[str, str]] = []
        self.output_rows = 2
        self.source_uris = [uri]
        self.destination = table
        self.job_id = job_id
        self.project = table.split(".", maxsplit=1)[0]
        self.location = location
        self.source_format = job_config.source_format
        self.write_disposition = job_config.write_disposition
        self.ignore_unknown_values = job_config.ignore_unknown_values
        self.max_bad_records = job_config.max_bad_records
        self.schema_update_options = job_config.schema_update_options
        self.interrupt_once = interrupt_once
        self.result_calls = 0

    def result(self) -> None:
        self.result_calls += 1
        if self.interrupt_once:
            self.interrupt_once = False
            raise ConnectionError("client disconnected after job submission")
        return None


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def load_table_from_uri(self, uri: str, table: str, **kwargs: Any) -> FakeLoadJob:
        self.calls.append({"uri": uri, "table": table, **kwargs})
        return FakeLoadJob(
            uri=uri,
            table=table,
            job_id=kwargs["job_id"],
            job_config=kwargs["job_config"],
            location=kwargs["location"],
        )


class RecoveringBigQueryClient(FakeBigQueryClient):
    def __init__(self) -> None:
        super().__init__()
        self.job: FakeLoadJob | None = None

    def load_table_from_uri(self, uri: str, table: str, **kwargs: Any) -> FakeLoadJob:
        self.calls.append({"uri": uri, "table": table, **kwargs})
        if self.job is not None:
            raise Conflict("job already exists")  # type: ignore[no-untyped-call]
        self.job = FakeLoadJob(
            uri=uri,
            table=table,
            job_id=kwargs["job_id"],
            job_config=kwargs["job_config"],
            location=kwargs["location"],
            interrupt_once=True,
        )
        return self.job

    def get_job(self, job_id: str, **kwargs: Any) -> FakeLoadJob:
        self.calls.append({"get_job": job_id, **kwargs})
        assert self.job is not None
        return self.job


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


def test_bigquery_loader_recovers_conflicting_job_after_interrupted_submission() -> None:
    manifest = LandingManifest(
        source="events",
        run_id="run-1",
        schema_version="event-v2",
        extracted_at=datetime(2026, 8, 24, 1, tzinfo=UTC),
        landed_at=datetime(2026, 8, 24, 2, tzinfo=UTC),
        watermark=None,
        record_count=2,
        sha256="c" * 64,
        object_path="gs://pj360-raw-test/events/c.jsonl",
    )
    client = RecoveringBigQueryClient()
    loader = BigQueryJsonLoader(client=client, location="australia-southeast1")

    with pytest.raises(ConnectionError, match="disconnected"):
        loader.load(manifest, table="valid-project.pj360_raw.events")
    recovered = loader.load(manifest, table="valid-project.pj360_raw.events")

    assert recovered.output_rows == manifest.record_count
    assert client.job is not None
    assert client.job.result_calls == 2
    assert client.calls[-1] == {
        "get_job": recovered.job_id,
        "project": "valid-project",
        "location": "australia-southeast1",
    }


def test_bigquery_loader_rejects_mismatched_recovered_job() -> None:
    manifest = LandingManifest(
        source="events",
        run_id="run-1",
        schema_version="event-v2",
        extracted_at=datetime(2026, 8, 24, 1, tzinfo=UTC),
        landed_at=datetime(2026, 8, 24, 2, tzinfo=UTC),
        watermark=None,
        record_count=2,
        sha256="d" * 64,
        object_path="gs://pj360-raw-test/events/d.jsonl",
    )
    client = RecoveringBigQueryClient()
    loader = BigQueryJsonLoader(client=client, location="australia-southeast1")

    with pytest.raises(ConnectionError, match="disconnected"):
        loader.load(manifest, table="valid-project.pj360_raw.events")
    assert client.job is not None
    client.job.destination = "valid-project.pj360_raw.other_events"

    with pytest.raises(RuntimeError, match="requested load: destination"):
        loader.load(manifest, table="valid-project.pj360_raw.events")
    assert client.job.result_calls == 1


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
