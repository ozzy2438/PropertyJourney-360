"""Create-only GCS raw landing with content hashes and manifest replay safety."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from propertyjourney360.ingestion.landing import (
    LandingManifest,
    _canonical_json_line,
    validate_manifest_replay,
)


class GCSRawLanding:
    """Land JSONL objects without granting overwrite or delete semantics."""

    def __init__(
        self,
        *,
        bucket_name: str,
        client: Any | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not bucket_name:
            raise ValueError("bucket_name is required")
        if client is None:
            import google.cloud.storage as storage  # type: ignore[import-untyped]

            client = storage.Client()
        self.bucket_name = bucket_name
        self.client = client
        self._now = now or (lambda: datetime.now(UTC))

    def land(
        self,
        *,
        source: str,
        run_id: str,
        schema_version: str,
        records: Iterable[Mapping[str, Any]],
        extracted_at: datetime,
        watermark: str | None = None,
    ) -> LandingManifest:
        lines = tuple(_canonical_json_line(record) for record in records)
        payload = b"".join(lines)
        digest = hashlib.sha256(payload).hexdigest()
        partition = extracted_at.astimezone(UTC).strftime("extracted_date=%Y-%m-%d")
        prefix = f"{source}/{partition}/run_id={run_id}"
        object_name = f"{prefix}/{digest}.jsonl"
        manifest_name = f"{prefix}/{digest}.manifest.json"
        bucket = self.client.bucket(self.bucket_name)

        self._upload_create_only(
            bucket.blob(object_name),
            payload,
            content_type="application/x-ndjson",
            digest=digest,
        )
        manifest = LandingManifest(
            source=source,
            run_id=run_id,
            schema_version=schema_version,
            extracted_at=extracted_at,
            landed_at=self._now().astimezone(UTC),
            watermark=watermark,
            record_count=len(lines),
            sha256=digest,
            object_path=f"gs://{self.bucket_name}/{object_name}",
        )
        manifest_blob = bucket.blob(manifest_name)
        try:
            manifest_blob.upload_from_string(
                manifest.model_dump_json(indent=2).encode(),
                content_type="application/json",
                if_generation_match=0,
                checksum="crc32c",
            )
        except Exception as exc:
            from google.api_core.exceptions import PreconditionFailed

            if not isinstance(exc, PreconditionFailed):
                raise
            previous = LandingManifest.model_validate_json(manifest_blob.download_as_bytes())
            try:
                validate_manifest_replay(previous, manifest, location="GCS landing")
            except RuntimeError as mismatch:
                raise mismatch from exc
            return previous
        return manifest

    @staticmethod
    def _upload_create_only(
        blob: Any,
        payload: bytes,
        *,
        content_type: str,
        digest: str,
    ) -> None:
        blob.metadata = {"sha256": digest}
        try:
            blob.upload_from_string(
                payload,
                content_type=content_type,
                if_generation_match=0,
                checksum="crc32c",
            )
        except Exception as exc:
            from google.api_core.exceptions import PreconditionFailed

            if not isinstance(exc, PreconditionFailed):
                raise
            blob.reload()
            if (blob.metadata or {}).get("sha256") != digest or blob.size != len(payload):
                raise RuntimeError("existing GCS object does not match replay payload") from exc
