"""Bounded, licence-gated acquisition of authoritative public data."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.public_data.catalog import SourceCatalog, SourceDefinition

_SAFE_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXTENSIONS = {
    "csv": "csv",
    "esri_json": "json",
    "xlsx": "xlsx",
    "zip_xml": "zip",
}


def _atomic_write(path: Path, payload: bytes) -> None:
    """Write via a same-directory temp file then rename, so a crash never leaves a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def verify_public_fetch_manifest(
    *,
    raw_root: Path,
    manifest_path: Path,
    source: SourceDefinition,
) -> PublicFetchManifest:
    """Load a fetch manifest only after re-validating its content-addressed payload."""
    root = raw_root.resolve()
    resolved_manifest = manifest_path.resolve(strict=True)
    try:
        manifest_relative = resolved_manifest.relative_to(root)
    except ValueError as exc:
        raise ValueError("public fetch manifest escapes the configured raw root") from exc

    manifest = PublicFetchManifest.model_validate_json(resolved_manifest.read_text())
    if manifest.source_id != source.source_id:
        raise ValueError("public fetch manifest source_id does not match the catalog source")
    if not manifest_relative.parts or manifest_relative.parts[0] != source.source_id:
        raise ValueError("public fetch manifest is outside its catalog source directory")

    catalog_claims = {
        "publisher": source.publisher,
        "dataset_title": source.dataset_title,
        "licence_name": source.licence_name,
        "licence_url": str(source.licence_url) if source.licence_url else None,
        "attribution_text": source.attribution_text,
        "redistribution_status": source.redistribution_status.value,
        "repo_policy": source.repo_policy.value,
    }
    for field, expected in catalog_claims.items():
        if getattr(manifest, field) != expected:
            raise ValueError(f"public fetch manifest {field} does not match the catalog")
    if not 200 <= manifest.status_code < 300:
        raise ValueError("public fetch manifest does not record a successful HTTP response")
    if manifest.retrieved_at.tzinfo is None:
        raise ValueError("public fetch manifest retrieved_at must be timezone-aware")

    object_path = Path(manifest.object_path)
    if object_path.is_absolute() or any(part in {"", ".", ".."} for part in object_path.parts):
        raise ValueError("public fetch object_path must be a normalized relative path")
    try:
        payload_path = (root / object_path).resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError("public fetch payload does not exist") from exc
    source_root = (root / source.source_id).resolve()
    try:
        payload_path.relative_to(source_root)
    except ValueError as exc:
        raise ValueError("public fetch payload escapes its catalog source directory") from exc
    if not payload_path.is_file():
        raise ValueError("public fetch payload is not a regular file")

    expected_extension = _EXTENSIONS.get(source.format, "bin")
    if payload_path.name != f"{manifest.sha256}.{expected_extension}":
        raise ValueError("public fetch payload name or suffix does not match its digest")
    if resolved_manifest.name != f"{manifest.sha256}.manifest.json":
        raise ValueError("public fetch manifest name does not match its digest")
    if payload_path.stat().st_size != manifest.byte_count:
        raise ValueError("public fetch payload byte count does not match its manifest")

    digest = hashlib.sha256()
    with payload_path.open("rb") as payload_file:
        for chunk in iter(lambda: payload_file.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != manifest.sha256:
        raise ValueError("public fetch payload SHA-256 does not match its manifest")
    return manifest


class PublicFetchManifest(BaseModel):
    """Auditable metadata for one raw public-source response."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    publisher: str
    dataset_title: str
    retrieved_at: datetime
    requested_url: str
    final_url: str
    status_code: int
    content_type: str | None
    etag: str | None
    last_modified: str | None
    byte_count: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    object_path: str
    response_record_count: int | None = Field(default=None, ge=0)
    licence_name: str
    licence_url: str | None
    attribution_text: str | None
    redistribution_status: str
    repo_policy: str
    limitations: tuple[str, ...] = ()


class PublicDataFetcher:
    """Fetch only enabled catalogue entries and persist content-addressed raw bytes."""

    def __init__(
        self,
        *,
        catalog: SourceCatalog,
        root: Path,
        client: httpx.Client | None = None,
        max_bytes: int = 50 * 1024 * 1024,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.catalog = catalog
        self.root = root
        self.max_bytes = max_bytes
        self._now = now or (lambda: datetime.now(UTC))
        self._owns_client = client is None
        self.client = client or httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(60.0, connect=15.0),
            headers={"User-Agent": "PropertyJourney360/0.1 public-data-acquisition"},
        )

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def __enter__(self) -> PublicDataFetcher:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def fetch_file(self, source_id: str) -> PublicFetchManifest:
        """Fetch one exact catalogue URL for bounded file/export methods."""
        source = self.catalog.require_enabled(source_id)
        allowed_methods = {"http_file", "opendata_export"}
        if source.access_method not in allowed_methods:
            raise ValueError(
                f"source {source_id} uses {source.access_method}; "
                "use its explicit acquisition adapter"
            )
        requested_url = str(source.access_url)
        with self.client.stream("GET", requested_url) as response:
            response.raise_for_status()
            self._validate_response(response)
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > self.max_bytes:
                    raise ValueError(
                        f"response exceeded configured maximum of {self.max_bytes} bytes"
                    )
            return self._persist(
                source=source,
                requested_url=requested_url,
                response=response,
                payload=bytes(payload),
                response_record_count=None,
                limitations=(source.quality_notes,),
            )

    def fetch_resolved(
        self,
        source_id: str,
        *,
        resolved_url: str,
        expected_sha256: str | None = None,
    ) -> PublicFetchManifest:
        """Fetch a bounded owner-resolved official file or SDMX query.

        Metadata catalogues frequently rotate signed download URLs. Same-domain resolutions are
        accepted directly; a cross-domain CDN resolution requires an independently supplied SHA-256
        so a changed redirect cannot silently alter the selected release.
        """

        source = self.catalog.require_enabled(source_id)
        allowed_methods = {"metadata_resolved_file", "sdmx_rest"}
        if source.access_method not in allowed_methods:
            raise ValueError(f"source {source_id} does not use a resolved acquisition method")
        parsed = urlsplit(resolved_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("resolved_url must be an absolute credential-free HTTPS URL")
        catalog_hosts = {
            urlsplit(str(source.access_url)).hostname,
            urlsplit(str(source.metadata_url)).hostname,
        }
        if parsed.hostname not in catalog_hosts and expected_sha256 is None:
            raise ValueError("cross-domain resolved URLs require --expected-sha256")
        if expected_sha256 is not None and not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
            raise ValueError("expected_sha256 must be 64 lowercase hexadecimal characters")

        with self.client.stream("GET", resolved_url) as response:
            response.raise_for_status()
            self._validate_response(response)
            if response.url.host not in catalog_hosts and expected_sha256 is None:
                raise ValueError("cross-domain resolved redirects require --expected-sha256")
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > self.max_bytes:
                    raise ValueError(
                        f"response exceeded configured maximum of {self.max_bytes} bytes"
                    )
            digest = hashlib.sha256(payload).hexdigest()
            if expected_sha256 is not None and digest != expected_sha256:
                raise ValueError("resolved response did not match expected_sha256")
            return self._persist(
                source=source,
                requested_url=resolved_url,
                response=response,
                payload=bytes(payload),
                response_record_count=None,
                limitations=(
                    source.quality_notes,
                    "Exact bounded release/query URL was resolved by the operator and recorded.",
                ),
            )

    def fetch_arcgis_page(
        self,
        source_id: str,
        *,
        where: str,
        out_fields: Sequence[str],
        record_limit: int = 100,
        result_offset: int = 0,
        include_geometry: bool = False,
    ) -> PublicFetchManifest:
        """Fetch one deliberately bounded ArcGIS page; statewide defaults are prohibited."""
        source = self.catalog.require_enabled(source_id)
        if source.access_method != "arcgis_rest":
            raise ValueError(f"source {source_id} is not an ArcGIS REST source")
        if not where.strip() or where.strip() == "1=1":
            raise ValueError("ArcGIS where must be an explicit bounded filter, not 1=1")
        if not 1 <= record_limit <= 1_000:
            raise ValueError("record_limit must be between 1 and 1000")
        if result_offset < 0:
            raise ValueError("result_offset cannot be negative")
        if not out_fields or any(not _SAFE_FIELD.fullmatch(field) for field in out_fields):
            raise ValueError("out_fields must contain explicit ArcGIS field identifiers")

        params: dict[str, str | int] = {
            "where": where,
            "outFields": ",".join(out_fields),
            "resultOffset": result_offset,
            "resultRecordCount": record_limit,
            "returnGeometry": str(include_geometry).lower(),
            "outSR": 4326,
            "f": "json",
        }
        requested_url = str(source.access_url)
        with self.client.stream("GET", requested_url, params=params) as response:
            response.raise_for_status()
            self._validate_response(response)
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > self.max_bytes:
                    raise ValueError(
                        f"response exceeded configured maximum of {self.max_bytes} bytes"
                    )
        try:
            document: dict[str, Any] = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError("ArcGIS response was not valid JSON") from exc
        if document.get("error"):
            raise ValueError(f"ArcGIS returned an error: {document['error']}")
        features = document.get("features")
        if not isinstance(features, list):
            raise ValueError("ArcGIS response did not contain a features list")
        if len(features) > record_limit:
            raise ValueError("ArcGIS response exceeded the requested record limit")

        return self._persist(
            source=source,
            requested_url=str(response.request.url),
            response=response,
            payload=bytes(payload),
            response_record_count=len(features),
            limitations=(
                source.quality_notes,
                "One explicitly filtered ArcGIS page; not a statewide completeness claim.",
            ),
        )

    def _validate_response(self, response: httpx.Response) -> None:
        if response.url.scheme != "https":
            raise ValueError("public-source redirects must remain on HTTPS")
        declared_length = response.headers.get("content-length")
        if declared_length and int(declared_length) > self.max_bytes:
            raise ValueError(f"declared response exceeds maximum of {self.max_bytes} bytes")

    def _persist(
        self,
        *,
        source: SourceDefinition,
        requested_url: str,
        response: httpx.Response,
        payload: bytes,
        response_record_count: int | None,
        limitations: tuple[str, ...],
    ) -> PublicFetchManifest:
        retrieved_at = self._now().astimezone(UTC)
        digest = hashlib.sha256(payload).hexdigest()
        extension = _EXTENSIONS.get(source.format, "bin")
        partition = retrieved_at.strftime("retrieved_date=%Y-%m-%d")
        target_dir = self.root / source.source_id / partition
        target_dir.mkdir(parents=True, exist_ok=True)
        data_path = target_dir / f"{digest}.{extension}"
        manifest_path = target_dir / f"{digest}.manifest.json"

        if data_path.exists():
            if hashlib.sha256(data_path.read_bytes()).hexdigest() != digest:
                raise RuntimeError(f"content-addressed public data was modified: {data_path}")
        else:
            _atomic_write(data_path, payload)

        manifest = PublicFetchManifest(
            source_id=source.source_id,
            publisher=source.publisher,
            dataset_title=source.dataset_title,
            retrieved_at=retrieved_at,
            requested_url=requested_url,
            final_url=str(response.url),
            status_code=response.status_code,
            content_type=response.headers.get("content-type"),
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            byte_count=len(payload),
            sha256=digest,
            object_path=str(data_path.relative_to(self.root)),
            response_record_count=response_record_count,
            licence_name=source.licence_name,
            licence_url=str(source.licence_url) if source.licence_url else None,
            attribution_text=source.attribution_text,
            redistribution_status=source.redistribution_status.value,
            repo_policy=source.repo_policy.value,
            limitations=limitations,
        )
        if manifest_path.exists():
            previous = PublicFetchManifest.model_validate_json(manifest_path.read_text())
            if previous.sha256 != digest:
                raise RuntimeError("public fetch manifest does not match content digest")
            return previous
        _atomic_write(manifest_path, manifest.model_dump_json(indent=2).encode())
        return manifest
