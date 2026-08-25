import hashlib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from propertyjourney360.public_data import fetch as fetch_module
from propertyjourney360.public_data.catalog import load_catalog
from propertyjourney360.public_data.fetch import (
    PublicDataFetcher,
    verify_public_fetch_manifest,
)

CATALOG = Path(__file__).parents[2] / "config" / "sources.yml"
NOW = datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC)


def test_public_fetch_write_is_atomic_and_recoverable_after_simulated_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash mid-write must never leave a half-written file under the final content-addressed
    name, and a retry afterward must succeed rather than hitting a permanent digest mismatch."""
    payload = b"DATE,CASH_RATE\n2026-08-01,3.60\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=payload, headers={"content-type": "text/csv"}, request=request
        )

    catalog = load_catalog(CATALOG)
    fetcher = PublicDataFetcher(
        catalog=catalog,
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
    )

    real_atomic_write = fetch_module._atomic_write

    def crashing_write(path: Path, write_payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_bytes(write_payload[: max(len(write_payload) // 2, 1)])
        raise RuntimeError("simulated process kill mid-write")

    monkeypatch.setattr(fetch_module, "_atomic_write", crashing_write)
    with pytest.raises(RuntimeError, match="simulated process kill"):
        fetcher.fetch_file("rba_f1_interest_rates")

    assert list(tmp_path.rglob("*.csv")) == []
    assert list(tmp_path.rglob("*.manifest.json")) == []

    monkeypatch.setattr(fetch_module, "_atomic_write", real_atomic_write)
    manifest = fetcher.fetch_file("rba_f1_interest_rates")
    assert (tmp_path / manifest.object_path).read_bytes() == payload

    replayed = fetcher.fetch_file("rba_f1_interest_rates")
    assert replayed == manifest


def test_file_fetch_is_bounded_content_addressed_and_attributed(tmp_path: Path) -> None:
    payload = b"DATE,CASH_RATE\n2026-08-01,3.60\n"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://www.rba.gov.au/statistics/tables/csv/f1-data.csv"
        return httpx.Response(
            200,
            content=payload,
            headers={"content-type": "text/csv", "etag": '"source-v1"'},
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=client,
        now=lambda: NOW,
    )
    first = fetcher.fetch_file("rba_f1_interest_rates")
    second = fetcher.fetch_file("rba_f1_interest_rates")

    assert first == second
    assert first.byte_count == len(payload)
    assert first.licence_name == "RBA Copyright and Disclaimer Notice"
    assert (tmp_path / first.object_path).read_bytes() == payload
    assert len(list(tmp_path.rglob("*.csv"))) == 1

    verified = verify_public_fetch_manifest(
        raw_root=tmp_path,
        manifest_path=next(tmp_path.rglob("*.manifest.json")),
        source=load_catalog(CATALOG).require_enabled("rba_f1_interest_rates"),
    )
    assert verified == first


def test_public_fetch_manifest_rejects_modified_payload(tmp_path: Path) -> None:
    payload = b"DATE,CASH_RATE\n2026-08-01,3.60\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, request=request)

    catalog = load_catalog(CATALOG)
    fetcher = PublicDataFetcher(
        catalog=catalog,
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
    )
    manifest = fetcher.fetch_file("rba_f1_interest_rates")
    (tmp_path / manifest.object_path).write_bytes(b"tampered payload")

    with pytest.raises(ValueError, match=r"byte count|SHA-256"):
        verify_public_fetch_manifest(
            raw_root=tmp_path,
            manifest_path=next(tmp_path.rglob("*.manifest.json")),
            source=catalog.require_enabled("rba_f1_interest_rates"),
        )


def test_public_fetch_manifest_rejects_payload_outside_source_directory(
    tmp_path: Path,
) -> None:
    payload = b"DATE,CASH_RATE\n2026-08-01,3.60\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, request=request)

    catalog = load_catalog(CATALOG)
    fetcher = PublicDataFetcher(
        catalog=catalog,
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
    )
    manifest = fetcher.fetch_file("rba_f1_interest_rates")
    manifest_path = next(tmp_path.rglob("*.manifest.json"))
    escaped_path = tmp_path / "elsewhere" / Path(manifest.object_path).name
    escaped_path.parent.mkdir()
    escaped_path.write_bytes(payload)
    manifest_path.write_text(
        manifest.model_copy(
            update={"object_path": str(escaped_path.relative_to(tmp_path))}
        ).model_dump_json(indent=2)
    )

    with pytest.raises(ValueError, match="source directory"):
        verify_public_fetch_manifest(
            raw_root=tmp_path,
            manifest_path=manifest_path,
            source=catalog.require_enabled("rba_f1_interest_rates"),
        )


def test_disabled_unverified_source_cannot_be_fetched(tmp_path: Path) -> None:
    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))),
    )
    with pytest.raises(PermissionError, match="disabled by catalogue policy"):
        fetcher.fetch_arcgis_page(
            "vips_transaction_level_sales",
            where="SALE_YEAR = 2025",
            out_fields=["SALE_YEAR"],
        )


def test_arcgis_fetch_requires_explicit_filter_and_fields(tmp_path: Path) -> None:
    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))),
    )
    with pytest.raises(ValueError, match="explicit bounded filter"):
        fetcher.fetch_arcgis_page(
            "vicmap_address",
            where="1=1",
            out_fields=["ADDRESS_ID"],
        )
    with pytest.raises(ValueError, match="field identifiers"):
        fetcher.fetch_arcgis_page(
            "vicmap_address",
            where="POSTCODE = '3000'",
            out_fields=["*"],
        )


def test_arcgis_response_count_cannot_exceed_requested_limit(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"features": [{"attributes": {"ADDRESS_ID": index}} for index in range(3)]},
            request=request,
        )

    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(ValueError, match="exceeded the requested record limit"):
        fetcher.fetch_arcgis_page(
            "vicmap_address",
            where="POSTCODE = '3000'",
            out_fields=["ADDRESS_ID"],
            record_limit=2,
        )


def test_arcgis_response_is_rejected_while_streaming_past_limit(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'{"features":[]}' + b" " * 100, request=request)

    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        max_bytes=20,
    )
    with pytest.raises(ValueError, match=r"declared response exceeds|exceeded configured maximum"):
        fetcher.fetch_arcgis_page(
            "vicmap_address",
            where="POSTCODE = '3000'",
            out_fields=["ADDRESS_ID"],
        )


def test_declared_file_size_is_rejected_before_streaming(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"too large",
            headers={"content-length": "999"},
            request=request,
        )

    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        max_bytes=10,
    )
    with pytest.raises(ValueError, match="declared response exceeds"):
        fetcher.fetch_file("rba_f1_interest_rates")


def test_resolved_sdmx_query_is_bounded_and_recorded(tmp_path: Path) -> None:
    payload = b"DATAFLOW,MEASURE,TIME_PERIOD,OBS_VALUE\nABS,POP,2021,123\n"
    resolved_url = "https://data.api.abs.gov.au/rest/data/ABS/example/1.0.0?startPeriod=2021"

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == resolved_url
        return httpx.Response(200, content=payload, request=request)

    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
    )
    manifest = fetcher.fetch_resolved("abs_data_api", resolved_url=resolved_url)
    assert manifest.sha256 == hashlib.sha256(payload).hexdigest()
    assert manifest.requested_url == resolved_url


def test_cross_domain_resolved_file_requires_and_verifies_hash(tmp_path: Path) -> None:
    payload = b"release"
    resolved_url = "https://cdn.example.test/vgv-release.xlsx"
    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=payload, request=request)
            )
        ),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="cross-domain"):
        fetcher.fetch_resolved(
            "vgv_property_sales_timeseries",
            resolved_url=resolved_url,
        )
    with pytest.raises(ValueError, match="did not match"):
        fetcher.fetch_resolved(
            "vgv_property_sales_timeseries",
            resolved_url=resolved_url,
            expected_sha256="0" * 64,
        )


def test_same_domain_resolved_url_cannot_redirect_cross_domain_without_hash(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.land.vic.gov.au":
            return httpx.Response(
                302,
                headers={"location": "https://cdn.example.test/release.xlsx"},
                request=request,
            )
        return httpx.Response(
            200,
            content=b"redirected release",
            request=request,
        )

    fetcher = PublicDataFetcher(
        catalog=load_catalog(CATALOG),
        root=tmp_path,
        client=httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="resolved redirects"):
        fetcher.fetch_resolved(
            "vgv_property_sales_timeseries",
            resolved_url=(
                "https://www.land.vic.gov.au/valuations/resources-and-reports/"
                "property-sales-statistics/download"
            ),
        )
