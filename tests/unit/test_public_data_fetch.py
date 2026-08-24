from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from propertyjourney360.public_data.catalog import load_catalog
from propertyjourney360.public_data.fetch import PublicDataFetcher

CATALOG = Path(__file__).parents[2] / "config" / "sources.yml"
NOW = datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC)


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
