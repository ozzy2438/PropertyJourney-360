from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from propertyjourney360.connectors.salesforce import (
    RetryPolicy,
    SalesforceAPIError,
    SalesforceConnector,
    SalesforceOAuthConfig,
)

INSTANCE_URL = "https://propertyjourney.my.salesforce.com"
CLIENT_SECRET = "never-print-this-client-secret"
ACCESS_TOKEN = "never-print-this-access-token"


def _oauth_config() -> SalesforceOAuthConfig:
    return SalesforceOAuthConfig(
        client_id="external-client-app-id",
        client_secret=CLIENT_SECRET,
        auth_base_url="https://propertyjourney.my.salesforce.com",
        api_version="v65.0",
    )


@pytest.mark.parametrize(
    "auth_base_url",
    [
        "https://login.salesforce.com",
        "https://test.salesforce.com",
        "https://login.salesforce.com.",
        "https://evil.example",
        "https://propertyjourney.my.salesforce.com.evil.example",
        "https://propertyjourney.my.salesforce.com:444",
        "https://propertyjourney.my.salesforce.com/oauth",
    ],
)
def test_client_credentials_rejects_generic_salesforce_login_hosts(
    auth_base_url: str,
) -> None:
    with pytest.raises(ValueError, match=r"\*\.my\.salesforce\.com"):
        SalesforceOAuthConfig(
            client_id="external-client-app-id",
            client_secret=CLIENT_SECRET,
            auth_base_url=auth_base_url,
        )


def _token_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "access_token": ACCESS_TOKEN,
            "instance_url": INSTANCE_URL,
            "token_type": "Bearer",
        },
    )


def test_describe_authenticates_and_exposes_structured_rate_limit_metadata() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/services/oauth2/token":
            form = dict(httpx.QueryParams(request.content.decode()))
            assert form == {
                "grant_type": "client_credentials",
                "client_id": "external-client-app-id",
                "client_secret": CLIENT_SECRET,
            }
            return _token_response()
        assert request.url.path.endswith("/sobjects/Contact/describe")
        assert request.headers["Authorization"] == f"Bearer {ACCESS_TOKEN}"
        return httpx.Response(
            200,
            json={"name": "Contact", "fields": [{"name": "Id", "type": "id"}]},
            headers={
                "Sforce-Limit-Info": "api-usage=17/15000; api-bursts=2/500",
                "X-SFDC-Request-ID": "request-123",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SalesforceConnector(_oauth_config(), client=client)
        schema = connector.describe("Contact")

    assert schema["name"] == "Contact"
    assert len(requests) == 2
    metadata = connector.last_response_metadata
    assert metadata is not None
    assert metadata.request_id == "request-123"
    assert metadata.rate_limits.api_usage is not None
    assert metadata.rate_limits.api_usage.used == 17
    assert metadata.rate_limits.api_usage.remaining == 14983
    assert metadata.rate_limits.get("api-bursts").limit == 500  # type: ignore[union-attr]


def test_incremental_query_uses_frozen_bounds_and_follows_next_records_url() -> None:
    observed_soql: list[str] = []
    page_requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/services/oauth2/token":
            return _token_response()
        page_requests.append(str(request.url))
        if request.url.path.endswith("/query"):
            observed_soql.append(request.url.params["q"])
            return httpx.Response(
                200,
                json={
                    "totalSize": 2,
                    "done": False,
                    "records": [
                        {
                            "Id": "003000000000001",
                            "Email": "one@example.com",
                            "SystemModstamp": "2026-08-24T00:00:01.000+0000",
                        }
                    ],
                    "nextRecordsUrl": "/services/data/v65.0/query/01g-next-1",
                },
                headers={"Sforce-Limit-Info": "api-usage=20/15000"},
            )
        assert request.url.path == "/services/data/v65.0/query/01g-next-1"
        assert "q" not in request.url.params
        return httpx.Response(
            200,
            json={
                "totalSize": 2,
                "done": True,
                "records": [
                    {
                        "Id": "003000000000002",
                        "Email": "two@example.com",
                        "SystemModstamp": "2026-08-24T01:00:00.000+0000",
                    }
                ],
            },
            headers={"Sforce-Limit-Info": "api-usage=21/15000"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SalesforceConnector(_oauth_config(), client=client)
        result = connector.extract_incremental(
            object_name="Contact",
            fields=("Email",),
            low_watermark=datetime(2026, 8, 23, tzinfo=UTC),
            high_watermark=datetime(2026, 8, 24, 1, tzinfo=UTC),
        )

    assert observed_soql == [
        "SELECT Id, Email, SystemModstamp FROM Contact "
        "WHERE SystemModstamp > 2026-08-23T00:00:00.000Z "
        "AND SystemModstamp <= 2026-08-24T01:00:00.000Z "
        "ORDER BY SystemModstamp ASC, Id ASC"
    ]
    assert [record["Id"] for record in result.records] == [
        "003000000000001",
        "003000000000002",
    ]
    assert result.total_size == 2
    assert result.pages == 2
    assert len(page_requests) == 2
    assert [limits.api_usage.used for limits in result.rate_limits if limits.api_usage] == [20, 21]


def test_429_honours_retry_after_then_recovers_without_reauthenticating() -> None:
    query_attempts = 0
    token_attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal query_attempts, token_attempts
        if request.url.path == "/services/oauth2/token":
            token_attempts += 1
            return _token_response()
        query_attempts += 1
        if query_attempts == 1:
            return httpx.Response(
                429,
                json=[{"errorCode": "REQUEST_LIMIT_EXCEEDED", "message": "slow down"}],
                headers={"Retry-After": "2", "Sforce-Limit-Info": "api-usage=15000/15000"},
            )
        return httpx.Response(
            200,
            json={"totalSize": 0, "done": True, "records": []},
            headers={"Sforce-Limit-Info": "api-usage=14999/15000"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SalesforceConnector(
            _oauth_config(),
            client=client,
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay_seconds=0.01,
                max_delay_seconds=5,
            ),
            sleep=sleeps.append,
        )
        result = connector.query("SELECT Id FROM Contact")

    assert result.records == ()
    assert query_attempts == 2
    assert token_attempts == 1
    assert sleeps == [2.0]
    assert result.response_metadata[0].retry_count == 1
    assert [item.status_code for item in connector.response_metadata_history[-2:]] == [429, 200]
    throttled_usage = connector.response_metadata_history[-2].rate_limits.api_usage
    assert throttled_usage is not None
    assert throttled_usage.remaining == 0


def test_credentials_and_tokens_never_appear_in_repr_logs_or_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/services/oauth2/token"
        return httpx.Response(
            400,
            json={
                "error": "invalid_client",
                "error_description": f"rejected {CLIENT_SECRET} and {ACCESS_TOKEN}",
            },
        )

    config = _oauth_config()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SalesforceConnector(config, client=client)
        with pytest.raises(SalesforceAPIError) as captured:
            connector.authenticate()

    rendered = "\n".join((repr(config), repr(connector), str(captured.value), caplog.text))
    assert CLIENT_SECRET not in rendered
    assert ACCESS_TOKEN not in rendered
    assert "invalid_client" in str(captured.value)


def test_bulk_api_query_polls_and_downloads_all_locator_pages() -> None:
    status_polls = 0
    result_requests: list[httpx.Request] = []
    submitted_job: dict[str, Any] = {}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal status_polls, submitted_job
        if request.url.path == "/services/oauth2/token":
            return _token_response()
        if request.method == "POST" and request.url.path.endswith("/jobs/query"):
            submitted_job = json.loads(request.content)
            return httpx.Response(200, json={"id": "750PJ0000000001", "state": "UploadComplete"})
        if request.url.path.endswith("/jobs/query/750PJ0000000001"):
            status_polls += 1
            state = "InProgress" if status_polls == 1 else "JobComplete"
            return httpx.Response(200, json={"id": "750PJ0000000001", "state": state})
        assert request.url.path.endswith("/jobs/query/750PJ0000000001/results")
        result_requests.append(request)
        if "locator" not in request.url.params:
            return httpx.Response(
                200,
                text='"Id","Email"\n"0031","one@example.com"\n"0032","two@example.com"\n',
                headers={"Sforce-Locator": "next-locator"},
            )
        assert request.url.params["locator"] == "next-locator"
        return httpx.Response(
            200,
            text='"Id","Email"\n"0033","three@example.com"\n',
            headers={"Sforce-Locator": "null", "Sforce-Limit-Info": "api-usage=30/15000"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SalesforceConnector(_oauth_config(), client=client, sleep=sleeps.append)
        result = connector.bulk_query(
            "SELECT Id, Email FROM Contact",
            poll_interval_seconds=0.01,
            result_page_size=2,
        )

    assert submitted_job == {
        "operation": "query",
        "query": "SELECT Id, Email FROM Contact",
        "contentType": "CSV",
        "columnDelimiter": "COMMA",
        "lineEnding": "LF",
    }
    assert status_polls == 2
    assert sleeps == [0.01]
    assert len(result_requests) == 2
    assert all(request.url.params["maxRecords"] == "2" for request in result_requests)
    assert result.job_id == "750PJ0000000001"
    assert result.result_pages == 2
    assert [record["Id"] for record in result.records] == ["0031", "0032", "0033"]
    assert result.rate_limits[-1].api_usage.used == 30  # type: ignore[union-attr]


def test_rejects_unsafe_object_and_field_names_before_network_access() -> None:
    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("invalid identifiers must fail before a request")

    with httpx.Client(transport=httpx.MockTransport(unexpected_request)) as client:
        connector = SalesforceConnector(_oauth_config(), client=client)
        with pytest.raises(ValueError, match="object name"):
            connector.describe("Contact/../../limits")
        with pytest.raises(ValueError, match="field name"):
            connector.extract_incremental(
                object_name="Contact",
                fields=("Email FROM User",),
                low_watermark="2026-01-01T00:00:00Z",
                high_watermark="2026-01-02T00:00:00Z",
            )
