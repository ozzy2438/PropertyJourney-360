"""Salesforce REST and Bulk API 2.0 extraction with bounded, observable retries.

The connector deliberately accepts an ``httpx.Client`` so tests and calling services can
provide their own transport, connection pool, proxy and TLS policy. OAuth credentials and
access tokens are sent only in request bodies/headers and are excluded from representations,
logs and raised error messages.
"""

from __future__ import annotations

import csv
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from io import StringIO
from typing import Any, Self, cast
from urllib.parse import urljoin, urlsplit

import httpx

logger = logging.getLogger(__name__)

_API_VERSION_RE = re.compile(r"^v?\d+\.\d+$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_LOCATOR_RE = re.compile(r"^[A-Za-z0-9._~-]{1,512}$")
_SAFE_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_LIMIT_RE = re.compile(r"^(?P<used>\d+)/(?P<limit>\d+)(?:\((?P<detail>.*)\))?$")


class SalesforceError(RuntimeError):
    """Base class for sanitized connector failures."""


class SalesforceTransportError(SalesforceError):
    """A request could not reach Salesforce after bounded retries."""


class SalesforceProtocolError(SalesforceError):
    """Salesforce returned a response that did not satisfy the API contract."""


class SalesforceAPIError(SalesforceError):
    """A non-success Salesforce response with a deliberately sanitized message."""

    def __init__(
        self,
        *,
        status_code: int,
        method: str,
        endpoint: str,
        retry_count: int,
        error_codes: tuple[str, ...] = (),
    ) -> None:
        self.status_code = status_code
        self.method = method
        self.endpoint = endpoint
        self.retry_count = retry_count
        self.error_codes = error_codes
        code_text = f"; codes={','.join(error_codes)}" if error_codes else ""
        super().__init__(
            f"Salesforce API request failed: {method} {endpoint}; "
            f"status={status_code}; retries={retry_count}{code_text}"
        )


@dataclass(frozen=True, slots=True)
class SalesforceOAuthConfig:
    """OAuth client-credentials settings for an owner-configured Salesforce app.

    The External Client App or legacy Connected App must have the client credentials flow
    enabled and an integration user selected by the Salesforce org owner. A My Domain login
    URL is preferred, while ``https://login.salesforce.com`` also works where org policy allows.
    """

    client_id: str
    client_secret: str = field(repr=False)
    auth_base_url: str = "https://login.salesforce.com"
    api_version: str = "v65.0"
    scope: str | None = None

    def __post_init__(self) -> None:
        if not self.client_id.strip():
            raise ValueError("client_id must not be empty")
        if not self.client_secret:
            raise ValueError("client_secret must not be empty")
        parsed = urlsplit(self.auth_base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("auth_base_url must be an absolute HTTPS URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("auth_base_url must not contain credentials, query or fragment")
        if not _API_VERSION_RE.fullmatch(self.api_version):
            raise ValueError("api_version must look like v65.0")
        normalized_version = self.api_version
        if not normalized_version.startswith("v"):
            normalized_version = f"v{normalized_version}"
        object.__setattr__(self, "api_version", normalized_version)
        object.__setattr__(self, "auth_base_url", self.auth_base_url.rstrip("/"))

    @property
    def token_url(self) -> str:
        return f"{self.auth_base_url}/services/oauth2/token"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential retry policy for throttling and transient server failures."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be non-negative")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be at least base_delay_seconds")


@dataclass(frozen=True, slots=True)
class OAuthToken:
    """Authenticated Salesforce session; the bearer token is hidden from repr/log output."""

    access_token: str = field(repr=False)
    instance_url: str
    token_type: str


@dataclass(frozen=True, slots=True)
class LimitWindow:
    """One ``Sforce-Limit-Info`` usage window."""

    name: str
    used: int
    limit: int
    detail: str | None = None

    @property
    def remaining(self) -> int:
        return max(self.limit - self.used, 0)

    @property
    def utilization(self) -> float:
        return self.used / self.limit if self.limit else 1.0


@dataclass(frozen=True, slots=True)
class SalesforceRateLimits:
    """Parsed, structured rate-limit metadata from a Salesforce response."""

    windows: tuple[LimitWindow, ...] = ()

    def get(self, name: str) -> LimitWindow | None:
        return next((window for window in self.windows if window.name == name), None)

    @property
    def api_usage(self) -> LimitWindow | None:
        return self.get("api-usage")


@dataclass(frozen=True, slots=True)
class ResponseMetadata:
    """Safe observability metadata for a single HTTP response."""

    status_code: int
    retry_count: int
    rate_limits: SalesforceRateLimits
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class QueryResult:
    """A fully paginated synchronous REST query result."""

    records: tuple[dict[str, Any], ...]
    total_size: int
    pages: int
    response_metadata: tuple[ResponseMetadata, ...]
    soql: str = field(repr=False)

    @property
    def rate_limits(self) -> tuple[SalesforceRateLimits, ...]:
        return tuple(item.rate_limits for item in self.response_metadata)


@dataclass(frozen=True, slots=True)
class IncrementalQueryResult:
    """REST records extracted inside one immutable SystemModstamp window."""

    object_name: str
    low_watermark: datetime
    high_watermark: datetime
    records: tuple[dict[str, Any], ...]
    total_size: int
    pages: int
    response_metadata: tuple[ResponseMetadata, ...]
    soql: str = field(repr=False)

    @property
    def rate_limits(self) -> tuple[SalesforceRateLimits, ...]:
        return tuple(item.rate_limits for item in self.response_metadata)


@dataclass(frozen=True, slots=True)
class BulkQueryResult:
    """Completed Bulk API 2.0 query records and safe execution metadata."""

    job_id: str
    records: tuple[dict[str, str], ...]
    result_pages: int
    response_metadata: tuple[ResponseMetadata, ...]

    @property
    def rate_limits(self) -> tuple[SalesforceRateLimits, ...]:
        return tuple(item.rate_limits for item in self.response_metadata)


def parse_rate_limit_info(header: str | None) -> SalesforceRateLimits:
    """Parse known and future ``Sforce-Limit-Info`` windows without losing their names."""

    if not header:
        return SalesforceRateLimits()
    windows: list[LimitWindow] = []
    for segment in header.split(";"):
        name, separator, raw_value = segment.strip().partition("=")
        if not separator or not name:
            continue
        match = _LIMIT_RE.fullmatch(raw_value.strip())
        if match is None:
            continue
        windows.append(
            LimitWindow(
                name=name.strip(),
                used=int(match.group("used")),
                limit=int(match.group("limit")),
                detail=match.group("detail"),
            )
        )
    return SalesforceRateLimits(windows=tuple(windows))


def _safe_endpoint(url: str) -> str:
    parsed = urlsplit(url)
    return parsed.path or "/"


def _validate_instance_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise SalesforceProtocolError("OAuth response contained an invalid HTTPS instance URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise SalesforceProtocolError("OAuth response contained an unsafe instance URL")
    return value.rstrip("/")


def _parse_datetime(value: datetime | str, *, field_name: str) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    else:
        parsed = value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed.astimezone(UTC)


def _salesforce_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class SalesforceConnector:
    """Synchronous Salesforce extractor for REST and Bulk API 2.0 query workloads."""

    def __init__(
        self,
        oauth: SalesforceOAuthConfig,
        *,
        client: httpx.Client | None = None,
        retry_policy: RetryPolicy | None = None,
        timeout_seconds: float = 30.0,
        max_query_pages: int = 10_000,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_query_pages < 1:
            raise ValueError("max_query_pages must be at least 1")
        self.oauth = oauth
        self.retry_policy = retry_policy or RetryPolicy()
        self.max_query_pages = max_query_pages
        self._sleep = sleep
        self._monotonic = monotonic
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout_seconds,
            headers={"User-Agent": "propertyjourney360-salesforce/0.1"},
        )
        self._token: OAuthToken | None = None
        self._last_response_metadata: ResponseMetadata | None = None
        self._response_metadata_history: list[ResponseMetadata] = []

    @property
    def last_response_metadata(self) -> ResponseMetadata | None:
        return self._last_response_metadata

    @property
    def response_metadata_history(self) -> tuple[ResponseMetadata, ...]:
        """All received response metadata, including throttled/retried responses."""

        return tuple(self._response_metadata_history)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        """Close an internally-created client; injected client ownership stays with the caller."""

        if self._owns_client:
            self._client.close()

    def authenticate(self, *, force: bool = False) -> OAuthToken:
        """Acquire an OAuth token using the app owner's configured integration user."""

        if self._token is not None and not force:
            return self._token
        form = {
            "grant_type": "client_credentials",
            "client_id": self.oauth.client_id,
            "client_secret": self.oauth.client_secret,
        }
        if self.oauth.scope:
            form["scope"] = self.oauth.scope
        response, _metadata = self._send_with_retry(
            "POST",
            self.oauth.token_url,
            headers={"Accept": "application/json"},
            data=form,
        )
        payload = self._json_object(response, context="OAuth token")
        access_token = payload.get("access_token")
        instance_url = payload.get("instance_url")
        token_type = payload.get("token_type", "Bearer")
        if not isinstance(access_token, str) or not access_token:
            raise SalesforceProtocolError("OAuth response omitted access_token")
        if not isinstance(instance_url, str):
            raise SalesforceProtocolError("OAuth response omitted instance_url")
        if not isinstance(token_type, str) or not token_type:
            raise SalesforceProtocolError("OAuth response contained an invalid token_type")
        self._token = OAuthToken(
            access_token=access_token,
            instance_url=_validate_instance_url(instance_url),
            token_type=token_type,
        )
        logger.info(
            "salesforce_oauth_succeeded",
            extra={"sf_event": {"endpoint": _safe_endpoint(self.oauth.token_url)}},
        )
        return self._token

    def describe(self, object_name: str) -> dict[str, Any]:
        """Return the live REST schema description for a Salesforce sObject."""

        self._validate_object_name(object_name)
        path = f"/services/data/{self.oauth.api_version}/sobjects/{object_name}/describe"
        response, _metadata = self._api_request("GET", path)
        return self._json_object(response, context=f"describe {object_name}")

    def query(self, soql: str, *, include_deleted: bool = False) -> QueryResult:
        """Execute SOQL and follow every server-provided ``nextRecordsUrl`` cursor."""

        if not soql.strip():
            raise ValueError("soql must not be empty")
        resource = "queryAll" if include_deleted else "query"
        path = f"/services/data/{self.oauth.api_version}/{resource}"
        records: list[dict[str, Any]] = []
        metadata: list[ResponseMetadata] = []
        seen_urls: set[str] = set()
        total_size: int | None = None
        next_path: str | None = path
        params: Mapping[str, str] | None = {"q": soql}

        while next_path is not None:
            if len(metadata) >= self.max_query_pages:
                raise SalesforceProtocolError("REST query exceeded the configured page limit")
            if next_path in seen_urls:
                raise SalesforceProtocolError("REST query returned a repeated nextRecordsUrl")
            seen_urls.add(next_path)
            response, response_metadata = self._api_request("GET", next_path, params=params)
            params = None
            metadata.append(response_metadata)
            payload = self._json_object(response, context="REST query")
            page_records = payload.get("records")
            if not isinstance(page_records, list) or not all(
                isinstance(record, dict) for record in page_records
            ):
                raise SalesforceProtocolError("REST query response contained invalid records")
            records.extend(cast(list[dict[str, Any]], page_records))
            if total_size is None:
                candidate_total = payload.get("totalSize")
                if not isinstance(candidate_total, int) or candidate_total < 0:
                    raise SalesforceProtocolError("REST query response omitted totalSize")
                total_size = candidate_total
            done = payload.get("done")
            if not isinstance(done, bool):
                raise SalesforceProtocolError("REST query response omitted done")
            if done:
                next_path = None
            else:
                candidate_next = payload.get("nextRecordsUrl")
                if not isinstance(candidate_next, str) or not candidate_next:
                    raise SalesforceProtocolError(
                        "REST query response omitted nextRecordsUrl before completion"
                    )
                next_path = candidate_next

        return QueryResult(
            records=tuple(records),
            total_size=total_size or 0,
            pages=len(metadata),
            response_metadata=tuple(metadata),
            soql=soql,
        )

    def extract_incremental(
        self,
        *,
        object_name: str,
        fields: Sequence[str],
        low_watermark: datetime | str,
        high_watermark: datetime | str,
        include_deleted: bool = False,
    ) -> IncrementalQueryResult:
        """Extract ``low < SystemModstamp <= frozen high`` without a moving-window gap."""

        self._validate_object_name(object_name)
        if not fields:
            raise ValueError("fields must not be empty")
        selected: list[str] = []
        seen_fields: set[str] = set()
        for candidate in ("Id", *fields, "SystemModstamp"):
            if not _FIELD_RE.fullmatch(candidate):
                raise ValueError(f"invalid Salesforce field name: {candidate!r}")
            folded = candidate.casefold()
            if folded not in seen_fields:
                seen_fields.add(folded)
                selected.append(candidate)

        low = _parse_datetime(low_watermark, field_name="low_watermark")
        high = _parse_datetime(high_watermark, field_name="high_watermark")
        if high < low:
            raise ValueError("high_watermark must not precede low_watermark")
        soql = (
            f"SELECT {', '.join(selected)} FROM {object_name} "  # noqa: S608
            f"WHERE SystemModstamp > {_salesforce_timestamp(low)} "
            f"AND SystemModstamp <= {_salesforce_timestamp(high)} "
            "ORDER BY SystemModstamp ASC, Id ASC"
        )
        query_result = self.query(soql, include_deleted=include_deleted)
        return IncrementalQueryResult(
            object_name=object_name,
            low_watermark=low,
            high_watermark=high,
            records=query_result.records,
            total_size=query_result.total_size,
            pages=query_result.pages,
            response_metadata=query_result.response_metadata,
            soql=soql,
        )

    def bulk_query(
        self,
        soql: str,
        *,
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float = 600.0,
        result_page_size: int = 100_000,
    ) -> BulkQueryResult:
        """Run a Bulk API 2.0 query job, poll it and download every CSV locator page."""

        if not soql.strip():
            raise ValueError("soql must not be empty")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if result_page_size < 1:
            raise ValueError("result_page_size must be positive")

        base_path = f"/services/data/{self.oauth.api_version}/jobs/query"
        metadata: list[ResponseMetadata] = []
        create_response, create_metadata = self._api_request(
            "POST",
            base_path,
            json={
                "operation": "query",
                "query": soql,
                "contentType": "CSV",
                "columnDelimiter": "COMMA",
                "lineEnding": "LF",
            },
        )
        metadata.append(create_metadata)
        create_payload = self._json_object(create_response, context="Bulk query creation")
        job_id = create_payload.get("id")
        if not isinstance(job_id, str) or not _JOB_ID_RE.fullmatch(job_id):
            raise SalesforceProtocolError("Bulk query creation returned an invalid job id")

        job_path = f"{base_path}/{job_id}"
        deadline = self._monotonic() + timeout_seconds
        state = ""
        while state != "JobComplete":
            poll_response, poll_metadata = self._api_request("GET", job_path)
            metadata.append(poll_metadata)
            poll_payload = self._json_object(poll_response, context="Bulk query status")
            candidate_state = poll_payload.get("state")
            if not isinstance(candidate_state, str):
                raise SalesforceProtocolError("Bulk query status omitted state")
            state = candidate_state
            if state in {"Failed", "Aborted"}:
                raise SalesforceProtocolError(f"Bulk query ended in terminal state {state}")
            if state == "JobComplete":
                break
            if self._monotonic() >= deadline:
                raise SalesforceProtocolError("Bulk query timed out before completion")
            self._sleep(poll_interval_seconds)

        records: list[dict[str, str]] = []
        locator: str | None = None
        seen_locators: set[str] = set()
        result_pages = 0
        while True:
            if result_pages >= self.max_query_pages:
                raise SalesforceProtocolError("Bulk query exceeded the configured page limit")
            params: dict[str, str | int] = {"maxRecords": result_page_size}
            if locator is not None:
                params["locator"] = locator
            result_response, result_metadata = self._api_request(
                "GET",
                f"{job_path}/results",
                params=params,
                headers={"Accept": "text/csv"},
            )
            metadata.append(result_metadata)
            result_pages += 1
            records.extend(self._parse_csv_records(result_response.text))

            raw_locator = result_response.headers.get("Sforce-Locator")
            if raw_locator is None or raw_locator.strip().lower() in {"", "null"}:
                break
            locator = raw_locator.strip()
            if not _LOCATOR_RE.fullmatch(locator):
                raise SalesforceProtocolError("Bulk query returned an invalid result locator")
            if locator in seen_locators:
                raise SalesforceProtocolError("Bulk query returned a repeated result locator")
            seen_locators.add(locator)

        return BulkQueryResult(
            job_id=job_id,
            records=tuple(records),
            result_pages=result_pages,
            response_metadata=tuple(metadata),
        )

    def _api_request(
        self,
        method: str,
        path_or_url: str,
        **kwargs: Any,
    ) -> tuple[httpx.Response, ResponseMetadata]:
        refreshed = False
        base_headers = dict(cast(Mapping[str, str], kwargs.pop("headers", {})))
        while True:
            token = self.authenticate(force=refreshed)
            url = self._resolve_api_url(token.instance_url, path_or_url)
            supplied_headers = base_headers.copy()
            supplied_headers["Authorization"] = f"{token.token_type} {token.access_token}"
            supplied_headers.setdefault("Accept", "application/json")
            try:
                return self._send_with_retry(method, url, headers=supplied_headers, **kwargs)
            except SalesforceAPIError as exc:
                if exc.status_code == 401 and not refreshed:
                    self._token = None
                    refreshed = True
                    continue
                raise

    def _send_with_retry(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> tuple[httpx.Response, ResponseMetadata]:
        endpoint = _safe_endpoint(url)
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                response = self._client.request(method, url, **kwargs)
            except httpx.TransportError:
                if attempt >= self.retry_policy.max_attempts:
                    raise SalesforceTransportError(
                        f"Salesforce transport failed: {method} {endpoint}; retries={attempt - 1}"
                    ) from None
                delay = self._backoff_delay(None, attempt)
                self._log_retry(
                    method,
                    endpoint,
                    None,
                    attempt,
                    delay,
                    SalesforceRateLimits(),
                )
                self._sleep(delay)
                continue

            metadata = ResponseMetadata(
                status_code=response.status_code,
                retry_count=attempt - 1,
                rate_limits=parse_rate_limit_info(response.headers.get("Sforce-Limit-Info")),
                request_id=self._request_id(response.headers),
            )
            self._last_response_metadata = metadata
            self._response_metadata_history.append(metadata)
            is_retryable = response.status_code == 429 or 500 <= response.status_code <= 599
            if is_retryable and (attempt < self.retry_policy.max_attempts):
                delay = self._backoff_delay(response, attempt)
                self._log_retry(
                    method,
                    endpoint,
                    response.status_code,
                    attempt,
                    delay,
                    metadata.rate_limits,
                )
                response.close()
                self._sleep(delay)
                continue
            if not response.is_success:
                raise SalesforceAPIError(
                    status_code=response.status_code,
                    method=method,
                    endpoint=endpoint,
                    retry_count=attempt - 1,
                    error_codes=self._safe_error_codes(response),
                )
            logger.info(
                "salesforce_request_completed",
                extra={
                    "sf_event": {
                        "method": method,
                        "endpoint": endpoint,
                        "status_code": response.status_code,
                        "retry_count": attempt - 1,
                        "rate_limits": [
                            {
                                "name": window.name,
                                "used": window.used,
                                "limit": window.limit,
                            }
                            for window in metadata.rate_limits.windows
                        ],
                    }
                },
            )
            return response, metadata
        raise AssertionError("retry loop must always return or raise")

    def _backoff_delay(self, response: httpx.Response | None, attempt: int) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    seconds = float(retry_after)
                except ValueError:
                    try:
                        retry_at = parsedate_to_datetime(retry_after)
                        if retry_at.tzinfo is None:
                            retry_at = retry_at.replace(tzinfo=UTC)
                        seconds = max((retry_at - datetime.now(UTC)).total_seconds(), 0.0)
                    except (TypeError, ValueError, OverflowError):
                        seconds = -1.0
                if seconds >= 0:
                    return min(seconds, self.retry_policy.max_delay_seconds)
        exponential = self.retry_policy.base_delay_seconds * (2.0 ** (attempt - 1))
        return min(exponential, self.retry_policy.max_delay_seconds)

    @staticmethod
    def _log_retry(
        method: str,
        endpoint: str,
        status_code: int | None,
        attempt: int,
        delay: float,
        rate_limits: SalesforceRateLimits,
    ) -> None:
        logger.warning(
            "salesforce_request_retrying",
            extra={
                "sf_event": {
                    "method": method,
                    "endpoint": endpoint,
                    "status_code": status_code,
                    "attempt": attempt,
                    "delay_seconds": delay,
                    "rate_limits": [
                        {
                            "name": window.name,
                            "used": window.used,
                            "limit": window.limit,
                        }
                        for window in rate_limits.windows
                    ],
                }
            },
        )

    @staticmethod
    def _request_id(headers: httpx.Headers) -> str | None:
        for name in ("X-SFDC-Request-ID", "Sforce-Request-Id", "X-Request-ID"):
            value = cast(str | None, headers.get(name))
            if value:
                return value[:256]
        return None

    @staticmethod
    def _safe_error_codes(response: httpx.Response) -> tuple[str, ...]:
        try:
            payload = response.json()
        except ValueError:
            return ()
        candidates: list[object] = []
        if isinstance(payload, dict):
            candidates.extend((payload.get("error"), payload.get("errorCode")))
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    candidates.extend((item.get("error"), item.get("errorCode")))
        return tuple(
            dict.fromkeys(
                candidate
                for candidate in candidates
                if isinstance(candidate, str) and _SAFE_ERROR_CODE_RE.fullmatch(candidate)
            )
        )

    @staticmethod
    def _json_object(response: httpx.Response, *, context: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            raise SalesforceProtocolError(f"{context} response was not valid JSON") from None
        if not isinstance(payload, dict):
            raise SalesforceProtocolError(f"{context} response was not a JSON object")
        return cast(dict[str, Any], payload)

    @staticmethod
    def _parse_csv_records(payload: str) -> list[dict[str, str]]:
        if not payload.strip():
            return []
        reader = csv.DictReader(StringIO(payload.lstrip("\ufeff"), newline=""))
        if reader.fieldnames is None or any(name is None or not name for name in reader.fieldnames):
            raise SalesforceProtocolError("Bulk query CSV response omitted a valid header")
        parsed: list[dict[str, str]] = []
        for row in reader:
            if None in row:
                raise SalesforceProtocolError("Bulk query CSV row had more values than headers")
            parsed.append({key: value if value is not None else "" for key, value in row.items()})
        return parsed

    @staticmethod
    def _validate_object_name(object_name: str) -> None:
        if not _IDENTIFIER_RE.fullmatch(object_name):
            raise ValueError(f"invalid Salesforce object name: {object_name!r}")

    @staticmethod
    def _resolve_api_url(instance_url: str, path_or_url: str) -> str:
        base = urlsplit(instance_url)
        candidate = urlsplit(path_or_url)
        if candidate.scheme or candidate.netloc:
            if (
                candidate.scheme != base.scheme
                or candidate.hostname != base.hostname
                or candidate.port != base.port
            ):
                raise SalesforceProtocolError("Salesforce pagination URL changed origin")
            return path_or_url
        if not path_or_url.startswith("/"):
            raise SalesforceProtocolError("Salesforce API path must start with a slash")
        return urljoin(f"{instance_url}/", path_or_url.lstrip("/"))
