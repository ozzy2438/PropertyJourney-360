"""Operational source connectors."""

from propertyjourney360.connectors.salesforce import (
    BulkQueryResult,
    IncrementalQueryResult,
    LimitWindow,
    OAuthToken,
    QueryResult,
    ResponseMetadata,
    RetryPolicy,
    SalesforceAPIError,
    SalesforceConnector,
    SalesforceError,
    SalesforceOAuthConfig,
    SalesforceProtocolError,
    SalesforceRateLimits,
    SalesforceTransportError,
)

__all__ = [
    "BulkQueryResult",
    "IncrementalQueryResult",
    "LimitWindow",
    "OAuthToken",
    "QueryResult",
    "ResponseMetadata",
    "RetryPolicy",
    "SalesforceAPIError",
    "SalesforceConnector",
    "SalesforceError",
    "SalesforceOAuthConfig",
    "SalesforceProtocolError",
    "SalesforceRateLimits",
    "SalesforceTransportError",
]
