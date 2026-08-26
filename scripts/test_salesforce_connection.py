"""Safe, secret-free smoke test for a Salesforce External Client App's Client Credentials Flow.

Run this locally, on the machine that holds the real `.env` (never inside this repository's CI
or any shared session): it authenticates using `SALESFORCE_CLIENT_ID`/`SALESFORCE_CLIENT_SECRET`,
then reports whether the flow succeeded, which identity Salesforce resolved the request to (the
configured admin "Run As User"), and whether the `api` scope is actually usable — all without ever
printing, logging or persisting the client secret or the bearer access token. Only the connector's
already-sanitized error messages and a small set of non-secret identity fields are printed.

Requires a `.env` (see `.env.example`) with:
    SALESFORCE_CLIENT_ID=...
    SALESFORCE_CLIENT_SECRET=...
    SALESFORCE_LOGIN_URL=https://your-domain.my.salesforce.com
    SALESFORCE_API_VERSION=65.0

Usage:
    uv run python scripts/test_salesforce_connection.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from propertyjourney360.connectors.salesforce import (
    SalesforceConnector,
    SalesforceError,
    SalesforceOAuthConfig,
)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(
            f"missing required environment variable: {name}. "
            "Populate it in the local, gitignored .env file (see .env.example) and re-run."
        )
    return value


def main() -> None:
    load_dotenv()

    oauth = SalesforceOAuthConfig(
        client_id=_require_env("SALESFORCE_CLIENT_ID"),
        client_secret=_require_env("SALESFORCE_CLIENT_SECRET"),
        auth_base_url=_require_env("SALESFORCE_LOGIN_URL"),
        api_version=os.environ.get("SALESFORCE_API_VERSION", "65.0").strip(),
    )

    result: dict[str, object] = {"client_credentials_flow": "not attempted"}
    try:
        with SalesforceConnector(oauth) as connector:
            token = connector.authenticate()
            result["client_credentials_flow"] = "succeeded"
            result["token_type"] = token.token_type
            result["instance_host"] = httpx.URL(token.instance_url).host

            with httpx.Client(timeout=30.0) as identity_client:
                userinfo = identity_client.get(
                    f"{token.instance_url}/services/oauth2/userinfo",
                    headers={"Authorization": f"{token.token_type} {token.access_token}"},
                )
                userinfo.raise_for_status()
            identity = userinfo.json()
            result["run_as_user"] = {
                "user_id": identity.get("user_id"),
                "username": identity.get("preferred_username") or identity.get("email"),
                "organization_id": identity.get("organization_id"),
                "active": identity.get("active"),
            }

            describe = connector.describe("User")
            result["api_scope_check"] = (
                "succeeded: authenticated REST call to /sobjects/User/describe returned a schema, "
                "confirming the `api` scope is granted and usable"
            )
            result["describe_field_count"] = len(describe.get("fields", []))
    except SalesforceError as exc:
        # SalesforceError messages are already sanitized by the connector: they carry only
        # method/endpoint/status/retry metadata, never the client secret or an access token.
        result["client_credentials_flow"] = "failed"
        result["error"] = str(exc)
        print(json.dumps(result, indent=2))
        raise SystemExit(1) from None
    except httpx.HTTPStatusError as exc:
        result["client_credentials_flow"] = "succeeded"
        result["identity_check"] = "failed"
        result["error"] = f"userinfo request failed: HTTP {exc.response.status_code}"
        print(json.dumps(result, indent=2))
        raise SystemExit(1) from None

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
