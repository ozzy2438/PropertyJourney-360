from __future__ import annotations

from typing import Any

import google.auth
import pytest
from google.oauth2 import service_account

from propertyjourney360.google_auth import impersonated_credentials_from_environment


def test_unconfigured_impersonation_preserves_credential_free_local_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PJ360_INGEST_SERVICE_ACCOUNT", raising=False)
    monkeypatch.setattr(
        google.auth,
        "default",
        lambda **_kwargs: pytest.fail("ADC must not be loaded in local mode"),
    )

    assert impersonated_credentials_from_environment("PJ360_INGEST_SERVICE_ACCOUNT") is None


def test_impersonation_uses_adc_and_one_hour_cloud_platform_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.auth import impersonated_credentials

    target = "pj360-dev-ingest@example.iam.gserviceaccount.com"
    source = object()
    observed: dict[str, Any] = {}
    result = object()
    monkeypatch.setenv("PJ360_INGEST_SERVICE_ACCOUNT", target)

    def fake_default(*, scopes: tuple[str, ...]) -> tuple[object, str]:
        observed["source_scopes"] = scopes
        return source, "example"

    def fake_credentials(**kwargs: Any) -> object:
        observed.update(kwargs)
        return result

    monkeypatch.setattr(google.auth, "default", fake_default)
    monkeypatch.setattr(impersonated_credentials, "Credentials", fake_credentials)

    assert impersonated_credentials_from_environment("PJ360_INGEST_SERVICE_ACCOUNT") is result
    assert observed == {
        "source_scopes": ("https://www.googleapis.com/auth/cloud-platform",),
        "source_credentials": source,
        "target_principal": target,
        "target_scopes": ("https://www.googleapis.com/auth/cloud-platform",),
        "lifetime": 3600,
    }


def test_impersonation_rejects_malformed_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PJ360_INGEST_SERVICE_ACCOUNT", "not-a-service-account")

    with pytest.raises(ValueError, match="service-account email"):
        impersonated_credentials_from_environment("PJ360_INGEST_SERVICE_ACCOUNT")


def test_impersonation_rejects_service_account_key_as_adc_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeServiceAccountKey:
        pass

    monkeypatch.setenv(
        "PJ360_INGEST_SERVICE_ACCOUNT",
        "pj360-dev-ingest@example.iam.gserviceaccount.com",
    )
    monkeypatch.setattr(service_account, "Credentials", FakeServiceAccountKey)
    monkeypatch.setattr(
        google.auth,
        "default",
        lambda **_kwargs: (FakeServiceAccountKey(), "example"),
    )

    with pytest.raises(RuntimeError, match="service-account key credentials"):
        impersonated_credentials_from_environment("PJ360_INGEST_SERVICE_ACCOUNT")
