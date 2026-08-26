import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from propertyjourney360.api.main import create_app
from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.config import AuthMode, Environment, Settings


def _repo(tmp_path: Path) -> CustomerRepository:
    repository = CustomerRepository(tmp_path / "api.db")
    repository.upsert_customer(
        {
            "customer_id": "customer-1",
            "display_name": "Synthetic Customer",
            "preferred_suburbs": json.dumps(["Melbourne", "Carlton"]),
            "observed_price_band": "700000-900000",
            "identity_rule_version": "identity-v1.0.0",
            "as_of": datetime(2026, 8, 24, tzinfo=UTC).isoformat(),
        }
    )
    repository.add_identity_source(
        {
            "customer_id": "customer-1",
            "source_system": "salesforce",
            "source_record_id": "003-test",
            "normalized_email": "synthetic.customer@example.com",
            "normalized_phone": "+61412345678",
            "match_score": 0.995,
            "match_decision": "AUTO_MATCH",
        }
    )
    return repository


def test_health_and_customer_contract(tmp_path: Path) -> None:
    app = create_app(
        settings=Settings(environment=Environment.TEST, database_path=tmp_path / "api.db"),
        repository=_repo(tmp_path),
    )
    with TestClient(app) as client:
        assert client.get("/live").status_code == 200
        assert client.get("/ready").status_code == 200
        assert client.get("/health").json()["readiness"] == "ok"
        response = client.get("/customers/customer-1")
    assert response.status_code == 200
    assert response.json()["preferred_suburbs"] == ["Melbourne", "Carlton"]


def test_readiness_returns_503_when_repository_dependency_is_unavailable(tmp_path: Path) -> None:
    repository = _repo(tmp_path)
    repository.ready = lambda: False  # type: ignore[method-assign]
    app = create_app(
        settings=Settings(environment=Environment.TEST, database_path=tmp_path / "api.db"),
        repository=repository,
    )
    with TestClient(app) as client:
        assert client.get("/live").status_code == 200
        assert client.get("/ready").status_code == 503
        assert client.get("/health").status_code == 503


def test_identity_sources_are_masked(tmp_path: Path) -> None:
    app = create_app(
        settings=Settings(environment=Environment.TEST, database_path=tmp_path / "api.db"),
        repository=_repo(tmp_path),
    )
    with TestClient(app) as client:
        item = client.get("/customers/customer-1/identity-sources").json()["items"][0]
    assert item["normalized_email"] != "synthetic.customer@example.com"
    assert item["normalized_email"].endswith("@example.com")
    assert item["normalized_phone"].endswith("678")


def test_bearer_auth_and_pagination_bounds(tmp_path: Path) -> None:
    settings = Settings(
        environment=Environment.TEST,
        database_path=tmp_path / "api.db",
        auth_mode=AuthMode.BEARER,
        api_token=SecretStr("test-token"),
    )
    app = create_app(settings=settings, repository=_repo(tmp_path))
    with TestClient(app) as client:
        assert client.get("/customers/customer-1").status_code == 401
        assert (
            client.get(
                "/customers/customer-1",
                headers={"Authorization": "Bearer test-token"},
            ).status_code
            == 200
        )
        assert (
            client.get(
                "/remediation?limit=201",
                headers={"Authorization": "Bearer test-token"},
            ).status_code
            == 422
        )


def test_cloud_environment_rejects_auth_bypass(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cloud environment requires bearer"):
        Settings(environment=Environment.CLOUD, database_path=tmp_path / "api.db")


def test_cloud_environment_requires_bigquery_coordinates() -> None:
    with pytest.raises(ValueError, match="PJ360_PROJECT_ID"):
        Settings(
            environment=Environment.CLOUD,
            auth_mode=AuthMode.BEARER,
            api_token=SecretStr("test-token"),
        )
