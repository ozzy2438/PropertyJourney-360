"""FastAPI application exposing a deliberately small trusted data product."""

import json
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status

from propertyjourney360.api.repository import CustomerReadRepository, CustomerRepository
from propertyjourney360.config import AuthMode, Environment, Settings


def _mask_email(value: str | None) -> str | None:
    if not value or "@" not in value:
        return None
    local, domain = value.split("@", 1)
    visible = local[:1]
    return f"{visible}{'*' * max(3, len(local) - 1)}@{domain}"


def _mask_phone(value: str | None) -> str | None:
    if not value:
        return None
    return f"{'*' * max(0, len(value) - 3)}{value[-3:]}"


def create_app(
    *,
    settings: Settings | None = None,
    repository: CustomerReadRepository | None = None,
) -> FastAPI:
    runtime_settings = settings or Settings()
    if repository is not None:
        runtime_repository = repository
    elif runtime_settings.environment is Environment.CLOUD:
        from propertyjourney360.api.bigquery_repository import BigQueryCustomerRepository

        runtime_repository = BigQueryCustomerRepository(
            project_id=runtime_settings.project_id or "",
            location=runtime_settings.location or "",
            core_dataset=runtime_settings.core_dataset or "",
            marts_dataset=runtime_settings.marts_dataset or "",
            governance_dataset=runtime_settings.governance_dataset or "",
            maximum_bytes_billed=runtime_settings.query_maximum_bytes_billed,
        )
    else:
        runtime_repository = CustomerRepository(runtime_settings.database_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = runtime_settings
        app.state.repository = runtime_repository
        yield

    app = FastAPI(
        title="PropertyJourney 360 Internal Data API",
        version="0.1.0",
        lifespan=lifespan,
    )

    def repo(request: Request) -> CustomerReadRepository:
        return request.app.state.repository  # type: ignore[no-any-return]

    def authorize(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        current: Settings = request.app.state.settings
        if current.auth_mode is AuthMode.LOCAL_DISABLED:
            return
        scheme, _, candidate = (authorization or "").partition(" ")
        expected = current.api_token.get_secret_value() if current.api_token else ""
        if scheme.casefold() != "bearer" or not secrets.compare_digest(candidate, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing or invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.get("/live", tags=["operations"])
    def live() -> dict[str, str]:
        """Process liveness must not depend on an external data service."""
        return {"status": "ok"}

    def readiness_payload(
        repository: Annotated[CustomerReadRepository, Depends(repo)],
    ) -> dict[str, str]:
        if not repository.ready():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="data dependency is unavailable",
            )
        return {"status": "ok", "liveness": "ok", "readiness": "ok"}

    app.get("/ready", tags=["operations"])(readiness_payload)
    app.get("/health", tags=["operations"])(readiness_payload)

    @app.get("/customers/{customer_id}", dependencies=[Depends(authorize)], tags=["customers"])
    def customer(
        customer_id: str,
        repository: Annotated[CustomerReadRepository, Depends(repo)],
    ) -> dict[str, Any]:
        result = repository.get_customer(customer_id)
        if result is None:
            raise HTTPException(status_code=404, detail="customer not found")
        if isinstance(result.get("preferred_suburbs"), str):
            result["preferred_suburbs"] = json.loads(result["preferred_suburbs"])
        return result

    @app.get(
        "/customers/{customer_id}/journey",
        dependencies=[Depends(authorize)],
        tags=["customers"],
    )
    def journey(
        customer_id: str,
        repository: Annotated[CustomerReadRepository, Depends(repo)],
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, Any]:
        if repository.get_customer(customer_id) is None:
            raise HTTPException(status_code=404, detail="customer not found")
        items = repository.list_journey(customer_id, limit=limit, offset=offset)
        return {"items": items, "limit": limit, "offset": offset}

    @app.get(
        "/customers/{customer_id}/identity-sources",
        dependencies=[Depends(authorize)],
        tags=["customers"],
    )
    def identity_sources(
        customer_id: str,
        repository: Annotated[CustomerReadRepository, Depends(repo)],
    ) -> dict[str, Any]:
        if repository.get_customer(customer_id) is None:
            raise HTTPException(status_code=404, detail="customer not found")
        items = repository.list_identity_sources(customer_id)
        for item in items:
            item["normalized_email"] = _mask_email(item.get("normalized_email"))
            item["normalized_phone"] = _mask_phone(item.get("normalized_phone"))
        return {"items": items}

    @app.get("/remediation", dependencies=[Depends(authorize)], tags=["governance"])
    def remediation(
        repository: Annotated[CustomerReadRepository, Depends(repo)],
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, Any]:
        return {
            "items": repository.list_remediation(limit=limit, offset=offset),
            "limit": limit,
            "offset": offset,
        }

    return app


app = create_app()
