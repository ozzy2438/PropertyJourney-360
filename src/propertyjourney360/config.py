"""Runtime configuration with explicit local/cloud security boundaries."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    LOCAL = "local"
    TEST = "test"
    CLOUD = "cloud"


class AuthMode(StrEnum):
    LOCAL_DISABLED = "local_disabled"
    BEARER = "bearer"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PJ360_", env_file=".env", extra="ignore")

    environment: Environment = Environment.LOCAL
    database_path: Path = Path("data/processed/propertyjourney360.db")
    postgres_dsn: SecretStr | None = None
    auth_mode: AuthMode = AuthMode.LOCAL_DISABLED
    api_token: SecretStr | None = None
    log_level: str = "INFO"
    project_id: str | None = None
    location: str | None = None
    core_dataset: str | None = None
    marts_dataset: str | None = None
    governance_dataset: str | None = None
    query_maximum_bytes_billed: int = 1_000_000_000

    @model_validator(mode="after")
    def enforce_cloud_auth(self) -> Settings:
        if self.environment is Environment.CLOUD and self.auth_mode is not AuthMode.BEARER:
            raise ValueError("cloud environment requires bearer authentication")
        if self.auth_mode is AuthMode.BEARER and not self.api_token:
            raise ValueError("bearer authentication requires PJ360_API_TOKEN")
        if self.environment is Environment.CLOUD:
            required = {
                "PJ360_PROJECT_ID": self.project_id,
                "PJ360_LOCATION": self.location,
                "PJ360_CORE_DATASET": self.core_dataset,
                "PJ360_MARTS_DATASET": self.marts_dataset,
                "PJ360_GOVERNANCE_DATASET": self.governance_dataset,
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise ValueError(
                    f"cloud environment missing required settings: {', '.join(missing)}"
                )
        if self.query_maximum_bytes_billed < 1:
            raise ValueError("PJ360_QUERY_MAXIMUM_BYTES_BILLED must be positive")
        return self
