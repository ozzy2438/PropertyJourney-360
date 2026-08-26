"""Short-lived Google service-account impersonation for cloud operators."""

from __future__ import annotations

import os
import re
from typing import Any

_CLOUD_PLATFORM_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
_SERVICE_ACCOUNT_EMAIL = re.compile(
    r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]"
    r"\.iam\.gserviceaccount\.com$"
)


def impersonated_credentials_from_environment(variable: str) -> Any | None:
    """Return short-lived credentials for the service account named by ``variable``.

    An unset variable intentionally returns ``None`` so the local, credential-free execution
    path remains unchanged. A configured target always uses ADC only as the source credential;
    service-account key files are neither required nor supported by this helper.
    """

    target_principal = os.getenv(variable, "").strip()
    if not target_principal:
        return None
    if not _SERVICE_ACCOUNT_EMAIL.fullmatch(target_principal):
        raise ValueError(f"{variable} must contain a service-account email address")

    import google.auth
    from google.auth import impersonated_credentials
    from google.oauth2.service_account import Credentials as ServiceAccountKeyCredentials

    source_credentials, _ = google.auth.default(scopes=_CLOUD_PLATFORM_SCOPES)
    if isinstance(source_credentials, ServiceAccountKeyCredentials):
        raise RuntimeError(
            "service-account key credentials are not accepted as an impersonation source; "
            "use user ADC or workload-federated credentials"
        )
    credential_factory: Any = impersonated_credentials.Credentials
    return credential_factory(
        source_credentials=source_credentials,
        target_principal=target_principal,
        target_scopes=_CLOUD_PLATFORM_SCOPES,
        lifetime=3600,
    )
