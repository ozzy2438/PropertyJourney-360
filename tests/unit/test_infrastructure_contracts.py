from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_event_consumer_can_create_and_read_immutable_landing_objects() -> None:
    storage = (ROOT / "infra" / "terraform" / "storage.tf").read_text(encoding="utf-8")
    assert '"raw_event_consumer_creator"' in storage
    assert '"raw_event_consumer_viewer"' in storage
    assert storage.count('workload["event_consumer"].email') >= 2
    assert 'role   = "roles/storage.objectCreator"' in storage
    assert 'role   = "roles/storage.objectViewer"' in storage


def test_event_consumer_can_publish_application_rejections_to_dlq() -> None:
    messaging = (ROOT / "infra" / "terraform" / "messaging.tf").read_text(encoding="utf-8")
    block = messaging.split('"dead_letter_event_consumer"', maxsplit=1)[1]
    assert 'role    = "roles/pubsub.publisher"' in block
    assert 'workload["event_consumer"].email' in block


def test_airflow_receives_salesforce_configuration_without_embedded_secrets() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    common = compose.split("services:", maxsplit=1)[0]
    for variable in (
        "SALESFORCE_CLIENT_ID",
        "SALESFORCE_CLIENT_SECRET",
        "SALESFORCE_LOGIN_URL",
        "SALESFORCE_API_VERSION",
    ):
        assert f"{variable}: ${{{variable}" in common
    assert "PJ360_SOURCE_TREE_SHA256: ${PJ360_SOURCE_TREE_SHA256:-UNVERIFIED}" in compose
    assert 'LABEL io.propertyjourney.source-tree-sha256="${PJ360_SOURCE_TREE_SHA256}"' in (
        ROOT / "Dockerfile"
    ).read_text(encoding="utf-8")
    assert 'LABEL io.propertyjourney.source-tree-sha256="${PJ360_SOURCE_TREE_SHA256}"' in (
        ROOT / "docker" / "airflow" / "Dockerfile"
    ).read_text(encoding="utf-8")


def test_cloud_compose_uses_read_only_adc_and_purpose_specific_accounts() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    cloud_override = (ROOT / "compose.gcp.yaml").read_text(encoding="utf-8")
    profile = (ROOT / "dbt" / "profiles.yml").read_text(encoding="utf-8")

    for variable in (
        "PJ360_INGEST_SERVICE_ACCOUNT",
        "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT",
        "DBT_BIGQUERY_IMPERSONATE_SERVICE_ACCOUNT",
    ):
        assert f"{variable}: ${{{variable}" in compose
        assert f"{variable}: ${{{variable}:?" in cloud_override
    assert "PJ360_ADC_FILE:?" in cloud_override
    assert "read_only: true" in cloud_override
    assert "application-default-credentials.json" in cloud_override
    assert "impersonate_service_account" in profile
    assert "DBT_BIGQUERY_IMPERSONATE_SERVICE_ACCOUNT" in profile


def test_airflow_and_ci_use_locked_runtime_dependencies() -> None:
    dockerfile = (ROOT / "docker" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert dockerfile.count("uv sync") == 2
    assert dockerfile.count("--locked") == 2
    assert "constraints-3.3.1" not in dockerfile
    assert "docker/airflow/dbt/uv.lock" in workflow
    assert "Audit locked dbt dependencies" in workflow
    assert "uvx --from dbt-core" not in workflow


def test_image_build_uses_repository_scoped_service_account() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-api.yml").read_text(encoding="utf-8")
    locals_tf = (ROOT / "infra" / "terraform" / "locals.tf").read_text(encoding="utf-8")
    registry = (ROOT / "infra" / "terraform" / "artifact_registry.tf").read_text(encoding="utf-8")

    assert "GCP_IMAGE_BUILDER_SERVICE_ACCOUNT" in workflow
    assert "GCP_TERRAFORM_SERVICE_ACCOUNT" not in workflow
    assert "github_image_builder = toset([])" in locals_tf
    assert 'role       = "roles/artifactregistry.writer"' in registry
    assert 'workload["github_image_builder"].email' in registry


def test_github_service_account_impersonation_is_workflow_scoped() -> None:
    wif = (ROOT / "infra" / "terraform" / "wif.tf").read_text(encoding="utf-8")

    assert '"attribute.workflow_ref"        = "assertion.workflow_ref"' in wif
    for condition in (
        "assertion.repository == '${var.github_repository}'",
        "assertion.repository_id == '${var.github_repository_id}'",
        "assertion.repository_owner_id == '${var.github_repository_owner_id}'",
        "assertion.ref == '${var.github_ref}'",
        "assertion.environment == '${var.github_environment}'",
    ):
        assert condition in wif
    assert (
        "/attribute.workflow_ref/${var.github_repository}/.github/workflows/"
        "terraform.yml@${var.github_ref}"
    ) in wif
    assert (
        "/attribute.workflow_ref/${var.github_repository}/.github/workflows/"
        "deploy-api.yml@${var.github_ref}"
    ) in wif
    workload_bindings = wif.split(
        'resource "google_service_account_iam_member" "github_workload_identity"',
        maxsplit=1,
    )[1]
    assert "/attribute.repository_id/" not in workload_bindings
