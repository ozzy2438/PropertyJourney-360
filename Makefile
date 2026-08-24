SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
TERRAFORM ?= terraform
DBT := $(UV) tool run --from dbt-core==1.12.3 --with dbt-bigquery==1.12.0 dbt
TF_ROOT := infra/terraform
DBT_PROFILES_DIR := .github/dbt

.PHONY: help bootstrap lint format-check typecheck test audit dbt-deps dbt-parse \
	terraform-fmt terraform-validate terraform-init terraform-plan terraform-apply \
	docker-build compose-config compose-up compose-emulators compose-down ci

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "PropertyJourney 360 targets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap: ## Create the exact locked Python environment.
	python3.12 -m pip install --disable-pip-version-check --no-deps uv==0.12.5
	$(UV) sync --locked --all-extras

lint: ## Run Ruff lint checks.
	$(UV) run ruff check .

format-check: ## Verify Python formatting without writing files.
	$(UV) run ruff format --check .

typecheck: ## Run strict mypy checks.
	$(UV) run mypy src

test: ## Run tests with branch coverage and the configured threshold.
	$(UV) run pytest --cov=propertyjourney360 --cov-branch --cov-report=term-missing

audit: ## Audit the locked, installed Python environment.
	$(UV) export --quiet --locked --all-extras --no-emit-project --format requirements.txt \
		--output-file /tmp/propertyjourney360-requirements.txt
	$(UV) run pip-audit --strict --require-hashes \
		--requirement /tmp/propertyjourney360-requirements.txt

dbt-deps: ## Resolve declared dbt packages.
	$(DBT) deps --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR)

dbt-parse: ## Parse every dbt node without making warehouse calls.
	DBT_BIGQUERY_PROJECT=propertyjourney360-ci \
	$(DBT) parse --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR) --target ci --no-partial-parse

terraform-fmt: ## Verify Terraform formatting without writing files.
	$(TERRAFORM) -chdir=$(TF_ROOT) fmt -check -diff -recursive

terraform-validate: ## Initialise providers locally and validate Terraform.
	$(TERRAFORM) -chdir=$(TF_ROOT) init -backend=false -input=false -lockfile=readonly
	$(TERRAFORM) -chdir=$(TF_ROOT) validate

terraform-init: ## Initialise owner-managed remote GCS state.
	@test -n "$(TF_STATE_BUCKET)" || (echo "TF_STATE_BUCKET is required" >&2; exit 2)
	@test -n "$(TF_STATE_PREFIX)" || (echo "TF_STATE_PREFIX is required" >&2; exit 2)
	$(TERRAFORM) -chdir=$(TF_ROOT) init -input=false -lockfile=readonly \
		-backend-config=bucket=$(TF_STATE_BUCKET) -backend-config=prefix=$(TF_STATE_PREFIX)

terraform-plan: ## Save a remote-state-backed plan to infra/terraform/tfplan.
	$(TERRAFORM) -chdir=$(TF_ROOT) plan -input=false -lock-timeout=5m -out=tfplan

terraform-apply: ## Apply only a saved plan after CONFIRM_APPLY=APPLY.
	@test "$(CONFIRM_APPLY)" = "APPLY" || (echo "Set CONFIRM_APPLY=APPLY after owner review" >&2; exit 2)
	@test -f "$(TF_ROOT)/tfplan" || (echo "Run make terraform-plan first" >&2; exit 2)
	$(TERRAFORM) -chdir=$(TF_ROOT) apply -input=false -lock-timeout=5m tfplan

docker-build: ## Build the production API image.
	docker build --tag propertyjourney360-api:local .

compose-config: ## Validate the fully rendered Compose model.
	docker compose config --quiet

compose-up: ## Start PostgreSQL, API, and the minimal Airflow 3 components.
	docker compose up --build --detach postgres api airflow-init airflow-api-server airflow-scheduler airflow-dag-processor

compose-emulators: ## Start the optional Pub/Sub emulator profile.
	docker compose --profile emulators up --detach pubsub-emulator

compose-down: ## Stop local services while retaining named data volumes.
	docker compose down --remove-orphans

ci: lint format-check typecheck test audit dbt-deps dbt-parse terraform-fmt terraform-validate compose-config docker-build ## Run the local equivalent of CI.
