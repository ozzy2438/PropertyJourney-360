# GCP foundation

This root module creates the low-cost production foundation: required APIs, a versioned and
checksummed raw GCS bucket with minimum retention, five BigQuery layers, at-least-once Pub/Sub with
a retained dead-letter subscription, Secret Manager
containers without values, an immutable Artifact Registry repository, optional private Cloud Run,
monitoring alerts, an optional billing budget, purpose-specific service accounts, and GitHub OIDC.

Terraform is constrained to `>= 1.15.9, < 1.16.0`; `hashicorp/google` is exactly `7.45.0`.

## Trust bootstrap

The first apply is an owner action because GitHub cannot federate until the pool and deployment
service account exist. Use Application Default Credentials from an authorised workstation; never
create or download a service-account JSON key.

1. Create a private, versioned, uniform-access GCS bucket for Terraform state outside this module.
2. Copy `terraform.tfvars.example` to a gitignored path outside the repository and set the project.
3. Run `terraform init -backend-config=bucket=STATE_BUCKET -backend-config=prefix=pj360/dev`.
4. Review a saved plan, then apply only with explicit owner approval.
5. Put the provider and deployment-service-account outputs into the protected GitHub `production`
Environment. Configure required reviewers before enabling authenticated workflows.

The Terraform workflow never recreates a plan during an apply. An apply requires `APPLY` plus the
successful plan workflow run ID, verifies that run came from this workflow on `main`, checks out its
exact head SHA, downloads its retained plan artifact and applies that file. A stale remote-state
serial causes Terraform to reject the saved plan.

The OIDC provider accepts only the immutable repository ID `1344424735`, immutable owner ID
`165224451`, `refs/heads/main`, and the `production` GitHub Environment. GitHub workflows also
require the initiating actor to be the repository owner for apply/deploy.

## Two-stage API deployment

Keep `deploy_api=false` for the bootstrap apply so Artifact Registry and the API token secret exist
first. Add the bearer token as an out-of-band secret version and set its pinned numeric version.
The manual API
workflow builds and pushes a commit-addressed image, captures its registry digest, plans
`deploy_api=true`, and applies the private Cloud Run service. No unauthenticated invoker is created.

Secret resources intentionally contain metadata only. Add values out of band with Secret Manager;
the API runtime alone receives accessor permission for `api-bearer-token`. The Compose stack is a
local development/test environment and must not be treated as a production Airflow deployment.
