output "raw_bucket_name" {
  description = "Versioned, checksummed raw landing bucket with a minimum retention period."
  value       = google_storage_bucket.raw.name
}

output "bigquery_datasets" {
  description = "Logical layer to physical BigQuery dataset mapping."
  value       = { for layer, dataset in google_bigquery_dataset.platform : layer => dataset.dataset_id }
}

output "pubsub" {
  description = "Event and dead-letter resources."
  value = {
    topic                    = google_pubsub_topic.events.id
    subscription             = google_pubsub_subscription.events.id
    dead_letter_topic        = google_pubsub_topic.dead_letter.id
    dead_letter_subscription = google_pubsub_subscription.dead_letter_review.id
  }
}

output "service_accounts" {
  description = "Purpose-specific service-account emails. No keys are created."
  value       = { for purpose, account in google_service_account.workload : purpose => account.email }
}

output "artifact_registry_repository" {
  description = "Docker repository path without an image name."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.api.repository_id}"
}

output "github_workload_identity_provider" {
  description = "Set this full provider name as the protected GitHub Environment variable GCP_WORKLOAD_IDENTITY_PROVIDER."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "github_terraform_service_account" {
  description = "Set this as protected GitHub Environment variable GCP_TERRAFORM_SERVICE_ACCOUNT."
  value       = google_service_account.workload["github_terraform"].email
}

output "github_image_builder_service_account" {
  description = "Set this as protected GitHub Environment variable GCP_IMAGE_BUILDER_SERVICE_ACCOUNT."
  value       = google_service_account.workload["github_image_builder"].email
}

output "cloud_run_api" {
  description = "Private Cloud Run service metadata; null until deploy_api is enabled."
  value = var.deploy_api ? {
    name = google_cloud_run_v2_service.api[0].name
    uri  = google_cloud_run_v2_service.api[0].uri
  } : null
}
