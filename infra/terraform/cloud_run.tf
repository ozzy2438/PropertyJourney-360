resource "google_cloud_run_v2_service" "api" {
  count = var.deploy_api ? 1 : 0

  project             = var.project_id
  location            = var.region
  name                = "${local.prefix}-api"
  description         = "Private trusted Customer 360 data product API"
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = var.deletion_protection
  labels              = local.common_labels

  template {
    service_account                  = google_service_account.workload["api"].email
    timeout                          = "60s"
    max_instance_request_concurrency = 40

    scaling {
      min_instance_count = 0
      max_instance_count = var.cloud_run_max_instances
    }

    containers {
      name  = "api"
      image = var.api_image

      ports {
        name           = "http1"
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "PJ360_ENVIRONMENT"
        value = "cloud"
      }
      env {
        name  = "PJ360_AUTH_MODE"
        value = "bearer"
      }
      env {
        name = "PJ360_API_TOKEN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.metadata["api-bearer-token"].secret_id
            version = var.api_bearer_secret_version
          }
        }
      }
      env {
        name  = "PJ360_LOG_LEVEL"
        value = "INFO"
      }
      env {
        name  = "PJ360_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "PJ360_LOCATION"
        value = var.bigquery_location
      }
      env {
        name  = "PJ360_CORE_DATASET"
        value = google_bigquery_dataset.platform["core"].dataset_id
      }
      env {
        name  = "PJ360_MARTS_DATASET"
        value = google_bigquery_dataset.platform["marts"].dataset_id
      }
      env {
        name  = "PJ360_GOVERNANCE_DATASET"
        value = google_bigquery_dataset.platform["governance"].dataset_id
      }
      env {
        name  = "PJ360_QUERY_MAXIMUM_BYTES_BILLED"
        value = tostring(var.api_query_maximum_bytes_billed)
      }

      startup_probe {
        initial_delay_seconds = 2
        timeout_seconds       = 3
        period_seconds        = 5
        failure_threshold     = 12
        http_get {
          path = "/health"
          port = 8080
        }
      }

      liveness_probe {
        initial_delay_seconds = 10
        timeout_seconds       = 3
        period_seconds        = 30
        failure_threshold     = 3
        http_get {
          path = "/health"
          port = 8080
        }
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_artifact_registry_repository.api,
    google_bigquery_dataset_iam_member.workload,
    google_project_iam_member.workload,
    google_secret_manager_secret_iam_member.api_token,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "invoker" {
  for_each = var.deploy_api ? var.cloud_run_invokers : toset([])

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api[0].name
  role     = "roles/run.invoker"
  member   = each.value
}
