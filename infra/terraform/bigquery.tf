resource "google_bigquery_dataset" "platform" {
  for_each = local.dataset_ids

  project       = var.project_id
  dataset_id    = each.value
  friendly_name = "PropertyJourney 360 ${upper(each.key)} (${var.environment})"
  description   = "${upper(each.key)} layer managed by Terraform."
  location      = var.bigquery_location
  labels        = local.common_labels

  delete_contents_on_destroy      = false
  max_time_travel_hours           = 168
  default_table_expiration_ms     = each.key == "raw" ? var.raw_bigquery_table_expiration_days * 86400000 : null
  default_partition_expiration_ms = each.key == "raw" ? 90 * 86400000 : null

  depends_on = [google_project_service.required]
}

locals {
  dataset_access = {
    "raw-ingest-editor" = {
      dataset = "raw"
      role    = "roles/bigquery.dataEditor"
      service = "ingest"
    }
    "raw-event-editor" = {
      dataset = "raw"
      role    = "roles/bigquery.dataEditor"
      service = "event_consumer"
    }
    "raw-transform-viewer" = {
      dataset = "raw"
      role    = "roles/bigquery.dataViewer"
      service = "transform"
    }
    "staging-transform-editor" = {
      dataset = "staging"
      role    = "roles/bigquery.dataEditor"
      service = "transform"
    }
    "core-transform-editor" = {
      dataset = "core"
      role    = "roles/bigquery.dataEditor"
      service = "transform"
    }
    "core-api-viewer" = {
      dataset = "core"
      role    = "roles/bigquery.dataViewer"
      service = "api"
    }
    "marts-transform-editor" = {
      dataset = "marts"
      role    = "roles/bigquery.dataEditor"
      service = "transform"
    }
    "marts-api-viewer" = {
      dataset = "marts"
      role    = "roles/bigquery.dataViewer"
      service = "api"
    }
    "governance-ingest-editor" = {
      dataset = "governance"
      role    = "roles/bigquery.dataEditor"
      service = "ingest"
    }
    "governance-transform-editor" = {
      dataset = "governance"
      role    = "roles/bigquery.dataEditor"
      service = "transform"
    }
    "governance-api-viewer" = {
      dataset = "governance"
      role    = "roles/bigquery.dataViewer"
      service = "api"
    }
  }
}

resource "google_bigquery_dataset_iam_member" "workload" {
  for_each = local.dataset_access

  project    = var.project_id
  dataset_id = google_bigquery_dataset.platform[each.value.dataset].dataset_id
  role       = each.value.role
  member     = "serviceAccount:${google_service_account.workload[each.value.service].email}"
}
