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
  raw_asset_fields = {
    raw_salesforce_accounts = [
      "id", "name", "abn", "industry", "billing_street", "billing_city",
      "billing_state", "billing_postal_code", "system_modstamp",
    ]
    raw_salesforce_contacts = [
      "id", "account_id", "first_name", "last_name", "email", "email_verified",
      "mobile_phone", "mailing_street", "mailing_city", "mailing_state",
      "mailing_postal_code", "system_modstamp",
    ]
    raw_salesforce_cases = [
      "id", "contact_id", "account_id", "status", "priority", "subject",
      "created_date", "closed_date", "system_modstamp",
    ]
    raw_legacy_customers = [
      "customer_id", "account_id", "first_name", "last_name", "email", "email_verified",
      "phone", "address_line_1", "suburb", "state", "postcode", "updated_at",
    ]
    raw_support_cases = [
      "case_id", "customer_id", "customer_first_name", "customer_last_name", "customer_email",
      "customer_email_verified", "customer_phone", "status", "priority", "subject", "opened_at",
      "closed_at", "updated_at",
    ]
    raw_marketing_consents = [
      "consent_event_id", "subject_source_system", "subject_source_id", "channel",
      "consent_status", "policy_version", "capture_source", "occurred_at",
    ]
    raw_property_events = [
      "event_id", "event_type", "session_id", "anonymous_id", "customer_id",
      "customer_source_system", "customer_source_id", "property_id", "price_observed",
      "device_type", "attributes", "schema_version", "occurred_at", "received_at",
    ]
    raw_property_master = [
      "property_id", "address_id", "address_line_1", "suburb", "state", "postcode",
      "property_type", "latitude", "longitude", "geometry_wkt", "source_dataset",
      "source_updated_at", "source_retrieved_at", "source_payload_sha256",
      "source_payload_object",
    ]
    raw_property_market = [
      "geography_code", "geography_type", "period_start", "property_type", "median_price",
      "sales_volume", "source_dataset", "source_retrieved_at", "source_payload_sha256",
      "source_payload_object",
    ]
    raw_abs_demographics = [
      "geography_code", "geography_type", "reference_period", "population",
      "median_household_income_weekly", "owner_occupied_dwellings", "rented_dwellings",
      "source_dataset", "source_retrieved_at", "source_payload_sha256",
      "source_payload_object",
    ]
    raw_rba_indicators = [
      "series_id", "series_name", "observation_date", "observation_value", "unit",
      "source_retrieved_at", "source_payload_sha256", "source_payload_object",
    ]
  }

  raw_metadata_fields = ["_ingested_at", "_window_id", "_source_run_id", "_schema_version"]
  raw_field_types = {
    email_verified                 = "BOOLEAN"
    customer_email_verified        = "BOOLEAN"
    schema_version                 = "INTEGER"
    price_observed                 = "NUMERIC"
    attributes                     = "JSON"
    latitude                       = "FLOAT"
    longitude                      = "FLOAT"
    median_price                   = "NUMERIC"
    sales_volume                   = "INTEGER"
    population                     = "INTEGER"
    median_household_income_weekly = "NUMERIC"
    owner_occupied_dwellings       = "INTEGER"
    rented_dwellings               = "INTEGER"
    observation_value              = "NUMERIC"
    system_modstamp                = "TIMESTAMP"
    created_date                   = "TIMESTAMP"
    closed_date                    = "TIMESTAMP"
    updated_at                     = "TIMESTAMP"
    opened_at                      = "TIMESTAMP"
    occurred_at                    = "TIMESTAMP"
    received_at                    = "TIMESTAMP"
    source_updated_at              = "TIMESTAMP"
    source_retrieved_at            = "TIMESTAMP"
    _ingested_at                   = "TIMESTAMP"
  }

  raw_table_schemas = {
    for table_name, fields in local.raw_asset_fields : table_name => [
      for field_name in concat(fields, local.raw_metadata_fields) : {
        name = field_name
        type = lookup(local.raw_field_types, field_name, "STRING")
        mode = "NULLABLE"
      }
    ]
  }
}

resource "google_bigquery_table" "raw_asset" {
  for_each = local.raw_table_schemas

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.platform["raw"].dataset_id
  table_id            = each.key
  description         = "Versioned append-only RAW contract for ${each.key}."
  deletion_protection = var.deletion_protection
  labels              = local.common_labels
  schema              = jsonencode(each.value)
  clustering          = ["_window_id"]

  time_partitioning {
    type  = "DAY"
    field = "_ingested_at"
  }
}

resource "google_bigquery_table" "remediation_event" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.platform["governance"].dataset_id
  table_id            = "remediation_event"
  description         = "Append-only controlled remediation state transitions."
  deletion_protection = var.deletion_protection
  labels              = local.common_labels
  clustering          = ["remediation_case_id", "to_status"]
  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "remediation_case_id", type = "STRING", mode = "REQUIRED" },
    { name = "from_status", type = "STRING", mode = "REQUIRED" },
    { name = "to_status", type = "STRING", mode = "REQUIRED" },
    { name = "actor", type = "STRING", mode = "REQUIRED" },
    { name = "note", type = "STRING", mode = "REQUIRED" },
    { name = "occurred_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "resolution_action", type = "STRING", mode = "NULLABLE" },
    { name = "replay_id", type = "STRING", mode = "NULLABLE" },
    { name = "verification_result_id", type = "STRING", mode = "NULLABLE" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "occurred_at"
  }
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
