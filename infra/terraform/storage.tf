resource "google_storage_bucket" "raw" {
  project                     = var.project_id
  name                        = local.raw_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.common_labels

  versioning {
    enabled = true
  }

  retention_policy {
    retention_period = var.raw_retention_seconds
    is_locked        = false
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      age                   = 30
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age                   = 90
      matches_storage_class = ["NEARLINE"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age                   = 365
      matches_storage_class = ["COLDLINE"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  lifecycle_rule {
    condition {
      age = 2555
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      num_newer_versions         = 3
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_iam_member" "raw_ingest_creator" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.workload["ingest"].email}"
}

resource "google_storage_bucket_iam_member" "raw_ingest_viewer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workload["ingest"].email}"
}

resource "google_storage_bucket_iam_member" "raw_event_consumer_creator" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.workload["event_consumer"].email}"
}

resource "google_storage_bucket_iam_member" "raw_event_consumer_viewer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workload["event_consumer"].email}"
}

resource "google_storage_bucket_iam_member" "raw_transform_viewer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workload["transform"].email}"
}
