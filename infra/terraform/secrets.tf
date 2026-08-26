resource "google_secret_manager_secret" "metadata" {
  for_each = var.secret_ids

  project   = var.project_id
  secret_id = each.value
  labels    = local.common_labels

  replication {
    auto {}
  }

  deletion_protection = var.deletion_protection

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_iam_member" "salesforce_ingest" {
  for_each = toset([
    for secret_id in var.secret_ids : secret_id
    if startswith(secret_id, "salesforce-")
  ])

  project   = var.project_id
  secret_id = google_secret_manager_secret.metadata[each.value].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.workload["ingest"].email}"
}

resource "google_secret_manager_secret_iam_member" "api_token" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.metadata["api-bearer-token"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.workload["api"].email}"
}
