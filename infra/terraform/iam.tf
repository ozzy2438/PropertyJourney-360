resource "google_service_account" "workload" {
  for_each = local.service_accounts

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = "Managed by Terraform for ${each.key}; do not attach user-managed keys."

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "workload" {
  for_each = local.project_role_bindings

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.workload[each.value.service].email}"
}

resource "google_service_account_iam_member" "github_can_use_api_runtime" {
  service_account_id = google_service_account.workload["api"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workload["github_terraform"].email}"
}
