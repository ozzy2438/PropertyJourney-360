resource "google_artifact_registry_repository" "api" {
  project       = var.project_id
  location      = var.region
  repository_id = "${local.prefix}-containers"
  description   = "Immutable PropertyJourney 360 application images"
  format        = "DOCKER"
  labels        = local.common_labels

  docker_config {
    immutable_tags = true
  }

  cleanup_policies {
    id     = "delete-untagged-after-30-days"
    action = "DELETE"
    condition {
      tag_state  = "UNTAGGED"
      older_than = "2592000s"
    }
  }

  cleanup_policies {
    id     = "keep-twenty-most-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 20
    }
  }

  cleanup_policy_dry_run = false

  depends_on = [google_project_service.required]
}
