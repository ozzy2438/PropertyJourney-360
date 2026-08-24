resource "google_pubsub_topic" "events" {
  project                    = var.project_id
  name                       = "${local.prefix}-property-events"
  message_retention_duration = "604800s"
  labels                     = local.common_labels

  depends_on = [google_project_service.required]
}

resource "google_pubsub_topic" "dead_letter" {
  project                    = var.project_id
  name                       = "${local.prefix}-property-events-dlq"
  message_retention_duration = "604800s"
  labels                     = local.common_labels

  depends_on = [google_project_service.required]
}

resource "google_pubsub_subscription" "events" {
  project = var.project_id
  name    = "${local.prefix}-validated-event-consumer"
  topic   = google_pubsub_topic.events.id

  ack_deadline_seconds         = 60
  message_retention_duration   = "604800s"
  retain_acked_messages        = false
  enable_exactly_once_delivery = false

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  labels = local.common_labels
}

resource "google_pubsub_subscription" "dead_letter_review" {
  project = var.project_id
  name    = "${local.prefix}-property-events-dlq-review"
  topic   = google_pubsub_topic.dead_letter.id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }

  labels = local.common_labels
}

resource "google_pubsub_topic_iam_member" "event_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.events.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.workload["event_publisher"].email}"
}

resource "google_pubsub_subscription_iam_member" "event_consumer" {
  project      = var.project_id
  subscription = google_pubsub_subscription.events.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.workload["event_consumer"].email}"
}

resource "google_pubsub_subscription_iam_member" "dead_letter_consumer" {
  project      = var.project_id
  subscription = google_pubsub_subscription.dead_letter_review.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.workload["event_consumer"].email}"
}

resource "google_pubsub_topic_iam_member" "dead_letter_service_agent" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "forwarding_service_agent" {
  project      = var.project_id
  subscription = google_pubsub_subscription.events.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
