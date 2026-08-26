resource "google_monitoring_notification_channel" "email" {
  count = var.alert_email == null ? 0 : 1

  project      = var.project_id
  display_name = "PropertyJourney operations email"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
  enabled = true

  depends_on = [google_project_service.required]
}

resource "google_monitoring_alert_policy" "pubsub_backlog" {
  project      = var.project_id
  display_name = "${local.prefix}: Pub/Sub event backlog"
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "Undelivered events above ${var.pubsub_backlog_threshold}"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"pubsub_subscription\"",
        "resource.label.subscription_id = \"${google_pubsub_subscription.events.name}\"",
        "metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_backlog_threshold
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  notification_channels = local.notification_channels
  user_labels           = local.common_labels
}

resource "google_monitoring_alert_policy" "pubsub_oldest_message" {
  project      = var.project_id
  display_name = "${local.prefix}: Pub/Sub streaming lag"
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "Oldest unacknowledged event exceeds target"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"pubsub_subscription\"",
        "resource.label.subscription_id = \"${google_pubsub_subscription.events.name}\"",
        "metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_oldest_message_seconds
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  notification_channels = local.notification_channels
  user_labels           = local.common_labels
}

resource "google_monitoring_alert_policy" "cloud_run_errors" {
  count = var.deploy_api ? 1 : 0

  project      = var.project_id
  display_name = "${local.prefix}: Cloud Run API 5xx"
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "API returns server errors"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_revision\"",
        "resource.label.service_name = \"${google_cloud_run_v2_service.api[0].name}\"",
        "metric.type = \"run.googleapis.com/request_count\"",
        "metric.label.response_code_class = \"5xx\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  notification_channels = local.notification_channels
  user_labels           = local.common_labels
}

resource "google_billing_budget" "project" {
  count = var.billing_account_id == null ? 0 : 1

  billing_account = var.billing_account_id
  display_name    = "PropertyJourney 360 ${var.environment} monthly budget"

  budget_filter {
    projects = ["projects/${data.google_project.current.number}"]
  }

  amount {
    specified_amount {
      currency_code = "AUD"
      units         = tostring(var.monthly_budget_aud)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }

  all_updates_rule {
    monitoring_notification_channels = local.notification_channels
    disable_default_iam_recipients   = false
  }

  depends_on = [google_project_service.required]
}
