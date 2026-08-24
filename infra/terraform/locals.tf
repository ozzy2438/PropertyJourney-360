locals {
  prefix          = "pj360-${var.environment}"
  raw_bucket_name = coalesce(var.raw_bucket_name, "${var.project_id}-pj360-raw")

  common_labels = merge(var.labels, {
    application = "propertyjourney360"
    environment = var.environment
    managed_by  = "terraform"
  })

  required_services = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudbilling.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceusage.googleapis.com",
    "sts.googleapis.com",
    "storage.googleapis.com",
  ])

  dataset_ids = {
    raw        = "pj360_raw_${replace(var.environment, "-", "_")}"
    staging    = "pj360_staging_${replace(var.environment, "-", "_")}"
    core       = "pj360_core_${replace(var.environment, "-", "_")}"
    marts      = "pj360_marts_${replace(var.environment, "-", "_")}"
    governance = "pj360_governance_${replace(var.environment, "-", "_")}"
  }

  service_accounts = {
    ingest = {
      account_id   = "${local.prefix}-ingest"
      display_name = "PropertyJourney source ingestion"
    }
    transform = {
      account_id   = "${local.prefix}-transform"
      display_name = "PropertyJourney dbt transformations"
    }
    event_publisher = {
      account_id   = "${local.prefix}-event-pub"
      display_name = "PropertyJourney event publisher"
    }
    event_consumer = {
      account_id   = "${local.prefix}-event-sub"
      display_name = "PropertyJourney validated event consumer"
    }
    api = {
      account_id   = "${local.prefix}-api"
      display_name = "PropertyJourney private API runtime"
    }
    github_terraform = {
      account_id   = "${local.prefix}-github-tf"
      display_name = "GitHub owner-gated Terraform deployment"
    }
  }

  project_roles_by_service = {
    ingest = toset([
      "roles/bigquery.jobUser",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
    ])
    transform = toset([
      "roles/bigquery.jobUser",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
    ])
    event_publisher = toset([
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
    ])
    event_consumer = toset([
      "roles/bigquery.jobUser",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
    ])
    api = toset([
      "roles/bigquery.jobUser",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
    ])
    github_terraform = toset([
      "roles/artifactregistry.admin",
      "roles/bigquery.admin",
      "roles/iam.serviceAccountAdmin",
      "roles/iam.workloadIdentityPoolAdmin",
      "roles/monitoring.admin",
      "roles/pubsub.admin",
      "roles/resourcemanager.projectIamAdmin",
      "roles/run.admin",
      "roles/secretmanager.admin",
      "roles/serviceusage.serviceUsageAdmin",
      "roles/storage.admin",
    ])
  }

  project_role_bindings = merge([
    for service, roles in local.project_roles_by_service : {
      for role in roles : "${service}-${role}" => {
        service = service
        role    = role
      }
    }
  ]...)

  notification_channels = var.alert_email == null ? [] : [google_monitoring_notification_channel.email[0].name]
}
