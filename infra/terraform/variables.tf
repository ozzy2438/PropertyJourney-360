variable "project_id" {
  description = "Existing GCP project in which PropertyJourney 360 is deployed."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid GCP project ID."
  }
}

variable "region" {
  description = "Primary GCP region."
  type        = string
  default     = "australia-southeast1"
}

variable "bigquery_location" {
  description = "Location for all BigQuery datasets."
  type        = string
  default     = "australia-southeast1"
}

variable "environment" {
  description = "Short environment identifier used in resource names."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,10}$", var.environment))
    error_message = "environment must be 2-11 lowercase letters, digits, or hyphens."
  }
}

variable "labels" {
  description = "Additional labels merged onto supported resources."
  type        = map(string)
  default     = {}
}

variable "raw_bucket_name" {
  description = "Globally unique raw landing bucket name; null derives it from project_id."
  type        = string
  default     = null
  nullable    = true
}

variable "raw_retention_seconds" {
  description = "Minimum retention for versioned, checksummed raw objects."
  type        = number
  default     = 2592000

  validation {
    condition     = var.raw_retention_seconds >= 604800
    error_message = "raw_retention_seconds must be at least seven days."
  }
}

variable "raw_bigquery_table_expiration_days" {
  description = "Default raw BigQuery table retention in days."
  type        = number
  default     = 365

  validation {
    condition     = var.raw_bigquery_table_expiration_days >= 30
    error_message = "Raw BigQuery tables must be retained for at least 30 days."
  }
}

variable "secret_ids" {
  description = "Secret Manager containers to create. No secret versions or values are managed."
  type        = set(string)
  default = [
    "api-bearer-token",
    "airflow-api-jwt-secret",
    "airflow-fernet-key",
    "salesforce-client-id",
    "salesforce-client-secret",
  ]

  validation {
    condition     = alltrue([for id in var.secret_ids : can(regex("^[a-z][a-z0-9-]{2,253}[a-z0-9]$", id))])
    error_message = "Secret IDs must use lowercase letters, digits, and hyphens."
  }
}

variable "api_bearer_secret_version" {
  description = "Pinned Secret Manager version containing the API bearer token. The owner creates this version out of band before API deployment."
  type        = string
  default     = "1"

  validation {
    condition     = can(regex("^[1-9][0-9]*$", var.api_bearer_secret_version))
    error_message = "api_bearer_secret_version must be a positive numeric secret version, never latest."
  }
}

variable "deploy_api" {
  description = "Create the private Cloud Run API. Keep false until an immutable image exists."
  type        = bool
  default     = false
}

variable "api_image" {
  description = "Artifact Registry image by immutable digest, required when deploy_api is true."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = !var.deploy_api || (
      var.api_image != null && can(regex("@sha256:[a-f0-9]{64}$", var.api_image))
    )
    error_message = "api_image must end in an immutable @sha256 digest when deploy_api is true."
  }
}

variable "cloud_run_invokers" {
  description = "Explicit IAM members allowed to invoke the API; public principals are rejected."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for member in var.cloud_run_invokers :
      !contains(["allUsers", "allAuthenticatedUsers"], member)
    ])
    error_message = "Cloud Run invokers cannot include allUsers or allAuthenticatedUsers."
  }
}

variable "cloud_run_max_instances" {
  description = "Cost and blast-radius limit for the API."
  type        = number
  default     = 3

  validation {
    condition     = var.cloud_run_max_instances >= 1 && var.cloud_run_max_instances <= 10
    error_message = "cloud_run_max_instances must be between 1 and 10."
  }
}

variable "api_query_maximum_bytes_billed" {
  description = "Hard per-query BigQuery byte cap for the private API."
  type        = number
  default     = 1000000000

  validation {
    condition     = var.api_query_maximum_bytes_billed >= 1000000
    error_message = "api_query_maximum_bytes_billed must be at least 1 MB."
  }
}

variable "deletion_protection" {
  description = "Protect stateful/security-sensitive resources from accidental deletion."
  type        = bool
  default     = true
}

variable "github_repository" {
  description = "Repository slug permitted to exchange GitHub OIDC tokens."
  type        = string
  default     = "ozzy2438/PropertyJourney-360"
}

variable "github_repository_id" {
  description = "Immutable GitHub repository ID used to prevent name-reuse attacks."
  type        = string
  default     = "1344424735"
}

variable "github_repository_owner_id" {
  description = "Immutable GitHub owner ID used to prevent owner-name reuse attacks."
  type        = string
  default     = "165224451"
}

variable "github_ref" {
  description = "Only this Git ref can authenticate through the workload identity provider."
  type        = string
  default     = "refs/heads/main"
}

variable "github_environment" {
  description = "Protected GitHub Environment required in the OIDC assertion."
  type        = string
  default     = "production"
}

variable "billing_account_id" {
  description = "Billing account for a scoped budget; null skips budget creation."
  type        = string
  default     = null
  nullable    = true
}

variable "monthly_budget_aud" {
  description = "Monthly project budget in AUD."
  type        = number
  default     = 25

  validation {
    condition     = var.monthly_budget_aud >= 1
    error_message = "monthly_budget_aud must be at least 1."
  }
}

variable "alert_email" {
  description = "Optional operational email notification channel."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.alert_email == null || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be null or a plausible email address."
  }
}

variable "pubsub_backlog_threshold" {
  description = "Undelivered event count that opens a backlog alert."
  type        = number
  default     = 1000
}

variable "pubsub_oldest_message_seconds" {
  description = "Oldest unacknowledged message age that opens a streaming-lag alert."
  type        = number
  default     = 300
}
