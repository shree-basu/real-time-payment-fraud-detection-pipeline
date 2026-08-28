variable "project_id" {
  description = "Target GCP project. Empty is valid for no-op validation only."
  type        = string
  default     = ""
}

variable "region" {
  description = "Dataflow and regional resource location."
  type        = string
  default     = "us-central1"
}

variable "bigquery_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
  default     = "fraud-streaming"
}

variable "deployment_enabled" {
  description = "Cost-safety gate. Defaults false so plan/apply manage zero resources."
  type        = bool
  default     = false
}

variable "deployment_confirmation" {
  description = "Must equal DEPLOY when deployment_enabled is true."
  type        = string
  default     = ""
  validation {
    condition     = !var.deployment_enabled || var.deployment_confirmation == "DEPLOY"
    error_message = "Set deployment_confirmation to DEPLOY only for an intentional cloud deployment."
  }
}

variable "enable_monitoring" {
  description = "Create static Pub/Sub alert policies when deployment is deliberately enabled."
  type        = bool
  default     = false
}

variable "notification_channel_ids" {
  description = "Existing Cloud Monitoring notification channel resource names."
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Labels applied to supported resources."
  type        = map(string)
  default = {
    application = "payment-fraud-detection"
    environment = "reference"
    managed_by  = "terraform"
  }
}
