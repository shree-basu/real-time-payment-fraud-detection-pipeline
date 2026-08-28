terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project      = length(trimspace(var.project_id)) > 0 ? var.project_id : "no-deploy-validation"
  region       = var.region
  access_token = var.deployment_enabled ? null : "cloud-free-static-validation-not-a-credential"
}
