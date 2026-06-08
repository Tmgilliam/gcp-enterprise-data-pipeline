# variables.tf — Input variables for GCP Enterprise Data Pipeline infrastructure

variable "project_id" {
  description = "GCP project ID for all resources"
  type        = string
}

variable "region" {
  description = "GCP region for regional resources (Dataflow, Vertex AI, Pub/Sub)"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev or prod)"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be 'dev' or 'prod'."
  }
}

variable "bigquery_location" {
  description = "BigQuery dataset location (multi-region US or EU)"
  type        = string
  default     = "US"
}

variable "data_retention_days" {
  description = "Days before GCS lifecycle deletes archived files"
  type        = number
  default     = 365
}

variable "nearline_transition_days" {
  description = "Days before GCS objects transition to Nearline storage class"
  type        = number
  default     = 30
}

variable "coldline_transition_days" {
  description = "Days before GCS objects transition to Coldline storage class"
  type        = number
  default     = 90
}

variable "pubsub_ack_deadline_seconds" {
  description = "Pub/Sub subscription acknowledgment deadline"
  type        = number
  default     = 60
}

variable "pubsub_max_delivery_attempts" {
  description = "Max delivery attempts before routing to dead letter topic"
  type        = number
  default     = 5
}

variable "feature_store_id" {
  description = "Vertex AI Feature Store instance ID"
  type        = string
  default     = "erp_feature_store"
}

variable "labels" {
  description = "Common labels applied to all resources"
  type        = map(string)
  default = {
    project     = "gcp-enterprise-data-pipeline"
    owner       = "tatianna-gilliam"
    managed_by  = "terraform"
  }
}
