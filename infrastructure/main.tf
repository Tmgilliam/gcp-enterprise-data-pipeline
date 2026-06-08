# main.tf — GCP Enterprise Data Pipeline core infrastructure
#
# Provisions the data platform layer for ERP operational data:
#   GCS landing zones → Pub/Sub events → BigQuery datasets → Feature Store
#
# Serves: pipeline ingestion, transformation, ML feature materialization,
#         and Vertex AI training for ERP AI Delay Risk model.
#
# Multi-cloud equivalents documented in docs/multi-cloud-comparison.md

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  name_prefix = "erp-data-${var.environment}"
  common_labels = merge(var.labels, {
    environment = var.environment
  })
}

# =============================================================================
# GCS — Data Lake Landing Zone
# Azure equivalent: Blob Storage with lifecycle management
# AWS equivalent: S3 with lifecycle policies
# Pipeline stage: Ingestion (raw → staging → processed → archive)
# =============================================================================

resource "google_storage_bucket" "erp_data" {
  # Single bucket with prefix-based zones: raw/, staging/, processed/, archive/
  # Naming convention: {env}/{source_system}/{date}/{file}
  name     = "${var.project_id}-erp-data-${var.environment}"
  location = var.region
  project  = var.project_id

  # Versioning enables point-in-time recovery for pipeline checkpoint files
  versioning {
    enabled = true
  }

  # Uniform bucket-level access simplifies IAM (no per-object ACLs)
  uniform_bucket_level_access = true

  # Lifecycle: Standard → Nearline (30d) → Coldline (90d) → Delete (365d)
  # Cost control principle: hot data near compute, cold data in cheaper tiers
  lifecycle_rule {
    condition {
      age = var.nearline_transition_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = var.coldline_transition_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels
}

# =============================================================================
# BigQuery — Data Warehouse Datasets
# Azure equivalent: Synapse dedicated SQL pools / serverless SQL
# AWS equivalent: Redshift / Athena + Glue Catalog
# Pipeline stage: Transformation (raw → staging → analytics → ml_features)
# =============================================================================

resource "google_bigquery_dataset" "raw" {
  # As-landed data from GCS external tables; minimal transformation
  dataset_id = "raw_${var.environment}"
  project    = var.project_id
  location   = var.bigquery_location

  description = "Raw ERP data as landed from GCS. Pipeline SA write-only."

  # Default table expiration: 90 days (raw data ages out after staging promotion)
  default_table_expiration_ms = 90 * 24 * 60 * 60 * 1000

  labels = local.common_labels
}

resource "google_bigquery_dataset" "staging" {
  # Cleaned, typed, deduplicated data ready for analytics
  dataset_id = "staging_${var.environment}"
  project    = var.project_id
  location   = var.bigquery_location

  description = "Cleaned and typed ERP data. Source for analytics transformations."

  default_table_expiration_ms = 180 * 24 * 60 * 60 * 1000

  labels = local.common_labels
}

resource "google_bigquery_dataset" "analytics" {
  # Business aggregates, KPIs, demand signals
  dataset_id = "analytics_${var.environment}"
  project    = var.project_id
  location   = var.bigquery_location

  description = "Business analytics aggregates. Demand signals, lead times, KPIs."

  labels = local.common_labels
}

resource "google_bigquery_dataset" "ml_features" {
  # Feature tables for ML training and Feature Store sync
  # Consumer: Vertex AI Pipelines, ERP AI Delay Risk Cloud Run API
  dataset_id = "ml_features_${var.environment}"
  project    = var.project_id
  location   = var.bigquery_location

  description = "ML feature tables. Feeds Vertex AI Feature Store and delay risk model."

  labels = local.common_labels
}

# =============================================================================
# Pub/Sub — Real-Time Event Ingestion
# Azure equivalent: Event Hubs
# AWS equivalent: Kinesis Data Streams
# Pipeline stage: Ingestion (streaming path to Dataflow → BigQuery)
# =============================================================================

resource "google_pubsub_topic" "erp_transactions" {
  # PO creation, receipt, invoice events from Sage 100 / ERP API
  name    = "erp-transactions-${var.environment}"
  project = var.project_id

  labels = local.common_labels
}

resource "google_pubsub_topic" "inventory_events" {
  # WMS events: pick, putaway, cycle count, adjustment
  name    = "inventory-events-${var.environment}"
  project = var.project_id

  labels = local.common_labels
}

resource "google_pubsub_topic" "order_events" {
  # Order lifecycle: placed, shipped, delayed, cancelled
  name    = "order-events-${var.environment}"
  project = var.project_id

  labels = local.common_labels
}

# Dead letter topics — failed messages after max delivery attempts
resource "google_pubsub_topic" "erp_transactions_dlq" {
  name    = "erp-transactions-dlq-${var.environment}"
  project = var.project_id

  labels = merge(local.common_labels, { purpose = "dead-letter" })
}

resource "google_pubsub_topic" "inventory_events_dlq" {
  name    = "inventory-events-dlq-${var.environment}"
  project = var.project_id

  labels = merge(local.common_labels, { purpose = "dead-letter" })
}

resource "google_pubsub_topic" "order_events_dlq" {
  name    = "order-events-dlq-${var.environment}"
  project = var.project_id

  labels = merge(local.common_labels, { purpose = "dead-letter" })
}

# Dataflow streaming subscriptions with dead letter routing
resource "google_pubsub_subscription" "erp_transactions_dataflow" {
  name    = "erp-transactions-dataflow-${var.environment}"
  topic   = google_pubsub_topic.erp_transactions.name
  project = var.project_id

  ack_deadline_seconds = var.pubsub_ack_deadline_seconds

  # Dead letter policy: route to DLQ after max attempts
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.erp_transactions_dlq.id
    max_delivery_attempts = var.pubsub_max_delivery_attempts
  }

  # Exponential backoff for transient failures
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = local.common_labels
}

resource "google_pubsub_subscription" "inventory_events_dataflow" {
  name    = "inventory-events-dataflow-${var.environment}"
  topic   = google_pubsub_topic.inventory_events.name
  project = var.project_id

  ack_deadline_seconds = var.pubsub_ack_deadline_seconds

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.inventory_events_dlq.id
    max_delivery_attempts = var.pubsub_max_delivery_attempts
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = local.common_labels
}

resource "google_pubsub_subscription" "order_events_dataflow" {
  name    = "order-events-dataflow-${var.environment}"
  topic   = google_pubsub_topic.order_events.name
  project = var.project_id

  ack_deadline_seconds = var.pubsub_ack_deadline_seconds

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.order_events_dlq.id
    max_delivery_attempts = var.pubsub_max_delivery_attempts
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = local.common_labels
}

# =============================================================================
# Service Account — Pipeline Workload Identity
# Least-privilege IAM for Dataflow, BigQuery, GCS, Vertex AI
# =============================================================================

resource "google_service_account" "pipeline" {
  account_id   = "erp-pipeline-${var.environment}"
  display_name = "ERP Data Pipeline SA (${var.environment})"
  project      = var.project_id
  description  = "Service account for data pipeline workloads. Least-privilege access."
}

# BigQuery Data Editor — write to staging, analytics, ml_features
resource "google_project_iam_member" "pipeline_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery Job User — execute transformation queries
resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Storage Object Admin — GCS landing zone read/write/archive
resource "google_storage_bucket_iam_member" "pipeline_storage_admin" {
  bucket = google_storage_bucket.erp_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# Vertex AI User — Feature Store, Pipelines, Model Registry, Endpoints
resource "google_project_iam_member" "pipeline_vertex_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Pub/Sub Subscriber — consume streaming events
resource "google_project_iam_member" "pipeline_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Dataflow Worker — execute Beam pipelines
resource "google_project_iam_member" "pipeline_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Dataset-level access controls for raw dataset (pipeline SA only)
resource "google_bigquery_dataset_iam_member" "pipeline_raw_access" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_staging_access" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_analytics_access" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_ml_features_access" {
  dataset_id = google_bigquery_dataset.ml_features.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# =============================================================================
# Vertex AI Feature Store
# Azure equivalent: Azure ML Feature Store
# AWS equivalent: SageMaker Feature Store
# Pipeline stage: ML Feature Layer (feeds ERP AI Delay Risk /predict)
# =============================================================================

resource "google_vertex_ai_featurestore" "erp_features" {
  name     = var.feature_store_id
  project  = var.project_id
  region   = var.region
  labels   = local.common_labels

  online_serving_config {
    # Fixed node count for predictable online serving latency
    fixed_node_count = var.environment == "prod" ? 2 : 1
  }
}

# Entity Type: SKU — demand signals, inventory coverage, risk scores
resource "google_vertex_ai_featurestore_entitytype" "sku" {
  name         = "sku"
  featurestore = google_vertex_ai_featurestore.erp_features.id
  description  = "SKU entity for demand signals and supply risk features"
  labels       = local.common_labels
}

resource "google_vertex_ai_featurestore_entitytype" "vendor" {
  name         = "vendor"
  featurestore = google_vertex_ai_featurestore.erp_features.id
  description  = "Vendor entity for lead time distributions and reliability"
  labels       = local.common_labels
}

resource "google_vertex_ai_featurestore_entitytype" "order" {
  name         = "order"
  featurestore = google_vertex_ai_featurestore.erp_features.id
  description  = "Order entity for real-time delay risk scoring"
  labels       = local.common_labels
}

resource "google_vertex_ai_featurestore_entitytype" "customer" {
  name         = "customer"
  featurestore = google_vertex_ai_featurestore.erp_features.id
  description  = "Customer entity for order frequency and payment risk"
  labels       = local.common_labels
}
