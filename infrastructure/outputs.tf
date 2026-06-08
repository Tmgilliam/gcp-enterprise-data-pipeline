# outputs.tf — Exported values for pipeline configuration and CI/CD

output "erp_data_bucket_name" {
  description = "GCS bucket for ERP data lake zones (raw, staging, processed, archive)"
  value       = google_storage_bucket.erp_data.name
}

output "erp_data_bucket_url" {
  description = "GCS bucket URL for pipeline configuration"
  value       = google_storage_bucket.erp_data.url
}

output "bigquery_dataset_ids" {
  description = "BigQuery dataset IDs for each transformation layer"
  value = {
    raw         = google_bigquery_dataset.raw.dataset_id
    staging     = google_bigquery_dataset.staging.dataset_id
    analytics   = google_bigquery_dataset.analytics.dataset_id
    ml_features = google_bigquery_dataset.ml_features.dataset_id
  }
}

output "pubsub_topic_ids" {
  description = "Pub/Sub topic IDs for real-time event ingestion"
  value = {
    erp_transactions  = google_pubsub_topic.erp_transactions.id
    inventory_events  = google_pubsub_topic.inventory_events.id
    order_events      = google_pubsub_topic.order_events.id
  }
}

output "pubsub_subscription_ids" {
  description = "Pub/Sub subscription IDs for Dataflow streaming consumers"
  value = {
    erp_transactions  = google_pubsub_subscription.erp_transactions_dataflow.id
    inventory_events  = google_pubsub_subscription.inventory_events_dataflow.id
    order_events      = google_pubsub_subscription.order_events_dataflow.id
  }
}

output "pipeline_service_account_email" {
  description = "Service account email for pipeline workloads (Dataflow, BQ, Vertex AI)"
  value       = google_service_account.pipeline.email
}

output "feature_store_name" {
  description = "Vertex AI Feature Store resource name"
  value       = google_vertex_ai_featurestore.erp_features.name
}

output "feature_store_id" {
  description = "Vertex AI Feature Store instance ID"
  value       = google_vertex_ai_featurestore.erp_features.id
}

output "entity_type_ids" {
  description = "Feature Store entity type IDs for ERP domain entities"
  value = {
    sku      = google_vertex_ai_featurestore_entitytype.sku.id
    vendor   = google_vertex_ai_featurestore_entitytype.vendor.id
    order    = google_vertex_ai_featurestore_entitytype.order.id
    customer = google_vertex_ai_featurestore_entitytype.customer.id
  }
}

output "environment" {
  description = "Deployed environment"
  value       = var.environment
}

output "region" {
  description = "Deployed region"
  value       = var.region
}
