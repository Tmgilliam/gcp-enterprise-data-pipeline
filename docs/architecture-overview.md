# GCP Enterprise Data Pipeline — Architecture Overview

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

**Project:** GCP Enterprise Data Pipeline — BigQuery + Vertex AI  
**Upstream consumer:** [ERP AI Delay Risk](https://github.com/Tmgilliam/erp-ai-delay-risk) (Cloud Run inference service)  
**Companion project:** [GCP Microservices + Apigee](https://github.com/Tmgilliam/gcp-microservices-api-gateway) (Phase 7 service decomposition)  
**Positioning:** Platform layer above individual services — proves GCP data platform architecture depth

---

## Executive Summary

ERP operational data lives in transactional systems optimized for OLTP, not analytics or ML. Without a dedicated data platform layer, ML models train on stale CSV exports and production APIs score against features that drift from the training distribution.

This architecture designs the **missing layer** between raw ERP data and production ML models. It ingests ERP transactions, inventory events, and order lifecycle signals; transforms them through governed BigQuery datasets; materializes ML features in Vertex AI Feature Store; and orchestrates model training through Vertex AI Pipelines.

**Phase 1 of ERP AI Delay Risk** trained locally on exported data and deployed to Cloud Run. **This project is Phase 2+ infrastructure** — the enterprise-scale data platform that feeds the same delay risk model at production cadence.

| Layer | GCP Service | Azure Equivalent | AWS Equivalent |
|-------|-------------|------------------|----------------|
| Raw landing | GCS | Blob Storage | S3 |
| Real-time events | Pub/Sub | Event Hubs | Kinesis |
| ETL | Dataflow (Beam) | Data Factory + Stream Analytics | Glue + Kinesis Analytics |
| Warehouse | BigQuery | Synapse Analytics | Redshift |
| Feature store | Vertex AI Feature Store | Azure ML Feature Store | SageMaker Feature Store |
| ML orchestration | Vertex AI Pipelines | Azure ML Pipelines | SageMaker Pipelines |
| Model serving | Vertex AI Endpoints + Cloud Run | Azure ML Endpoints + Container Apps | SageMaker Endpoints + Lambda/ECS |

---

## Business Context

### Problem Statement

Healthcare manufacturing ERP environments generate high-value operational signals — lead time variance, inventory coverage, supplier reliability, order backlog — that remain trapped in transactional schemas. Data teams export CSVs for ad-hoc analysis. ML engineers train on snapshots that age before deployment. Production APIs score against features computed at request time with no lineage back to training data.

### Solution

A medallion-style data pipeline on GCP that:

1. Lands ERP data in governed GCS zones with lifecycle policies
2. Ingests real-time events via Pub/Sub for operational freshness
3. Transforms data through BigQuery dataset layers (raw → staging → analytics → ml_features)
4. Materializes features in Vertex AI Feature Store for online and offline serving
5. Orchestrates model retraining via Vertex AI Pipelines with full artifact lineage
6. Feeds the **ERP AI Delay Risk** Cloud Run API with consistent, versioned features

### Connection to ERP AI Delay Risk

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    GCP ENTERPRISE DATA PIPELINE (this project)          │
│  GCS → Dataflow → BigQuery → Feature Store → Vertex AI Pipelines        │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ features + model artifacts
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ERP AI DELAY RISK (production deployment)            │
│  Cloud Run FastAPI → POST /predict → delay risk score per open order    │
└─────────────────────────────────────────────────────────────────────────┘
```

| Phase | Training | Serving | Data Source |
|-------|----------|---------|-------------|
| Phase 1 | Local scikit-learn on CSV export | Cloud Run container | Manual export |
| Phase 2+ | Vertex AI Pipelines (this project) | Cloud Run + Feature Store lookup | Automated pipeline |

The delay risk model features — demand signals, lead time distributions, inventory coverage, supplier risk scores — are computed in `ml_features.sku_risk_features` and registered in Vertex AI Feature Store entity types (`SKU`, `Vendor`, `Order`, `Customer`).

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LAYER                                    │
├──────────────────────────────┬───────────────────────────────────────────────┤
│  GCS Landing Zone            │  Cloud Pub/Sub                                │
│  raw/ staging/ processed/    │  erp-transactions                             │
│  archive/                    │  inventory-events                             │
│  Lifecycle: Standard →       │  order-events                                 │
│  Nearline (30d) →            │  Dead letter topic + push subscription        │
│  Coldline (90d) → delete     │  → Cloud Functions (lightweight processing)   │
│  (365d)                      │                                               │
└──────────────┬───────────────┴──────────────────────┬────────────────────────┘
               │                                      │
               ▼                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION LAYER                                  │
├──────────────────────────────┬───────────────────────────────────────────────┤
│  Cloud Dataflow (Apache Beam)│  BigQuery                                     │
│  Batch: GCS raw → BQ staging │  Datasets: raw, staging, analytics,           │
│  Streaming: Pub/Sub → BQ     │            ml_features                        │
│  Shared pipeline code        │  Partitioned tables (date)                    │
│  (Beam handles both modes)   │  Clustered tables (SKU, vendor_id)            │
│                              │  Views as transformation layer                │
└──────────────┬───────────────┴──────────────────────┬────────────────────────┘
               │                                      │
               ▼                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ML FEATURE LAYER                                      │
│  Vertex AI Feature Store                                                     │
│  Entity types: SKU, Vendor, Order, Customer                                  │
│  Features: demand_7d/30d/90d, lead_time_p50/p90, stockout_flag, risk_score  │
│  Online serving: low-latency lookup for Cloud Run /predict                   │
│  Offline serving: batch export for Vertex AI Pipelines training              │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ML PLATFORM LAYER                                     │
│  Vertex AI Pipelines (Kubeflow)                                              │
│  data_validation → feature_engineering → training → evaluation →             │
│  registration → deployment                                                   │
│  Vertex AI Model Registry (versioned artifacts)                              │
│  Vertex AI Endpoints (online prediction)                                     │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                         │
│  ERP AI Delay Risk — Cloud Run FastAPI                                       │
│  Real-time scoring with Feature Store online lookup                          │
│  Batch scoring via BigQuery ml_features export                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Ingestion Layer

### Google Cloud Storage (GCS) — Raw Landing Zone

GCS implements the **data lake zone pattern** — the same architectural primitive as Azure Data Lake Storage Gen2 and AWS S3 with prefix-based zones.

#### Bucket Structure

```
gs://{project_id}-erp-data-{env}/
├── raw/           # Immutable source files as received
├── staging/       # Pre-validation landing (optional intermediate)
├── processed/     # Successfully ingested files (pipeline checkpoint)
└── archive/       # Long-term retention before lifecycle deletion
```

#### Naming Convention

```
{env}/{source_system}/{date}/{file}

Examples:
  prod/sage100/inventory/2026-06-08/inventory_transactions_20260608.parquet
  prod/sage100/orders/2026-06-08/open_orders_20260608.csv
  prod/wms/events/2026-06-08/pick_confirmations_20260608.json
```

#### Lifecycle Policies

| Age | Storage Class | Rationale |
|-----|---------------|-----------|
| 0–30 days | Standard | Active pipeline reads, low latency |
| 30–90 days | Nearline | Infrequent reprocessing, 50% cost reduction |
| 90–365 days | Coldline | Compliance retention, audit replay |
| 365+ days | Delete | GDPR/retention policy enforcement |

**Azure equivalent:** Blob Storage lifecycle management rules on `raw/`, `processed/`, `archive/` containers.  
**AWS equivalent:** S3 lifecycle policies with Intelligent-Tiering or Glacier transitions.

#### File Metadata

Every uploaded file carries metadata tags:

| Key | Example | Purpose |
|-----|---------|---------|
| `source_system` | `sage100` | Lineage tracking |
| `ingestion_timestamp` | `2026-06-08T14:30:00Z` | Audit trail |
| `schema_version` | `v2.1` | Schema evolution |
| `record_count` | `15420` | Pre-validation check |
| `pipeline_run_id` | `df-20260608-001` | Dataflow job correlation |

Implementation: [`pipeline/ingestion/gcs_loader.py`](../pipeline/ingestion/gcs_loader.py)

---

### Cloud Pub/Sub — Real-Time Event Ingestion

Pub/Sub provides **decoupled event streaming** — the same pattern as Azure Event Hubs and AWS Kinesis Data Streams.

#### Topics

| Topic | Source | Event Types |
|-------|--------|-------------|
| `erp-transactions` | Sage 100 / ERP API | PO creation, receipt, invoice |
| `inventory-events` | WMS (Scanco) | Pick, putaway, cycle count, adjustment |
| `order-events` | Order management | Order placed, shipped, delayed, cancelled |

#### Dead Letter Configuration

Failed messages route to `{topic}-dlq` after 5 delivery attempts with exponential backoff. DLQ messages trigger Cloud Monitoring alerts and are replayed after root-cause resolution.

#### Push Subscription Pattern

Lightweight real-time processing via Cloud Functions push subscriptions:

```
Pub/Sub topic → push subscription → Cloud Function → BigQuery streaming insert
```

Heavy transformations defer to Dataflow streaming pipeline for exactly-once semantics and complex windowing.

**Azure equivalent:** Event Hubs → Azure Functions → Synapse dedicated pool.  
**AWS equivalent:** Kinesis → Lambda → Redshift / S3.

Implementation: [`pipeline/ingestion/pubsub_consumer.py`](../pipeline/ingestion/pubsub_consumer.py)

---

## Transformation Layer

### Cloud Dataflow (Apache Beam)

Dataflow executes **unified batch and streaming ETL** — one codebase, two execution modes. This is the GCP expression of the pattern Azure implements with Data Factory (batch) + Stream Analytics (streaming), and AWS with Glue (batch) + Kinesis Data Analytics (streaming).

#### Batch Pipeline (Daily Schedule)

```
GCS raw/ → read (Parquet/CSV/JSON) → validate schema → deduplicate →
write BigQuery staging.* → update GCS processed/ checkpoint
```

Schedule: Cloud Scheduler triggers Dataflow template at 02:00 UTC daily.

#### Streaming Pipeline (Continuous)

```
Pub/Sub subscription → parse event → enrich with reference data →
windowed aggregation (1-min tumbling) → BigQuery streaming insert
```

#### Shared Beam Pipeline Design

```python
# Conceptual — see pipeline/transformation/dataflow_pipeline.py
with beam.Pipeline(options) as p:
    if options.streaming:
        events = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription)
    else:
        events = p | 'ReadGCS' >> beam.io.ReadFromParquet(gcs_pattern)

    validated = events | 'Validate' >> beam.ParDo(ValidateSchema())
    transformed = validated | 'Transform' >> beam.ParDo(ApplyBusinessRules())
    transformed | 'WriteBQ' >> beam.io.WriteToBigQuery(table_spec)
```

**Why one pipeline:** Beam's `PipelineOptions.streaming` flag switches execution mode. Business logic stays DRY. The architectural principle — separate orchestration from transformation logic — is identical across clouds.

Implementation: [`pipeline/transformation/dataflow_pipeline.py`](../pipeline/transformation/dataflow_pipeline.py)

---

### BigQuery — Data Warehouse and Transformation

BigQuery serves as the **analytical warehouse and SQL transformation engine** — equivalent to Azure Synapse dedicated SQL pools and AWS Redshift.

#### Dataset Structure

| Dataset | Purpose | Retention | Access |
|---------|---------|-----------|--------|
| `raw` | As-landed data, minimal transformation | 90 days | Pipeline SA only |
| `staging` | Cleaned, typed, deduplicated | 180 days | Pipeline SA + analysts (read) |
| `analytics` | Business aggregates, KPIs | Indefinite | Analysts, BI tools |
| `ml_features` | Feature tables for ML training/serving | Indefinite | Pipeline SA + Vertex AI SA |

#### Partitioning and Clustering Strategy

```sql
-- Partition on ingestion_date for cost control (partition pruning)
-- Cluster on sku_id, vendor_id for filter performance
CREATE TABLE staging.inventory_transactions (
  transaction_id STRING,
  sku_id STRING,
  vendor_id STRING,
  quantity INT64,
  transaction_type STRING,
  transaction_timestamp TIMESTAMP,
  ingestion_date DATE,
  _pipeline_run_id STRING,
  _ingested_at TIMESTAMP
)
PARTITION BY ingestion_date
CLUSTER BY sku_id, vendor_id;
```

**Cost control principle:** Partition pruning eliminates full table scans. This applies identically to Synapse distribution/partition keys and Redshift sort/dist keys.

#### Views as Transformation Layer

SQL views between datasets enforce the **medallion pattern** without physical duplication:

```
raw.inventory_transactions_raw (external table over GCS)
  → VIEW staging.v_inventory_transactions_clean
    → VIEW analytics.v_demand_signals_by_sku
      → VIEW ml_features.v_sku_risk_features
```

Implementation: [`pipeline/transformation/bq_transform.py`](../pipeline/transformation/bq_transform.py)

Sample SQL transformations:

- `sql/staging_inventory_transactions.sql`
- `sql/analytics_demand_signals.sql`
- `sql/ml_features_sku_risk.sql`

---

## ML Feature Layer

### Vertex AI Feature Store

The Feature Store is the **centralized feature registry** — the same architectural role as Azure ML Feature Store and SageMaker Feature Store. It solves the #1 production ML failure mode: training-serving skew.

#### Entity Types

| Entity Type | ID Field | Features | Serves |
|-------------|----------|----------|--------|
| `SKU` | `sku_id` | demand_7d, demand_30d, demand_90d, stockout_rate, avg_inventory_days | Delay risk model, demand forecasting |
| `Vendor` | `vendor_id` | lead_time_p50, lead_time_p90, on_time_rate, defect_rate | Supplier risk scoring |
| `Order` | `order_id` | days_since_order, requested_lead_time, backlog_depth | Real-time /predict scoring |
| `Customer` | `customer_id` | order_frequency, avg_order_value, payment_risk_score | Customer-level risk context |

#### Feature Definitions (SKU Example)

| Feature | Type | Source | Refresh |
|---------|------|--------|---------|
| `demand_7d` | DOUBLE | `analytics.demand_signals` | Daily batch |
| `demand_30d` | DOUBLE | `analytics.demand_signals` | Daily batch |
| `lead_time_p90` | DOUBLE | `analytics.vendor_lead_times` | Weekly batch |
| `stockout_flag` | INT64 | `analytics.demand_signals` | Daily batch |
| `delay_risk_score` | DOUBLE | Vertex AI model output | On retrain |

#### Serving Modes

| Mode | Latency | Use Case | Consumer |
|------|---------|----------|----------|
| Online | < 50ms | Real-time API scoring | ERP AI Delay Risk `/predict` |
| Offline | Batch | Model training | Vertex AI Pipelines training component |

**Connection to ERP AI Delay Risk:** The Cloud Run API currently computes features at request time from the ERP payload. With Feature Store online serving, the API fetches pre-computed `SKU` and `Vendor` features by entity ID, ensuring training-serving consistency and reducing API latency.

Implementation: [`pipeline/feature_store/vertex_feature_store.py`](../pipeline/feature_store/vertex_feature_store.py)

---

## ML Platform Layer

### Vertex AI Pipelines (Kubeflow)

Vertex AI Pipelines orchestrates the **ML lifecycle** — equivalent to Azure ML Pipelines and SageMaker Pipelines.

#### Pipeline Stages

```
┌──────────────┐   ┌──────────────────┐   ┌──────────────┐
│    Data      │──▶│    Feature       │──▶│   Training   │
│  Validation  │   │  Engineering     │   │  (sklearn)   │
└──────────────┘   └──────────────────┘   └──────┬───────┘
                                                  │
┌──────────────┐   ┌──────────────────┐   ┌──────▼───────┐
│  Deployment  │◀──│  Registration    │◀──│  Evaluation  │
│  (Endpoint)  │   │  (Model Registry)│   │  (vs baseline)│
└──────────────┘   └──────────────────┘   └──────────────┘
```

| Component | Input | Output | Gate |
|-----------|-------|--------|------|
| Data validation | `ml_features.sku_risk_features` | Validation report artifact | Completeness ≥ 95% |
| Feature engineering | BQ feature table | Engineered feature parquet | Schema match |
| Training | Feature parquet | sklearn model artifact | F1 logged to Experiments |
| Evaluation | Holdout set + production baseline | Metrics comparison | F1 ≥ threshold |
| Registration | Model artifact | Model Registry version | Evaluation passed |
| Deployment | Registered model | Vertex AI Endpoint | Champion model only |

#### Artifact Lineage

Every model version links to:

- Exact BigQuery table snapshot (partition date)
- Pipeline run ID and component outputs
- Training hyperparameters and metrics
- Evaluation comparison against production baseline

This lineage is the enterprise MLOps requirement that separates portfolio projects from Kaggle notebooks.

Implementation: [`ml/training_pipeline.py`](../ml/training_pipeline.py)

---

### Vertex AI Model Registry and Endpoints

| Component | Purpose | Azure Equivalent | AWS Equivalent |
|-----------|---------|------------------|----------------|
| Model Registry | Versioned model store with metadata | Azure ML Model Registry | SageMaker Model Registry |
| Endpoints | Managed online prediction serving | Azure ML Managed Endpoints | SageMaker Endpoints |
| Experiments | Training run tracking and comparison | Azure ML Experiments | SageMaker Experiments |

**Serving architecture:** Vertex AI Endpoint serves batch/online prediction. ERP AI Delay Risk Cloud Run API can call the endpoint for model inference while Feature Store provides feature lookup — separating feature serving from model serving is a deliberate architectural choice.

---

## Infrastructure and Governance

### Terraform IaC

All infrastructure is provisioned via Terraform — [`infrastructure/main.tf`](../infrastructure/main.tf).

Resources:

- GCS buckets with lifecycle policies and versioning
- BigQuery datasets with IAM access controls
- Pub/Sub topics, subscriptions, and dead letter topics
- Service accounts with least-privilege IAM
- Vertex AI Feature Store entity type definitions

### Data Governance

See [`data-governance.md`](data-governance.md) for:

- Data classification (PII handling for customer entities)
- Access control matrix (pipeline SA, analyst, ML SA)
- Retention policies aligned with GCS lifecycle
- Audit logging via Cloud Audit Logs

### Monitoring

| Signal | Tool | Alert Threshold |
|--------|------|-----------------|
| Pipeline job failures | Cloud Monitoring + Dataflow metrics | Any failed job |
| BigQuery slot utilization | INFORMATION_SCHEMA | > 80% sustained |
| Pub/Sub backlog depth | Subscription metrics | > 10,000 unacked |
| Feature freshness | Custom metric (last sync timestamp) | > 25 hours stale |
| Model drift | ERP AI Delay Risk drift monitor | KS-test p < 0.05 |

Dashboard: [`dashboard/app.py`](../dashboard/app.py)

---

## Multi-Cloud Architectural Principles

This project is designed for **platform-transferable patterns**, not GCP lock-in:

1. **Data lake zones** (raw → staging → processed → archive) exist on every cloud as storage prefix/container patterns with lifecycle policies
2. **Medallion architecture** (bronze → silver → gold) maps to BigQuery datasets, Synapse schemas, and Redshift schemas identically
3. **Feature stores** solve training-serving skew on all three platforms — the entity/feature/online-offline serving model is universal
4. **ML pipeline stages** (validate → engineer → train → evaluate → register → deploy) are identical across Vertex AI, Azure ML, and SageMaker
5. **Serving layer separation** (Feature Store for features, Endpoint/Cloud Run for inference) is a platform-agnostic best practice

Full service mapping: [`multi-cloud-comparison.md`](multi-cloud-comparison.md)

---

## Deployment Topology

| Environment | Project | Region | Purpose |
|-------------|---------|--------|---------|
| `dev` | `erp-data-pipeline-dev` | `us-central1` | Development and integration testing |
| `prod` | `erp-data-pipeline-prod` | `us-central1` | Production pipeline serving ERP AI Delay Risk |

### Service Account Permissions (Least Privilege)

| Role | Scope | Pipeline Stage |
|------|-------|----------------|
| `roles/bigquery.dataEditor` | `staging`, `analytics`, `ml_features` datasets | Transformation, feature materialization |
| `roles/storage.objectAdmin` | ERP data bucket | Ingestion, checkpoint management |
| `roles/aiplatform.user` | Vertex AI resources | Feature Store, Pipelines, Endpoints |
| `roles/pubsub.subscriber` | Event topics | Real-time ingestion |

---

## Related Documentation

| Document | Purpose |
|----------|---------|
| [multi-cloud-comparison.md](multi-cloud-comparison.md) | GCP / Azure / AWS service mapping (interview reference) |
| [data-governance.md](data-governance.md) | Classification, access, retention |
| [design-decisions.md](design-decisions.md) | Architecture Decision Records |
| [../portfolio/case-study.md](../portfolio/case-study.md) | Business narrative |
| [../portfolio/interview-talk-track.md](../portfolio/interview-talk-track.md) | Interview preparation |

---

*Dr. Tatianna Gilliam — My production deployment is on GCP. My certifications are on Azure. My architecture thinking is platform-agnostic.*
