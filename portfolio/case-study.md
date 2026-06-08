# Case Study: GCP Enterprise Data Pipeline — BigQuery + Vertex AI

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

---

## The Business Problem

ERP operational data is trapped in transactional systems. Sage 100, WMS platforms, and order management tools are optimized for OLTP — purchase orders, inventory movements, shipment confirmations — not for analytics or machine learning.

The result: data teams export CSVs for ad-hoc analysis. ML engineers train on snapshots that age before deployment. Production APIs compute features at request time with no lineage back to the training distribution. When the model drifts, nobody can trace whether the problem is in the data pipeline, the feature computation, or the model itself.

In healthcare manufacturing, this is not an academic concern. The ERP AI Delay Risk system I deployed to Cloud Run scores open orders for shipment delay probability. Operations leadership acts on those scores in daily planning meetings. If the features feeding that model are stale, computed differently at training time vs. serving time, or derived from incomplete data — the planning decisions are wrong.

**The question this project answers:** Can I design and build the enterprise data platform layer that makes production ML reliable, traceable, and scalable on GCP?

Not a tutorial exercise. Infrastructure that a real ML system runs on.

---

## What This Project Builds

The missing layer between raw ERP data and production ML models.

| Layer | What It Does | GCP Service |
|-------|-------------|-------------|
| Ingestion | Land ERP files and events in governed storage zones | GCS + Pub/Sub |
| Transformation | Clean, aggregate, and feature-engineer through SQL layers | Dataflow + BigQuery |
| Feature Management | Centralized feature registry with online/offline serving | Vertex AI Feature Store |
| ML Lifecycle | Train, evaluate, register, and deploy with artifact lineage | Vertex AI Pipelines |
| Infrastructure | Reproducible, least-privilege, environment-separated | Terraform |

Every layer includes multi-cloud mapping to Azure and AWS equivalents — because the architecture patterns are platform-transferable even when the production deployment is on GCP.

---

## How It Connects to ERP AI Delay Risk

This is the most important relationship in the portfolio.

```
Phase 1 (ERP AI Delay Risk)          Phase 2+ (This Project)
─────────────────────────            ─────────────────────────
Train locally on CSV export    →     Vertex AI Pipelines (automated)
Compute features at API time   →     Feature Store (pre-computed)
Deploy model in container      →     Model Registry + Endpoint
Manual data refresh            →     Daily batch + real-time streaming
No feature lineage             →     Full artifact lineage per model version
```

| Aspect | Phase 1 | This Project |
|--------|---------|-------------|
| Training | `src/train_model.py` on local CSV | `ml/training_pipeline.py` on BigQuery features |
| Features | `src/features.py` at request time | `ml_features.sku_risk_features` + Feature Store |
| Serving | Cloud Run FastAPI `/predict` | Cloud Run + Vertex AI Endpoint |
| Data source | Manual ERP export | Automated GCS landing + Pub/Sub events |
| Infrastructure | Cloud Run deployment | Terraform IaC for full data platform |

The delay risk model is the same. The infrastructure underneath it is what separates a portfolio project from a production system.

The feature table `ml_features.sku_risk_features` computes the same signals the Phase 1 model uses — demand windows, lead time percentiles, stockout flags, supply risk scores — but at pipeline cadence with full lineage. When the Vertex AI Pipeline trains a new model version, every artifact links to the exact BigQuery partition and pipeline run that produced it.

---

## What It Proves

### GCP Data Platform Depth

I already proved I can deploy on GCP — ERP AI Delay Risk runs on Cloud Run in production. This project proves I can architect at the data platform level: GCS data lake zones, BigQuery medallion datasets, Dataflow ETL, Pub/Sub event streaming, and Vertex AI Feature Store + Pipelines.

That is the difference between "I deployed a container" and "I designed a data platform."

### ML Pipeline Architecture

The Vertex AI Pipeline implements the full MLOps lifecycle:

```
data_validation → feature_engineering → training → evaluation →
registration → deployment
```

With gates at each stage: completeness thresholds, F1 score vs. production baseline, champion-only deployment. This is the enterprise pattern — not "train a model and hope."

### Terraform IaC

Every resource — GCS buckets, BigQuery datasets, Pub/Sub topics, service accounts, Feature Store entity types — is provisioned through Terraform with least-privilege IAM, environment separation (dev/prod), and commented rationale for every configuration choice.

### Multi-Cloud Thinking

Every architecture document includes GCP → Azure → AWS service mapping. The [`multi-cloud-comparison.md`](../docs/multi-cloud-comparison.md) document is structured for interview use — layer by layer, with the architectural principle that transcends platform choice.

My Azure certifications (AZ-305, AI-102, AZ-104) prove cloud architecture depth. My GCP production deployment proves I can ship on a second platform. This project proves the architecture thinking is platform-agnostic.

---

## Architecture Highlights

### Data Lake Zones (GCS)

```
raw/ → staging/ → processed/ → archive/
```

Lifecycle policies: Standard → Nearline (30d) → Coldline (90d) → Delete (365d). Naming convention: `{env}/{source_system}/{date}/{file}`.

### BigQuery Medallion Datasets

```
raw → staging → analytics → ml_features
```

Partitioned on date, clustered on SKU/vendor. SQL views as transformation layer. Three sample transformations included: inventory staging, demand signals, SKU risk features.

### Vertex AI Feature Store

Entity types: `SKU`, `Vendor`, `Order`, `Customer`. Online serving for Cloud Run `/predict`. Offline serving for training pipeline. Solves training-serving skew — the #1 production ML failure mode.

### Vertex AI Training Pipeline

Six KFP components with real `@dsl.component` syntax. Evaluation compares against ERP AI Delay Risk Phase 1 production baseline. Registration and deployment gated on evaluation pass.

---

## Target Roles

| Role | What This Project Demonstrates |
|------|-------------------------------|
| Google Solutions Architect | GCP data platform fluency beyond Cloud Run |
| Apple IS&T | GCP-native data pipeline (Apple runs on GCP) |
| Multi-cloud architect | Platform-agnostic patterns with GCP production proof |
| Cloud data engineer | End-to-end pipeline: ingestion → warehouse → features → ML |

---

## Related Projects

| Project | Relationship |
|---------|-------------|
| [ERP AI Delay Risk](../../erp-ai-delay-risk/) | Application layer — this project is its data platform |
| [Forecasting and Planning Support](../../forecasting-planning-support/) | Complementary — demand forecasting feeds demand signals |
| [ERP Cloud Integration](../../../OneDrive/Documents/MTP-Projects/erp-cloud-integration/) | Azure-side data integration — multi-cloud portfolio pair |

---

*Dr. Tatianna Gilliam — My production deployment is on GCP. My certifications are on Azure. My architecture thinking is platform-agnostic.*
