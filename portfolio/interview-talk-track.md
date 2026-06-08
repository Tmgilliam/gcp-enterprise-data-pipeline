# Interview Talk Track: GCP Enterprise Data Pipeline

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

Three versions for different interviewer contexts. Each connects to ERP AI Delay Risk.

---

## Version 1: "Why GCP?" — For Azure-Focused Interviewers

**Setup:** Interviewer sees Azure certs (AZ-305, AI-102, AZ-104) and asks why you also work on GCP.

**Answer (2 minutes):**

> My certifications are on Azure because that is where I went deep on cloud architecture fundamentals — landing zones, identity, networking, governance. Those patterns transfer.
>
> I chose GCP for my production ML deployment — the ERP AI Delay Risk system — for three specific reasons:
>
> **First, BigQuery.** Serverless warehouse with separation of storage and compute. For an ERP data pipeline where query patterns are unpredictable — ad-hoc analyst queries during the day, batch transformations at night — pay-per-query beats provisioned clusters. The equivalent on Azure is Synapse serverless; on AWS, Athena. But BigQuery's SQL dialect and partitioning model are exceptionally clean for the medallion pattern.
>
> **Second, Vertex AI Feature Store.** Production ML fails when training features differ from serving features. Feature Store solves this with entity-based feature definitions and online/offline serving modes. Azure ML Feature Store and SageMaker Feature Store do the same thing — the pattern is platform-agnostic. I needed it for the delay risk model, and Vertex AI had the most straightforward integration with BigQuery as the feature source.
>
> **Third, Apache Beam on Dataflow.** One pipeline codebase for batch and streaming. My ERP data arrives two ways: nightly CSV exports from Sage 100 (batch) and real-time WMS events (streaming). Beam handles both with a single `PipelineOptions.streaming` flag. On Azure, that is Data Factory plus Stream Analytics — two services. On AWS, Glue plus Kinesis Analytics — two services. Beam's unified model reduced my pipeline complexity.
>
> The GCP Enterprise Data Pipeline project is the infrastructure layer underneath the Cloud Run deployment. Phase 1 proved the model works. This project proves the data platform that makes it work at enterprise scale. And every architecture decision in the project documents the Azure and AWS equivalent — because I am not a GCP specialist. I am a multi-cloud architect who chose GCP for this workload.

**Follow-up they might ask:** "Would you migrate to Azure?"

> The ERP AI Delay Risk project already has a Bicep IaC migration path and Azure Container Apps deployment architecture documented. The data pipeline patterns — lake zones, medallion datasets, feature stores, ML pipeline stages — map directly. Migration is a service mapping exercise, not an architecture redesign.

---

## Version 2: "Tell Me About Your GCP Experience" — For Google / Apple

**Setup:** Interviewer at Google, Apple IS&T, or a GCP-native organization wants depth.

**Answer (3 minutes):**

> I have two GCP projects that demonstrate different layers of the stack.
>
> **Production deployment — ERP AI Delay Risk on Cloud Run.** This is a live AI system that predicts shipment delay risk for open ERP orders. FastAPI inference service, scikit-learn classifier, Streamlit executive dashboard, deployed to Cloud Run. It handles real-time single-order scoring and batch scoring for the open-order book. Phase 1 trained locally; deployed to GCP because Cloud Run's scale-to-zero model fit the inference pattern — bursty usage during planning meetings, idle overnight.
>
> **Data platform — GCP Enterprise Data Pipeline.** This is what I would build next for that same model. It is the infrastructure layer above individual services:
>
> - **GCS data lake** with zone-based prefixes: raw, staging, processed, archive. Lifecycle policies from Standard to Nearline to Coldline. Files land from Sage 100 nightly exports and WMS event streams.
>
> - **Pub/Sub** for real-time events: erp-transactions, inventory-events, order-events. Dead letter topics for failed messages. Push subscriptions to Cloud Functions for lightweight processing; heavy transforms go to Dataflow.
>
> - **Dataflow** running Apache Beam pipelines. Same code for batch (GCS to BigQuery, daily schedule) and streaming (Pub/Sub to BigQuery, continuous). Shared business logic, two execution modes.
>
> - **BigQuery** as the transformation engine. Four datasets: raw, staging, analytics, ml_features. Partitioned on date, clustered on SKU and vendor. SQL views between layers enforce the medallion pattern without physical duplication.
>
> - **Vertex AI Feature Store** with entity types for SKU, Vendor, Order, and Customer. Online serving for the Cloud Run API — sub-50ms feature lookup. Offline serving for the training pipeline. This solves training-serving skew.
>
> - **Vertex AI Pipelines** with six KFP components: validate, engineer, train, evaluate, register, deploy. Evaluation compares against the Phase 1 production baseline. Only champion models deploy.
>
> - **Terraform** for all of it. Least-privilege service accounts, environment separation, commented rationale on every resource.
>
> The connection between the two projects is deliberate. The delay risk model I deployed to Cloud Run in Phase 1 is the same model the data pipeline trains and serves in Phase 2+. The application layer and the data platform layer are separate concerns — and that separation is itself an architectural best practice.
>
> For Apple specifically: IS&T runs on GCP. This project demonstrates the exact data platform patterns — BigQuery warehouse, Pub/Sub event streaming, Vertex AI ML platform — that enterprise GCP environments require.

**Follow-up they might ask:** "How do you handle data governance?"

> Three layers. Classification: customer entity data is PII, inventory and order data is internal. Access control: pipeline service account has write access to staging and ml_features; analysts have read on analytics; the Cloud Run API service account has Feature Store online read only. Retention: GCS lifecycle policies enforce 365-day maximum; BigQuery raw dataset has 90-day default expiration. Audit: Cloud Audit Logs on every data access. Full matrix in `docs/data-governance.md`.

---

## Version 3: Deep Dive — Pipeline Architecture Walkthrough

**Setup:** Technical interview, 45-minute architecture deep dive. Walk through ingestion to model serving.

**Structure:** Layer by layer, 5-7 minutes each. Draw the diagram as you talk.

### Layer 1: Ingestion (5 min)

> ERP data arrives two ways. Batch: nightly CSV and Parquet exports from Sage 100 land in GCS `raw/` zone via the `GCSLoader` module. Files follow the naming convention `{env}/{source_system}/{date}/{file}`. Every upload carries metadata tags — source system, schema version, record count, pipeline run ID.
>
> Before any file enters the transformation pipeline, `validate_file` checks column names, types, and null rates against a schema definition. Files that fail validation stay in `raw/` and trigger an alert. Files that pass get processed and copied to `processed/` as a checkpoint.
>
> Streaming: WMS events and ERP API webhooks publish to Pub/Sub topics. Three topics — erp-transactions, inventory-events, order-events — each with a dead letter topic. Subscriptions feed Dataflow streaming pipelines. Cloud Functions handle lightweight routing on push subscriptions.
>
> **Multi-cloud note:** GCS zones map to Blob Storage containers and S3 prefixes. Pub/Sub maps to Event Hubs and Kinesis. The zone pattern and event streaming pattern are identical.

**Key code reference:** `pipeline/ingestion/gcs_loader.py` — show `upload_file`, `validate_file`, `list_unprocessed`, `move_to_processed`.

### Layer 2: Transformation (7 min)

> Two execution paths converge in BigQuery.
>
> **Batch path:** Cloud Scheduler triggers Dataflow at 02:00 UTC. Beam pipeline reads from GCS `raw/`, validates schema, applies business rules, writes to BigQuery `staging`. After success, `GCSLoader.move_to_processed` checkpoints the file.
>
> **Streaming path:** Dataflow streaming pipeline reads from Pub/Sub subscriptions, parses events, enriches with reference data, applies 1-minute tumbling windows, and inserts to BigQuery via streaming API.
>
> **SQL transformation layer:** `BQTransformer` executes SQL files between dataset layers. Three transformations in this project:
>
> 1. `staging_inventory_transactions.sql` — clean and type-cast raw inventory data, deduplicate, add processing metadata.
> 2. `analytics_demand_signals.sql` — aggregate demand by SKU with 7d/30d/90d rolling windows, stockout flags, days of supply.
> 3. `ml_features_sku_risk.sql` — combine demand, lead time, and inventory signals into the feature table that feeds both Feature Store and the delay risk model.
>
> Every table is partitioned on date and clustered on SKU/vendor for cost control. The `apply_partition_filter` method generates cost-efficient queries for training data extraction.
>
> **Multi-cloud note:** BigQuery datasets map to Synapse schemas and Redshift schemas. The medallion pattern — raw, staging, analytics, ml_features — is the same bronze/silver/gold pattern on every platform.

**Key code reference:** `pipeline/transformation/bq_transform.py` — show `run_transformation`, `validate_row_count`, `run_staging_pipeline`.

### Layer 3: Feature Store (5 min)

> The Feature Store is the contract between training and serving. Without it, the #1 production ML failure mode — training-serving skew — is guaranteed.
>
> Four entity types: SKU, Vendor, Order, Customer. Each maps to a business object in the ERP domain, not a database table. Features are computed properties: demand_7d, lead_time_p90, stockout_flag, delay_risk_score.
>
> **Online serving:** The ERP AI Delay Risk Cloud Run API calls Feature Store for sub-50ms feature lookup by entity ID. Instead of computing features from the ERP payload at request time (Phase 1), the API fetches pre-computed features that match the training distribution exactly.
>
> **Offline serving:** Vertex AI Pipelines training component exports features from BigQuery to parquet. Same feature definitions, same values, guaranteed consistency.
>
> Batch sync runs daily after the `ml_features_sku_risk.sql` transformation completes. Feature freshness is monitored — alert if last sync exceeds 25 hours.
>
> **Multi-cloud note:** Entity/feature/online-offline serving model is identical on Azure ML Feature Store and SageMaker Feature Store.

### Layer 4: ML Platform (7 min)

> Vertex AI Pipelines orchestrates the full lifecycle with six KFP components:
>
> 1. **Data validation** — checks feature table completeness (≥ 95%) and distribution sanity.
> 2. **Feature engineering** — reads from BigQuery, applies additional transformations aligned with ERP AI Delay Risk `src/features.py`.
> 3. **Training** — sklearn RandomForest, same algorithm as Phase 1. Logs metrics to Vertex AI Experiments.
> 4. **Evaluation** — compares F1 against both an absolute threshold (0.75) and the Phase 1 production baseline (0.72). Must beat both.
> 5. **Registration** — uploads to Model Registry only if evaluation passes.
> 6. **Deployment** — deploys to Vertex AI Endpoint only if registered. Champion model only.
>
> Every model version links to the exact BigQuery partition, pipeline run ID, and training metrics. This lineage is the enterprise MLOps requirement.
>
> The pipeline uses `dsl.Condition` gates — validation failure stops the pipeline; evaluation failure prevents registration; registration failure prevents deployment. No manual promotion.
>
> **Multi-cloud note:** The six stages map identically to Azure ML Pipelines and SageMaker Pipelines. The KFP `@dsl.component` syntax transfers to Kubeflow on any platform.

**Key code reference:** `ml/training_pipeline.py` — show the pipeline definition and component chain.

### Layer 5: Infrastructure and Closing (5 min)

> All infrastructure is Terraform. One service account with least-privilege roles: BigQuery Data Editor, Storage Object Admin, Vertex AI User, Pub/Sub Subscriber, Dataflow Worker. Environment separation via `dev` and `prod` variable.
>
> GCS lifecycle: Standard → Nearline (30d) → Coldline (90d) → Delete (365d). BigQuery raw dataset expires at 90 days. Pub/Sub dead letter after 5 attempts.
>
> Monitoring at three levels: infrastructure (pipeline job failures), data (row count validation, feature freshness), model (drift detection connected to ERP AI Delay Risk `drift_monitor.py`).
>
> **Closing framing:**
>
> This project is the infrastructure layer. ERP AI Delay Risk is the application layer. The architecture patterns — lake zones, medallion datasets, feature stores, ML pipeline stages, serving separation — are platform-agnostic. I chose GCP for this workload because BigQuery, Beam, and Vertex AI Feature Store fit the ERP data pattern. I document Azure and AWS equivalents in every architecture decision because that is what multi-cloud architecture means — patterns first, services second.

---

## Quick Reference: Numbers to Know

| Metric | Value | Context |
|--------|-------|---------|
| GCS lifecycle: Nearline | 30 days | Cost optimization threshold |
| GCS lifecycle: Coldline | 90 days | Compliance retention |
| GCS lifecycle: Delete | 365 days | Maximum retention |
| BigQuery raw expiration | 90 days | Staging promotion window |
| Feature completeness gate | 95% | Pipeline validation threshold |
| Model F1 threshold | 0.75 | Absolute minimum for registration |
| Production baseline F1 | 0.72 | ERP AI Delay Risk Phase 1 benchmark |
| Feature freshness alert | 25 hours | Stale feature detection |
| Pub/Sub max delivery attempts | 5 | Dead letter routing threshold |
| Feature Store entity types | 4 | SKU, Vendor, Order, Customer |

---

## Questions to Ask the Interviewer

1. "How does your team handle training-serving skew today — Feature Store, or request-time computation?"
2. "What is your current data lake zone structure — do you use medallion/bronze-silver-gold?"
3. "How do you gate model promotion — manual review, automated evaluation, or champion/challenger?"
4. "Are you standardized on one cloud for data platform, or multi-cloud with platform-specific implementations?"

---

*Dr. Tatianna Gilliam — My production deployment is on GCP. My certifications are on Azure. My architecture thinking is platform-agnostic.*
