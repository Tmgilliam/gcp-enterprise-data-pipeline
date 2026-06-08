# Platform-Agnostic Data Pipeline Architecture: GCP, Azure, and AWS Equivalents

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

This document is the **primary interview reference** for this project. It maps every GCP service to its Azure and AWS equivalent and explains the architectural principle that transcends platform choice.

**Upstream consumer:** [ERP AI Delay Risk](../../erp-ai-delay-risk/) — the production Cloud Run inference service that this data platform feeds at enterprise scale.

---

## INGESTION LAYER

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Object storage (data lake) | Cloud Storage (GCS) | Blob Storage / ADLS Gen2 | S3 | GCS uses uniform bucket-level access by default; ADLS Gen2 adds hierarchical namespace for POSIX-like semantics; S3 has the largest ecosystem of third-party integrations |
| Batch file landing | GCS `raw/` prefix zones | Blob containers (`raw`, `processed`, `archive`) | S3 prefixes with lifecycle policies | All three implement the same zone pattern — the principle is immutable raw landing with lifecycle-managed promotion |
| Real-time event streaming | Cloud Pub/Sub | Event Hubs | Kinesis Data Streams | Pub/Sub is fully managed with push/pull; Event Hubs uses partition-based ordering; Kinesis uses shard-based scaling — choose based on ordering requirements and throughput model |
| Dead letter handling | Pub/Sub dead letter topics | Event Hubs capture + Service Bus DLQ | Kinesis → SQS DLQ / Lambda DLQ | The pattern is identical: failed messages route to a separate queue for inspection and replay |
| Lightweight event processing | Cloud Functions (Pub/Sub push) | Azure Functions (Event Hub trigger) | Lambda (Kinesis trigger) | All three support event-driven micro-processing before heavy ETL — use for schema validation and routing, not complex transforms |
| Lifecycle / tiering | GCS lifecycle rules (Standard → Nearline → Coldline) | Blob lifecycle management (Hot → Cool → Archive) | S3 lifecycle (Standard → IA → Glacier) | Cost optimization through automated tiering is a universal data lake principle — hot data near compute, cold data in cheaper tiers |

**Cross-platform principle:** Ingestion separates *how data arrives* (batch files vs. event streams) from *where it lands* (immutable raw zone). Every cloud implements this as storage prefixes or containers with lifecycle policies. The architect's job is designing the zone structure and naming conventions — not choosing the storage vendor.

---

## TRANSFORMATION LAYER

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Batch ETL | Cloud Dataflow (Apache Beam) | Azure Data Factory | AWS Glue | Dataflow runs open-source Beam pipelines; ADF is primarily orchestration with mapping data flows; Glue is Spark-based with catalog integration — Beam's unified batch/streaming model is unique to GCP |
| Streaming ETL | Cloud Dataflow (streaming mode) | Stream Analytics | Kinesis Data Analytics / Glue Streaming | Dataflow uses the same Beam code for batch and streaming; Azure and AWS typically require separate services — the principle of unified transformation logic still applies via shared code repos |
| SQL transformations | BigQuery SQL jobs | Synapse SQL / serverless SQL pools | Redshift SQL / Athena | BigQuery is serverless with separation of storage and compute; Synapse offers dedicated and serverless pools; Redshift requires cluster provisioning — all support the medallion view pattern |
| Pipeline orchestration | Cloud Composer (Airflow) / Cloud Scheduler | Data Factory pipelines | Step Functions + Glue workflows | Scheduling and dependency management is a universal need — the DAG pattern (directed acyclic graph) is identical across Airflow, ADF, and Step Functions |
| Schema validation | Dataflow DoFn + BigQuery constraints | Data Factory data flows + Synapse constraints | Glue Data Quality + Deequ | Pre-ingestion validation prevents garbage-in-garbage-out — implement at the earliest transformation stage regardless of platform |

**Cross-platform principle:** The medallion architecture (bronze → silver → gold, or raw → staging → analytics) is a *pattern*, not a product. SQL views between dataset layers enforce transformations without physical duplication on every platform. The architect designs the layer boundaries and data contracts; the cloud provides the execution engine.

---

## DATA WAREHOUSE

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Analytical warehouse | BigQuery | Synapse Analytics | Redshift | BigQuery: serverless, pay-per-query. Synapse: dedicated pools for predictable performance. Redshift: provisioned clusters with RA3 managed storage |
| Partitioning | BigQuery table partitioning (DATE, TIMESTAMP, INTEGER range) | Synapse table distribution + partition schemes | Redshift distribution keys + sort keys | All platforms support partition pruning for cost control — partition on the column most frequently used in WHERE clauses |
| Clustering / sorting | BigQuery clustering columns | Synapse columnstore indexes | Redshift sort keys | Secondary optimization for filter columns within partitions — reduces bytes scanned |
| External tables | BigQuery external tables over GCS | Synapse external tables over Blob/ADLS | Redshift Spectrum over S3 / Athena | Query data in place without loading — the raw zone stays in object storage, warehouse queries it on demand |
| Cost control | Partition pruning + slot reservations + BI Engine | Workload management + reserved capacity | Concurrency scaling + reserved nodes | The principle is identical: eliminate full table scans, right-size compute, and monitor slot/cluster utilization |
| Dataset / schema organization | BigQuery datasets (`raw`, `staging`, `analytics`, `ml_features`) | Synapse schemas / databases | Redshift schemas | Logical separation of transformation layers with IAM at the dataset/schema level |

**Cross-platform principle:** The data warehouse is the *transformation engine*, not the storage layer. Raw data stays in object storage; the warehouse creates curated views and tables through SQL. Partitioning and clustering are cost-control mechanisms that work identically — partition on date, cluster on filter columns.

---

## ML PLATFORM

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| ML pipeline orchestration | Vertex AI Pipelines (Kubeflow) | Azure ML Pipelines | SageMaker Pipelines | All three use DAG-based pipeline definitions with component isolation — the validate → engineer → train → evaluate → register → deploy stages are identical |
| Experiment tracking | Vertex AI Experiments | Azure ML Experiments | SageMaker Experiments | Training run comparison with hyperparameters and metrics — universal MLOps requirement |
| Model registry | Vertex AI Model Registry | Azure ML Model Registry | SageMaker Model Registry | Versioned model store with metadata, lineage, and approval gates |
| Online prediction | Vertex AI Endpoints | Azure ML Managed Endpoints | SageMaker Endpoints | Managed model serving with autoscaling — separate from the application serving layer (Cloud Run, Container Apps, ECS) |
| Batch prediction | Vertex AI Batch Prediction | Azure ML Batch Endpoints | SageMaker Batch Transform | Score large datasets without real-time latency requirements |
| Model monitoring | Vertex AI Model Monitoring | Azure ML Data Drift / Model Monitor | SageMaker Model Monitor | Drift detection is critical for ERP AI Delay Risk — KS-test on feature distributions, connected to the production scoring history |

**Cross-platform principle:** ML platform services implement the same lifecycle stages regardless of cloud. The pipeline stages (validate → engineer → train → evaluate → register → deploy) are the MLOps contract. Artifact lineage — linking every model version to the exact dataset and pipeline run — is a platform-agnostic enterprise requirement.

---

## FEATURE STORE

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Feature registry | Vertex AI Feature Store | Azure ML Feature Store (managed feature store) | SageMaker Feature Store | All three implement entity → feature → version semantics |
| Entity types | Feature Store EntityTypes (`SKU`, `Vendor`, `Order`, `Customer`) | Feature Store entities | Feature Group | Domain entities map to feature containers — design entities around business objects, not database tables |
| Online serving | Feature Store online store (low-latency lookup) | Online feature store | Online feature store (DynamoDB-backed) | Sub-50ms feature retrieval for real-time API scoring — ERP AI Delay Risk `/predict` endpoint |
| Offline serving | Feature Store offline store (BigQuery export) | Offline store (ADLS/Blob parquet) | Offline store (S3 parquet) | Batch feature export for training pipelines — must match online feature definitions |
| Feature freshness | Batch sync from BigQuery on schedule | Materialization jobs | Feature store ingestion pipeline | Stale features are worse than no features — monitor last-sync timestamp |
| Training-serving skew prevention | Shared feature definitions in Feature Store | Shared feature definitions | Shared feature definitions | The #1 production ML failure mode — Feature Store exists on all three platforms to solve it |

**Cross-platform principle:** A Feature Store is not optional for production ML — it is the contract between training and serving. Entity types represent business objects (SKU, Vendor, Order); features are computed properties of those entities. Online and offline serving modes ensure the same feature definitions feed both the training pipeline and the real-time API. This pattern is identical on GCP, Azure, and AWS.

---

## ORCHESTRATION

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Workflow orchestration | Cloud Composer (managed Airflow) | Azure Data Factory | AWS Step Functions + MWAA | Airflow DAGs are the industry standard — Composer and MWAA are managed Airflow; ADF uses its own pipeline DSL |
| Job scheduling | Cloud Scheduler | Logic Apps / ADF triggers | EventBridge Scheduler | Cron-based scheduling for batch pipelines (daily GCS → BigQuery load) |
| Event-driven triggers | Pub/Sub → Cloud Functions → Dataflow | Event Grid → Functions → Data Factory | EventBridge → Lambda → Glue | Event-driven architecture for real-time path — trigger on file landing or message arrival |
| Infrastructure as Code | Terraform (this project) | Bicep / Terraform | CloudFormation / Terraform | IaC is platform-agnostic — this project's Terraform patterns transfer directly to Azure and AWS providers |
| CI/CD for pipelines | Cloud Build | Azure DevOps / GitHub Actions | CodePipeline / GitHub Actions | Pipeline code (Beam, SQL, KFP) is tested and deployed through CI/CD regardless of cloud |

**Cross-platform principle:** Orchestration separates *when* pipelines run from *what* they do. Batch pipelines run on schedule; streaming pipelines run continuously; ML pipelines run on retrain triggers. The DAG pattern with explicit dependencies and failure handling is universal. Infrastructure as Code (Terraform) makes the entire platform reproducible and reviewable.

---

## MONITORING

| Capability | GCP | Azure | AWS | Key Architectural Difference |
|------------|-----|-------|-----|------------------------------|
| Metrics and alerting | Cloud Monitoring | Azure Monitor | CloudWatch | Pipeline job failures, Pub/Sub backlog depth, BigQuery slot utilization — all platforms expose metrics with alert policies |
| Logging | Cloud Logging | Log Analytics | CloudWatch Logs | Structured logging from pipeline components with correlation IDs |
| Data quality monitoring | BigQuery INFORMATION_SCHEMA + custom checks | Synapse DMVs + Azure Purview | Glue Data Quality + Deequ | Row count validation, schema drift detection, null rate monitoring |
| ML model monitoring | Vertex AI Model Monitoring + custom drift (ERP AI Delay Risk) | Azure ML Data Drift | SageMaker Model Monitor | Feature distribution drift detection — connected to ERP AI Delay Risk `drift_monitor.py` |
| Pipeline health dashboard | Streamlit (`dashboard/app.py`) | Power BI / Grafana | QuickSight / Grafana | Business-facing pipeline health view — not engineer-facing metrics |
| Audit trail | Cloud Audit Logs | Azure Activity Log | CloudTrail | Who accessed what data, when — compliance requirement for healthcare manufacturing data |

**Cross-platform principle:** Monitoring operates at three levels: infrastructure (is the pipeline running?), data (is the data correct?), and model (is the model still accurate?). Each level has distinct metrics and alert thresholds. The ERP AI Delay Risk drift monitor demonstrates model-level monitoring — KS-test on scoring feature distributions — which integrates with this pipeline's feature freshness metrics.

---

## End-to-End Pipeline Mapping

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION                                            │
│  GCP: GCS + Pub/Sub                                                         │
│  Azure: Blob Storage + Event Hubs                                           │
│  AWS: S3 + Kinesis                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                        TRANSFORMATION                                       │
│  GCP: Dataflow (Beam) + BigQuery SQL                                        │
│  Azure: Data Factory + Synapse SQL                                          │
│  AWS: Glue + Redshift SQL                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                        DATA WAREHOUSE                                       │
│  GCP: BigQuery (raw → staging → analytics → ml_features)                    │
│  Azure: Synapse (bronze → silver → gold)                                    │
│  AWS: Redshift / Athena + Glue Catalog                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                        FEATURE STORE                                        │
│  GCP: Vertex AI Feature Store                                               │
│  Azure: Azure ML Feature Store                                              │
│  AWS: SageMaker Feature Store                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                        ML PLATFORM                                          │
│  GCP: Vertex AI Pipelines → Model Registry → Endpoints                      │
│  Azure: Azure ML Pipelines → Model Registry → Managed Endpoints             │
│  AWS: SageMaker Pipelines → Model Registry → Endpoints                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                        SERVING                                              │
│  GCP: Cloud Run (ERP AI Delay Risk) + Vertex AI Endpoint                    │
│  Azure: Container Apps + Azure ML Endpoint                                  │
│  AWS: ECS/Fargate + SageMaker Endpoint                                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Connection to ERP AI Delay Risk

| Component | This Project (Data Platform) | ERP AI Delay Risk (Application) |
|-----------|---------------------------|--------------------------------|
| Feature source | `ml_features.sku_risk_features` + Feature Store | `src/features.py` (request-time computation) |
| Training | Vertex AI Pipelines (this project) | Local scikit-learn (Phase 1) |
| Serving | Vertex AI Endpoint (batch/online) | Cloud Run FastAPI `/predict` |
| Monitoring | Feature freshness + pipeline health | `monitoring/drift_monitor.py` |
| Infrastructure | Terraform (this project) | Cloud Run deployment (Phase 1) |

Phase 1 proved the model works. This project proves the *infrastructure* that makes it work at enterprise scale.

---

## Architect's Framing

My production deployment is on GCP. My certifications are on Azure. My architecture thinking is platform-agnostic.

The patterns — data lake zones, feature stores, ML pipeline stages, serving layer separation — are the same regardless of which cloud logo is on the slide. That is what it means to be a multi-cloud architect.

When I designed this pipeline, I did not ask "what GCP services exist?" I asked "what architectural layers does an ERP ML system need?" — ingestion, transformation, feature management, model lifecycle, serving — and then mapped each layer to the best-fit service on each platform. The GCP implementation in this project is the production proof. The Azure and AWS mappings in this document are the architecture proof.

The ERP AI Delay Risk model I deployed to Cloud Run in Phase 1 is the same model this pipeline trains, evaluates, and serves at enterprise cadence in Phase 2+. The application layer and the data platform layer are separate concerns — and that separation is itself a platform-agnostic architectural principle.

---

*Dr. Tatianna Gilliam — Cloud & AI Architect | AZ-305 | AI-102 | AZ-104*
