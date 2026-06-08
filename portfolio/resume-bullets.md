# Resume Bullets: GCP Enterprise Data Pipeline

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

---

## Primary Bullets (choose 2-3)

- Architected and built a GCP enterprise data pipeline (BigQuery, Dataflow, Vertex AI Feature Store, Pub/Sub) that ingests ERP operational data through medallion dataset layers and feeds a production ML model deployed on Cloud Run — bridging the gap between transactional ERP systems and enterprise-scale ML infrastructure

- Designed platform-agnostic data platform patterns (data lake zones, feature stores, ML pipeline stages) with documented GCP/Azure/AWS service mappings — demonstrating multi-cloud architecture depth beyond single-platform certification

- Implemented end-to-end MLOps pipeline on Vertex AI (Kubeflow Pipelines) with 6 gated components (validate → engineer → train → evaluate → register → deploy), artifact lineage, and production baseline comparison for an ERP delay risk classifier

- Provisioned full data platform infrastructure via Terraform (GCS lifecycle policies, BigQuery partitioned datasets, Pub/Sub dead letter routing, least-privilege IAM) with environment-separated dev/prod topology

---

## Supporting Bullets (technical depth)

- Built production GCS ingestion module with schema validation, exponential backoff retry, file-level metadata tracking, and zone-based lifecycle management (raw → processed → archive)

- Authored BigQuery SQL transformation pipeline across 4 dataset layers (raw, staging, analytics, ml_features) with date partitioning, SKU/vendor clustering, and rolling demand window aggregations (7d/30d/90d)

- Integrated Vertex AI Feature Store with 4 entity types (SKU, Vendor, Order, Customer) for online/offline serving — eliminating training-serving skew for production ML API scoring

- Developed Apache Beam pipeline with unified batch (GCS → BigQuery) and streaming (Pub/Sub → BigQuery) execution modes from a single codebase

---

## Context Bullets (for cover letters / LinkedIn)

- This project is the enterprise data platform layer underneath ERP AI Delay Risk — a production GCP deployment that predicts shipment delay risk for open ERP orders. Phase 1 proved the model; this project proves the infrastructure at scale

- Portfolio demonstrates GCP fluency (production Cloud Run deployment + data platform architecture) alongside Azure certifications (AZ-305, AI-102, AZ-104) — positioning for Google Solutions Architect, Apple IS&T, and multi-cloud architect roles

---

## Keyword Alignment

| Target Role | Keywords Covered |
|-------------|-----------------|
| Google Solutions Architect | BigQuery, Dataflow, Vertex AI, Pub/Sub, Terraform, ML pipelines |
| Apple IS&T | GCP data platform, ERP integration, feature stores, MLOps |
| Multi-cloud architect | Platform-agnostic patterns, Azure/AWS mapping, medallion architecture |
| Cloud data engineer | ETL, data lake, streaming, SQL transformations, IaC |
| ML engineer | Feature Store, training pipelines, model evaluation, batch prediction |

---

*Tailor bullet selection to the role. Lead with the bullet most relevant to the job description.*
