# GCP Enterprise Data Pipeline — BigQuery + Vertex AI

**Dr. Tatianna Gilliam** | Cloud & AI Architect | AZ-305 | AI-102 | AZ-104

Enterprise data platform for ERP operational data on GCP. Ingests, transforms, and materializes ML features that feed the [ERP AI Delay Risk](../erp-ai-delay-risk/) production model on Cloud Run.

---

## What This Project Proves

| Capability | Evidence |
|------------|----------|
| GCP data platform architecture | GCS lake zones, BigQuery medallion, Dataflow ETL, Pub/Sub streaming |
| ML pipeline orchestration | Vertex AI Pipelines with 6 KFP components and artifact lineage |
| Feature management | Vertex AI Feature Store with online/offline serving |
| Infrastructure as Code | Terraform with least-privilege IAM and environment separation |
| Multi-cloud thinking | GCP/Azure/AWS mapping in every architecture document |

**Phase 1** (ERP AI Delay Risk): trained locally, deployed to Cloud Run.  
**Phase 2+** (this project): enterprise data platform that feeds the same model at scale.

---

## Project Structure

```
gcp-enterprise-data-pipeline/
├── README.md
├── pipeline/
│   ├── ingestion/
│   │   ├── gcs_loader.py           # GCS landing zone loader
│   │   └── pubsub_consumer.py      # Real-time event ingestion
│   ├── transformation/
│   │   ├── bq_transform.py         # BigQuery transformation jobs
│   │   ├── dataflow_pipeline.py    # Apache Beam / Dataflow job
│   │   └── sql/                    # Sample SQL transformations
│   └── feature_store/
│       └── vertex_feature_store.py # Vertex AI Feature Store ops
├── ml/
│   ├── training_pipeline.py        # Vertex AI Pipeline (KFP)
│   ├── model_evaluation.py         # Evaluation and registration
│   └── batch_prediction.py         # Batch prediction job
├── infrastructure/
│   ├── main.tf                     # Terraform IaC
│   ├── variables.tf
│   └── outputs.tf
├── docs/
│   ├── architecture-overview.md
│   ├── multi-cloud-comparison.md   # Interview reference
│   ├── data-governance.md
│   └── design-decisions.md
├── dashboard/
│   └── app.py                      # Streamlit pipeline health view
└── portfolio/
    ├── case-study.md
    ├── interview-talk-track.md
    └── resume-bullets.md
```

---

## Quick Start

### Prerequisites

- GCP project with BigQuery, GCS, Pub/Sub, Vertex AI APIs enabled
- Python 3.11+
- Terraform >= 1.5.0
- Application Default Credentials configured (`gcloud auth application-default login`)

### Deploy Infrastructure

```bash
cd infrastructure
terraform init
terraform plan -var="project_id=YOUR_PROJECT" -var="environment=dev"
terraform apply -var="project_id=YOUR_PROJECT" -var="environment=dev"
```

### Run Transformations

```python
from pipeline.transformation.bq_transform import BQTransformer

transformer = BQTransformer(project_id="YOUR_PROJECT")
results = transformer.run_staging_pipeline()
```

### Compile Training Pipeline

```bash
pip install kfp google-cloud-aiplatform scikit-learn
python ml/training_pipeline.py --output delay_risk_training_pipeline.json
```

### Pipeline Health Dashboard

```bash
pip install streamlit google-cloud-bigquery google-cloud-monitoring
streamlit run dashboard/app.py
```

---

## Key Documents

| Document | Purpose |
|----------|---------|
| [architecture-overview.md](docs/architecture-overview.md) | Full architecture with multi-cloud mapping |
| [multi-cloud-comparison.md](docs/multi-cloud-comparison.md) | GCP/Azure/AWS service equivalents (interview reference) |
| [case-study.md](portfolio/case-study.md) | Business narrative and portfolio positioning |
| [interview-talk-track.md](portfolio/interview-talk-track.md) | Three interview versions |

---

## Multi-Cloud Positioning

| Platform | Role in Portfolio |
|----------|-------------------|
| **GCP** | Production deployment (ERP AI Delay Risk) + this data platform |
| **Azure** | Certifications (AZ-305, AI-102, AZ-104) + Bicep IaC migration path |
| **AWS** | Documented equivalents (planned: SAA-C03 → SAP-C02 → MLS-C01) |

Architecture patterns — data lake zones, medallion datasets, feature stores, ML pipeline stages — are platform-transferable. Service names change; principles do not.

---

## Related Projects

- [ERP AI Delay Risk](../erp-ai-delay-risk/) — Application layer (Cloud Run inference)
- [Forecasting and Planning Support](../forecasting-planning-support/) — Demand forecasting (feeds demand signals)

---

*Dr. Tatianna Gilliam — My production deployment is on GCP. My certifications are on Azure. My architecture thinking is platform-agnostic.*
