# Data Governance

**Dr. Tatianna Gilliam** | Cloud & AI Architect

Governance framework for the GCP Enterprise Data Pipeline. Applies to all datasets feeding the ERP AI Delay Risk model.

---

## Data Classification

| Classification | Data Types | Examples | Handling |
|----------------|-----------|----------|----------|
| **Public** | None in this pipeline | — | N/A |
| **Internal** | Inventory, orders, vendor metrics | SKU demand signals, lead times, stockout flags | Standard access controls |
| **Confidential** | Customer PII | Customer names, payment terms, AR data | Encrypted at rest, restricted access |
| **Restricted** | Financial transactions | Invoice amounts, pricing | Audit logging, need-to-know access |

Customer entity features in Feature Store (`customer` entity type) are classified **Confidential**. All other entity types are **Internal**.

---

## Access Control Matrix

| Identity | GCS raw/ | BQ raw | BQ staging | BQ analytics | BQ ml_features | Feature Store | Vertex AI |
|----------|----------|--------|------------|--------------|----------------|---------------|-----------|
| Pipeline SA | Read/Write | Write | Write | Write | Write | Write | User |
| Analyst (human) | — | — | Read | Read | — | — | — |
| ML Engineer | — | — | Read | Read | Read | Read/Write | User |
| Cloud Run API SA | — | — | — | — | Read | Online Read | Predict |
| Admin | Full | Full | Full | Full | Full | Admin | Admin |

Principle: least privilege. The Cloud Run API service account (ERP AI Delay Risk) has Feature Store online read only — not write, not BigQuery write.

---

## Retention Policies

| Layer | Retention | Enforcement | Rationale |
|-------|-----------|-------------|-----------|
| GCS raw/ | 365 days (lifecycle delete) | GCS lifecycle rules | Audit replay window |
| GCS processed/ | 365 days | GCS lifecycle rules | Pipeline checkpoint history |
| GCS archive/ | 365 days | GCS lifecycle rules | Compliance maximum |
| BQ raw | 90 days | Dataset default expiration | Staging promotion window |
| BQ staging | 180 days | Dataset default expiration | Reprocessing window |
| BQ analytics | Indefinite | Manual review | Business KPI history |
| BQ ml_features | Indefinite | Manual review | Model training lineage |
| Feature Store | Latest + 90 days history | Feature Store versioning | Training-serving consistency |

---

## Audit Logging

| Event | Log Source | Retention |
|-------|-----------|-----------|
| Data access (BQ queries) | Cloud Audit Logs (data_access) | 400 days |
| GCS object access | Cloud Audit Logs (data_access) | 400 days |
| IAM changes | Cloud Audit Logs (admin_activity) | 400 days |
| Pipeline job execution | Dataflow job logs + Cloud Logging | 90 days |
| Model training runs | Vertex AI Experiments | Indefinite |
| Feature Store sync | Custom pipeline logs | 90 days |

---

## PII Handling

Customer entity data requires:

1. **No raw PII in ml_features tables** — customer_id is the only customer field in feature tables
2. **Feature Store entity IDs are opaque** — no names, emails, or addresses in feature values
3. **BigQuery column-level security** (production) — policy tags on confidential columns
4. **Right to deletion** — customer entity removal triggers Feature Store entity deletion and BQ row purge

---

## Data Quality Standards

| Check | Threshold | Enforcement |
|-------|-----------|-------------|
| Schema validation (ingestion) | 100% column match | `GCSLoader.validate_file` — reject on failure |
| Null rate (required columns) | ≤ 5% | `GCSLoader.validate_file` |
| Row count (post-transform) | ≥ 1 (non-empty) | `BQTransformer.validate_row_count` |
| Feature completeness | ≥ 95% | Vertex AI Pipeline data_validation_component |
| Feature freshness | ≤ 25 hours | Monitoring alert |

---

## Connection to ERP AI Delay Risk

The delay risk model's production reliability depends on governed data:

- Feature Store ensures training features match serving features (no training-serving skew)
- Row count validation prevents empty feature tables from reaching the training pipeline
- Feature freshness monitoring alerts before stale features reach the Cloud Run `/predict` endpoint
- Audit logging provides lineage for model governance reviews

---

*Multi-cloud note: Azure equivalent uses Purview for classification and catalog. AWS equivalent uses Macie for PII detection and Lake Formation for access control. The governance principles — classify, restrict, retain, audit — are identical.*
