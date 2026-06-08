# Design Decisions

**Dr. Tatianna Gilliam** | Cloud & AI Architect

Architecture Decision Records (ADRs) for the GCP Enterprise Data Pipeline.

---

## ADR-001: Single GCS Bucket with Prefix Zones

**Status:** Accepted

**Context:** Need storage for raw, staging, processed, and archive data lake zones.

**Decision:** One GCS bucket with prefix-based zones (`raw/`, `staging/`, `processed/`, `archive/`) rather than separate buckets per zone.

**Rationale:**
- Simpler IAM (one bucket policy, prefix-based conditions if needed)
- Unified lifecycle policy management
- Lower operational overhead

**Alternatives considered:**
- Separate buckets per zone (more IAM complexity, harder lifecycle coordination)
- Separate buckets per source system (over-engineering for current scale)

**Multi-cloud:** Same pattern on Azure (one storage account, multiple containers) and AWS (one bucket, prefix zones).

---

## ADR-002: BigQuery as Transformation Engine (Not Dataflow)

**Status:** Accepted

**Context:** Need SQL-based transformations between dataset layers (raw → staging → analytics → ml_features).

**Decision:** BigQuery SQL jobs for dataset-layer transformations. Dataflow for ingestion (GCS/Pub/Sub → BigQuery), not for inter-dataset transforms.

**Rationale:**
- SQL transformations are more readable and maintainable for analysts
- BigQuery handles partitioning, clustering, and cost control natively
- Dataflow reserved for data movement and complex event processing

**Multi-cloud:** Synapse SQL and Redshift SQL serve the same role. Data Factory/Glue serve the same ingestion role.

---

## ADR-003: Vertex AI Feature Store (Not Request-Time Computation)

**Status:** Accepted

**Context:** ERP AI Delay Risk Phase 1 computed features at API request time. Risk of training-serving skew as features evolve.

**Decision:** Materialize features in Vertex AI Feature Store. Cloud Run API fetches pre-computed features by entity ID.

**Rationale:**
- Eliminates training-serving skew (the #1 production ML failure mode)
- Reduces API latency (feature lookup < 50ms vs. computation)
- Enables feature reuse across models (demand forecasting, delay risk, fill rate)

**Trade-off:** Added infrastructure complexity and feature freshness monitoring requirement.

**Multi-cloud:** Azure ML Feature Store and SageMaker Feature Store solve the same problem identically.

---

## ADR-004: sklearn RandomForest (Not Deep Learning)

**Status:** Accepted

**Context:** Need a classifier for ERP delay risk prediction.

**Decision:** Continue with scikit-learn RandomForest from ERP AI Delay Risk Phase 1. Same algorithm, enterprise training infrastructure.

**Rationale:**
- 20 features, thousands of records — not a deep learning problem
- Interpretability requirement: operations leadership needs to trust feature importance
- Phase 1 model already validated on ERP domain data
- Vertex AI Pipelines supports sklearn natively

**Multi-cloud:** Algorithm choice is platform-agnostic. Any cloud ML platform supports sklearn.

---

## ADR-005: Terraform (Not Deployment Manager or gcloud)

**Status:** Accepted

**Context:** Need reproducible, reviewable infrastructure provisioning.

**Decision:** Terraform for all infrastructure. Not Deployment Manager, not manual gcloud scripts.

**Rationale:**
- Multi-cloud IaC skill (same tool for Azure and AWS providers)
- PR-reviewable infrastructure changes
- State management and drift detection
- Comments on every resource document pipeline stage purpose

**Multi-cloud:** Terraform is the platform-agnostic IaC choice. Azure Bicep used in ERP AI Delay Risk Azure migration; Terraform used here for cross-cloud portability.

---

## ADR-006: Evaluation Gate — Beat Baseline AND Meet Threshold

**Status:** Accepted

**Context:** Model promotion must prevent regression against production.

**Decision:** Vertex AI Pipeline evaluation component requires candidate F1 ≥ absolute threshold (0.75) AND candidate F1 > Phase 1 production baseline (0.72).

**Rationale:**
- Absolute threshold prevents deploying weak models
- Baseline comparison prevents regression against known-good production model
- Both gates must pass — no manual override in automated pipeline

**Connection:** ERP AI Delay Risk Phase 1 F1 = 0.72 is the production baseline referenced in `ml/training_pipeline.py`.

---

## ADR-007: Unified Beam Pipeline (Batch + Streaming)

**Status:** Accepted

**Context:** ERP data arrives via batch exports (nightly) and real-time events (WMS).

**Decision:** One Apache Beam pipeline codebase with `PipelineOptions.streaming` flag. Not separate batch and streaming pipelines.

**Rationale:**
- DRY business logic (validation, enrichment, metadata)
- Beam designed for this dual-mode execution
- Reduces maintenance burden as business rules evolve

**Multi-cloud:** Azure requires Data Factory + Stream Analytics (two services). AWS requires Glue + Kinesis Analytics (two services). Beam on Dataflow is uniquely unified on GCP, but the business logic modules transfer to any platform.

---

*Each ADR documents the architectural principle, not just the GCP service choice. Principles transfer; services map.*
