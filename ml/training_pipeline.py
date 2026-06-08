"""
Vertex AI Training Pipeline — ERP Delay Risk Model.

Enterprise-scale training infrastructure for the ERP AI Delay Risk model.
Phase 1 trained locally on CSV exports; this pipeline trains at production
cadence with full artifact lineage.

Multi-cloud equivalent:
  - Azure ML Pipelines (Kubeflow-compatible)
  - AWS SageMaker Pipelines

Connection to ERP AI Delay Risk:
  - Same feature schema as src/features.py + ml_features.sku_risk_features
  - Same sklearn RandomForest classifier approach
  - Production baseline compared during evaluation
  - Registered model deploys to Vertex AI Endpoint; Cloud Run can call it
"""

from __future__ import annotations

import json
import logging
from typing import NamedTuple

from kfp import dsl
from kfp.dsl import Input, Output, Artifact, Metrics, Model

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Component 1: Data Validation
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.25.0",
        "google-cloud-bigquery-storage==2.25.0",
        "pandas==2.2.2",
        "pyarrow==16.1.0",
    ],
)
def data_validation_component(
    project_id: str,
    dataset_name: str,
    feature_table: str,
    min_completeness: float,
    validation_report: Output[Artifact],
) -> NamedTuple("ValidationOutput", [("passed", str), ("row_count", int)]):
    """Validate feature table completeness and distribution."""
    from collections import namedtuple
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_name}.{feature_table}"

    query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE feature_date = (
            SELECT MAX(feature_date) FROM `{table_ref}`
        )
    """
    df = client.query(query).to_dataframe()

    report = {
        "table": table_ref,
        "row_count": len(df),
        "columns": list(df.columns),
        "completeness": {},
        "distribution": {},
        "passed": True,
        "failures": [],
    }

    required_features = [
        "sku_id", "demand_7d", "demand_30d", "lead_time_p90",
        "days_of_supply", "stockout_flag", "delay_risk_score",
    ]

    for col in required_features:
        if col not in df.columns:
            report["passed"] = False
            report["failures"].append(f"Missing required column: {col}")
            continue
        null_rate = df[col].isnull().mean()
        completeness = 1.0 - null_rate
        report["completeness"][col] = round(completeness, 4)
        if completeness < min_completeness:
            report["passed"] = False
            report["failures"].append(
                f"{col} completeness {completeness:.2%} < {min_completeness:.2%}"
            )

    numeric_cols = ["demand_7d", "demand_30d", "lead_time_p90", "delay_risk_score"]
    for col in numeric_cols:
        if col in df.columns:
            report["distribution"][col] = {
                "mean": round(float(df[col].mean()), 4),
                "std": round(float(df[col].std()), 4),
                "min": round(float(df[col].min()), 4),
                "max": round(float(df[col].max()), 4),
            }

    with open(validation_report.path, "w") as f:
        json.dump(report, f, indent=2)

    ValidationOutput = namedtuple("ValidationOutput", ["passed", "row_count"])
    return ValidationOutput(
        passed=str(report["passed"]),
        row_count=len(df),
    )


# ---------------------------------------------------------------------------
# Component 2: Feature Engineering
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.25.0",
        "pandas==2.2.2",
        "pyarrow==16.1.0",
        "scikit-learn==1.5.0",
    ],
)
def feature_engineering_component(
    project_id: str,
    dataset_name: str,
    feature_table: str,
    engineered_features: Output[Artifact],
) -> NamedTuple("FeatureOutput", [("feature_count", int), ("sample_count", int)]):
    """Read ml_features.sku_risk_features and apply additional transformations."""
    from collections import namedtuple
    from google.cloud import bigquery
    import pandas as pd
    import numpy as np

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_name}.{feature_table}"

    query = f"""
        SELECT
            sku_id,
            vendor_id,
            demand_7d,
            demand_30d,
            demand_90d,
            demand_cv_30d,
            demand_acceleration_ratio,
            days_of_supply,
            stockout_flag,
            stockout_days_30d,
            days_since_last_activity,
            lead_time_p50,
            lead_time_p90,
            late_delivery_rate,
            po_count_90d,
            demand_pressure_ratio,
            supply_risk_flag,
            delay_risk_score
        FROM `{table_ref}`
        WHERE feature_date = (SELECT MAX(feature_date) FROM `{table_ref}`)
    """
    df = client.query(query).to_dataframe()

    # Additional feature engineering aligned with ERP AI Delay Risk src/features.py
    df["lead_time_buffer_ratio"] = df["lead_time_p90"] / df["lead_time_p50"].clip(lower=1)
    df["inventory_coverage_ratio"] = df["days_of_supply"] / df["lead_time_p90"].clip(lower=1)
    df["demand_momentum"] = df["demand_7d"] / df["demand_30d"].clip(lower=0.01)
    df["vendor_reliability_score"] = 1.0 - df["late_delivery_rate"]
    df["composite_supply_pressure"] = (
        df["demand_pressure_ratio"].fillna(0) * 0.4
        + df["stockout_flag"] * 0.3
        + (1 - df["inventory_coverage_ratio"].clip(upper=1).fillna(0)) * 0.3
    )

    # Synthetic label for training (production uses actual delay outcomes)
    # In enterprise deployment, join with order_outcomes table
    np.random.seed(42)
    df["is_delayed"] = (
        (df["supply_risk_flag"] == 1)
        | (df["late_delivery_rate"] > 0.2)
        | (df["stockout_flag"] == 1)
    ).astype(int)

    # Add noise to simulate real-world label distribution (~25% delay rate)
    noise = np.random.random(len(df)) < 0.1
    df.loc[noise, "is_delayed"] = 1 - df.loc[noise, "is_delayed"]

    df.to_parquet(engineered_features.path, index=False)

    FeatureOutput = namedtuple("FeatureOutput", ["feature_count", "sample_count"])
    return FeatureOutput(
        feature_count=len(df.columns),
        sample_count=len(df),
    )


# ---------------------------------------------------------------------------
# Component 3: Training
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "scikit-learn==1.5.0",
        "pandas==2.2.2",
        "pyarrow==16.1.0",
        "google-cloud-aiplatform==1.56.0",
    ],
)
def training_component(
    project_id: str,
    region: str,
    experiment_name: str,
    engineered_features: Input[Artifact],
    model_artifact: Output[Model],
    training_metrics: Output[Metrics],
    test_size: float = 0.2,
    n_estimators: int = 100,
    max_depth: int = 10,
) -> NamedTuple("TrainingOutput", [("f1_score", float), ("model_path", str)]):
    """Train sklearn RandomForest and log metrics to Vertex AI Experiments."""
    from collections import namedtuple
    import pickle

    import pandas as pd
    from google.cloud import aiplatform
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
    from sklearn.model_selection import train_test_split

    aiplatform.init(project=project_id, location=region, experiment=experiment_name)

    df = pd.read_parquet(engineered_features.path)

    feature_cols = [
        "demand_7d", "demand_30d", "demand_90d", "demand_cv_30d",
        "demand_acceleration_ratio", "days_of_supply", "stockout_flag",
        "stockout_days_30d", "days_since_last_activity",
        "lead_time_p50", "lead_time_p90", "late_delivery_rate",
        "po_count_90d", "demand_pressure_ratio", "supply_risk_flag",
        "lead_time_buffer_ratio", "inventory_coverage_ratio",
        "demand_momentum", "vendor_reliability_score", "composite_supply_pressure",
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_delayed"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=42,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    metrics = {
        "f1_score": float(f1_score(y_test, y_pred)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "feature_count": len(feature_cols),
    }

    for key, value in metrics.items():
        training_metrics.log_metric(key, value)

    with open(model_artifact.path, "wb") as f:
        pickle.dump({"model": model, "feature_cols": feature_cols}, f)

    # Log to Vertex AI Experiments
    with aiplatform.start_run("delay-risk-training") as run:
        run.log_params({
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "test_size": test_size,
        })
        run.log_metrics(metrics)

    TrainingOutput = namedtuple("TrainingOutput", ["f1_score", "model_path"])
    return TrainingOutput(
        f1_score=metrics["f1_score"],
        model_path=model_artifact.path,
    )


# ---------------------------------------------------------------------------
# Component 4: Evaluation
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "scikit-learn==1.5.0",
        "pandas==2.2.2",
        "pyarrow==16.1.0",
    ],
)
def evaluation_component(
    f1_score: float,
    model_threshold_f1: float,
    baseline_f1: float,
    evaluation_report: Output[Artifact],
) -> NamedTuple("EvalOutput", [("passed", str), ("f1_score", float)]):
    """Evaluate against holdout set and compare to production baseline model."""
    from collections import namedtuple

    report = {
        "candidate_f1": f1_score,
        "threshold_f1": model_threshold_f1,
        "baseline_f1": baseline_f1,
        "beats_baseline": f1_score > baseline_f1,
        "meets_threshold": f1_score >= model_threshold_f1,
        "passed": f1_score >= model_threshold_f1 and f1_score > baseline_f1,
        "improvement_over_baseline": round(f1_score - baseline_f1, 4),
        "evaluation_notes": (
            "Compared against ERP AI Delay Risk Phase 1 production baseline. "
            "Candidate must exceed both absolute threshold and production baseline."
        ),
    }

    with open(evaluation_report.path, "w") as f:
        json.dump(report, f, indent=2)

    EvalOutput = namedtuple("EvalOutput", ["passed", "f1_score"])
    return EvalOutput(
        passed=str(report["passed"]),
        f1_score=f1_score,
    )


# ---------------------------------------------------------------------------
# Component 5: Registration
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-aiplatform==1.56.0",
        "scikit-learn==1.5.0",
    ],
)
def registration_component(
    project_id: str,
    region: str,
    evaluation_passed: str,
    model_artifact: Input[Model],
    model_display_name: str,
) -> NamedTuple("RegistrationOutput", [("model_resource_name", str), ("registered", str)]):
    """Register model in Vertex AI Model Registry if evaluation passes threshold."""
    from collections import namedtuple
    from google.cloud import aiplatform

    RegistrationOutput = namedtuple("RegistrationOutput", ["model_resource_name", "registered"])

    if evaluation_passed.lower() != "true":
        return RegistrationOutput(
            model_resource_name="",
            registered="false",
        )

    aiplatform.init(project=project_id, location=region)

    model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=model_artifact.uri,
        serving_container_image_uri=(
            "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-5:latest"
        ),
        description=(
            "ERP Delay Risk classifier — enterprise pipeline trained model. "
            "Feeds ERP AI Delay Risk Cloud Run inference service."
        ),
        labels={
            "model_type": "delay_risk",
            "framework": "sklearn",
            "pipeline": "gcp-enterprise-data-pipeline",
        },
    )

    return RegistrationOutput(
        model_resource_name=model.resource_name,
        registered="true",
    )


# ---------------------------------------------------------------------------
# Component 6: Deployment
# ---------------------------------------------------------------------------

@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-aiplatform==1.56.0",
    ],
)
def deployment_component(
    project_id: str,
    region: str,
    registered: str,
    model_resource_name: str,
    endpoint_name: str,
    machine_type: str = "n1-standard-2",
    min_replica_count: int = 1,
    max_replica_count: int = 3,
) -> NamedTuple("DeployOutput", [("endpoint_resource_name", str), ("deployed", str)]):
    """Deploy to Vertex AI Endpoint if model is champion."""
    from collections import namedtuple
    from google.cloud import aiplatform

    DeployOutput = namedtuple("DeployOutput", ["endpoint_resource_name", "deployed"])

    if registered.lower() != "true" or not model_resource_name:
        return DeployOutput(
            endpoint_resource_name="",
            deployed="false",
        )

    aiplatform.init(project=project_id, location=region)

    endpoints = aiplatform.Endpoint.list(
        filter=f'display_name="{endpoint_name}"',
    )
    if endpoints:
        endpoint = endpoints[0]
    else:
        endpoint = aiplatform.Endpoint.create(display_name=endpoint_name)

    model = aiplatform.Model(model_resource_name)
    model.deploy(
        endpoint=endpoint,
        deployed_model_display_name="delay-risk-champion",
        machine_type=machine_type,
        min_replica_count=min_replica_count,
        max_replica_count=max_replica_count,
        traffic_percentage=100,
    )

    return DeployOutput(
        endpoint_resource_name=endpoint.resource_name,
        deployed="true",
    )


# ---------------------------------------------------------------------------
# Pipeline Definition
# ---------------------------------------------------------------------------

@dsl.pipeline(
    name="erp-delay-risk-training-pipeline",
    description=(
        "Enterprise training pipeline for ERP AI Delay Risk model. "
        "Phase 1 trained locally; this is Phase 2+ infrastructure."
    ),
)
def delay_risk_training_pipeline(
    project_id: str,
    region: str = "us-central1",
    dataset_name: str = "ml_features",
    feature_table: str = "sku_risk_features",
    model_threshold_f1: float = 0.75,
    baseline_f1: float = 0.72,
    endpoint_name: str = "erp-delay-risk-endpoint",
    experiment_name: str = "erp-delay-risk-experiments",
    min_completeness: float = 0.95,
):
    """
    Chain: validate → engineer → train → evaluate → register → deploy.

    Parameters:
        project_id: GCP project ID.
        dataset_name: BigQuery dataset containing feature table.
        feature_table: Feature table name (sku_risk_features).
        model_threshold_f1: Minimum F1 to pass evaluation.
        baseline_f1: ERP AI Delay Risk Phase 1 production baseline F1.
        endpoint_name: Vertex AI Endpoint display name.
    """
    # Step 1: Validate feature table
    validation_task = data_validation_component(
        project_id=project_id,
        dataset_name=dataset_name,
        feature_table=feature_table,
        min_completeness=min_completeness,
    )

    # Step 2: Feature engineering (depends on validation)
    with dsl.Condition(validation_task.outputs["passed"] == "true"):
        feature_task = feature_engineering_component(
            project_id=project_id,
            dataset_name=dataset_name,
            feature_table=feature_table,
        )

        # Step 3: Training
        training_task = training_component(
            project_id=project_id,
            region=region,
            experiment_name=experiment_name,
            engineered_features=feature_task.outputs["engineered_features"],
        )

        # Step 4: Evaluation
        evaluation_task = evaluation_component(
            f1_score=training_task.outputs["f1_score"],
            model_threshold_f1=model_threshold_f1,
            baseline_f1=baseline_f1,
        )

        # Step 5: Registration (only if evaluation passes)
        with dsl.Condition(evaluation_task.outputs["passed"] == "true"):
            registration_task = registration_component(
                project_id=project_id,
                region=region,
                evaluation_passed=evaluation_task.outputs["passed"],
                model_artifact=training_task.outputs["model_artifact"],
                model_display_name="erp-delay-risk-classifier",
            )

            # Step 6: Deployment (only if registered)
            deployment_component(
                project_id=project_id,
                region=region,
                registered=registration_task.outputs["registered"],
                model_resource_name=registration_task.outputs["model_resource_name"],
                endpoint_name=endpoint_name,
            )


# ---------------------------------------------------------------------------
# Pipeline Compiler Entry Point
# ---------------------------------------------------------------------------

def compile_pipeline(output_path: str = "delay_risk_training_pipeline.json") -> str:
    """Compile the pipeline to a JSON specification for Vertex AI Pipelines."""
    from kfp import compiler

    compiler.Compiler().compile(
        pipeline_func=delay_risk_training_pipeline,
        package_path=output_path,
    )
    logger.info("Pipeline compiled to %s", output_path)
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compile ERP Delay Risk training pipeline")
    parser.add_argument("--output", default="delay_risk_training_pipeline.json")
    args = parser.parse_args()

    compile_pipeline(args.output)
    print(f"Pipeline compiled: {args.output}")
