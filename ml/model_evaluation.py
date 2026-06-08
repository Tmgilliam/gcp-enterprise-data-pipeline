"""
Model Evaluation and Registration Utilities.

Evaluates trained models against holdout sets and production baseline
from ERP AI Delay Risk Phase 1. Gates model registration on threshold.

Multi-cloud equivalent:
  - Azure ML model evaluation + registration
  - SageMaker model evaluation + Model Registry
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

logger = logging.getLogger(__name__)


@dataclass
class EvaluationResult:
    """Model evaluation results with pass/fail gate."""

    model_version: str
    f1_score: float
    precision: float
    recall: float
    accuracy: float
    roc_auc: Optional[float]
    baseline_f1: float
    threshold_f1: float
    beats_baseline: bool
    meets_threshold: bool
    passed: bool
    confusion_matrix: list[list[int]]
    classification_report: dict[str, Any]
    evaluated_at: str


class ModelEvaluator:
    """
    Evaluate delay risk models against holdout data and production baseline.

    Connected to ERP AI Delay Risk Phase 1 baseline metrics.
    """

    def __init__(
        self,
        threshold_f1: float = 0.75,
        baseline_f1: float = 0.72,
    ):
        self.threshold_f1 = threshold_f1
        self.baseline_f1 = baseline_f1

    def evaluate(
        self,
        model: Any,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        model_version: str = "candidate",
    ) -> EvaluationResult:
        """
        Evaluate model and compare against baseline and threshold.

        Args:
            model: Trained sklearn classifier with predict/predict_proba.
            X_test: Holdout feature matrix.
            y_test: Holdout labels.
            model_version: Version identifier for reporting.

        Returns:
            EvaluationResult with pass/fail gate.
        """
        y_pred = model.predict(X_test)

        f1 = float(f1_score(y_test, y_pred))
        precision = float(precision_score(y_test, y_pred, zero_division=0))
        recall = float(recall_score(y_test, y_pred, zero_division=0))
        accuracy = float(accuracy_score(y_test, y_pred))

        roc_auc = None
        if hasattr(model, "predict_proba"):
            try:
                y_proba = model.predict_proba(X_test)[:, 1]
                roc_auc = float(roc_auc_score(y_test, y_proba))
            except Exception:
                pass

        cm = confusion_matrix(y_test, y_pred).tolist()
        report = classification_report(y_test, y_pred, output_dict=True)

        beats_baseline = f1 > self.baseline_f1
        meets_threshold = f1 >= self.threshold_f1
        passed = beats_baseline and meets_threshold

        result = EvaluationResult(
            model_version=model_version,
            f1_score=f1,
            precision=precision,
            recall=recall,
            accuracy=accuracy,
            roc_auc=roc_auc,
            baseline_f1=self.baseline_f1,
            threshold_f1=self.threshold_f1,
            beats_baseline=beats_baseline,
            meets_threshold=meets_threshold,
            passed=passed,
            confusion_matrix=cm,
            classification_report=report,
            evaluated_at=datetime.now(timezone.utc).isoformat(),
        )

        logger.info(
            "Evaluation complete",
            extra={
                "model_version": model_version,
                "f1": f1,
                "passed": passed,
                "beats_baseline": beats_baseline,
            },
        )
        return result

    def save_report(self, result: EvaluationResult, output_path: str) -> str:
        """Save evaluation report as JSON artifact."""
        report = {
            "model_version": result.model_version,
            "metrics": {
                "f1_score": result.f1_score,
                "precision": result.precision,
                "recall": result.recall,
                "accuracy": result.accuracy,
                "roc_auc": result.roc_auc,
            },
            "gates": {
                "baseline_f1": result.baseline_f1,
                "threshold_f1": result.threshold_f1,
                "beats_baseline": result.beats_baseline,
                "meets_threshold": result.meets_threshold,
                "passed": result.passed,
            },
            "confusion_matrix": result.confusion_matrix,
            "classification_report": result.classification_report,
            "evaluated_at": result.evaluated_at,
            "notes": (
                "Compared against ERP AI Delay Risk Phase 1 production baseline. "
                "Candidate must exceed both absolute threshold and production baseline."
            ),
        }

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        return str(path)
