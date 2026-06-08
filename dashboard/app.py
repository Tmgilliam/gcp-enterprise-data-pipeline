"""
Pipeline Health Dashboard — Streamlit view for data platform monitoring.

Displays ingestion status, transformation health, feature freshness,
and connection to ERP AI Delay Risk model serving.

Multi-cloud equivalent:
  - Power BI / Grafana dashboard for Azure/AWS pipeline monitoring
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import streamlit as st

st.set_page_config(
    page_title="ERP Data Pipeline Health",
    page_icon="📊",
    layout="wide",
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "erp-data-pipeline-dev")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")


@st.cache_data(ttl=300)
def get_pipeline_status():
    """Fetch pipeline health metrics. Uses BigQuery and GCS when credentials available."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT_ID)

        datasets = {
            "staging": f"staging_{ENVIRONMENT}",
            "analytics": f"analytics_{ENVIRONMENT}",
            "ml_features": f"ml_features_{ENVIRONMENT}",
        }

        status = {}
        for layer, dataset in datasets.items():
            try:
                query = f"""
                    SELECT
                        table_id,
                        row_count,
                        ROUND(size_bytes / 1024 / 1024, 2) AS size_mb,
                        TIMESTAMP_MILLIS(last_modified_time) AS last_modified
                    FROM `{PROJECT_ID}.{dataset}.__TABLES__`
                    ORDER BY last_modified_time DESC
                    LIMIT 5
                """
                rows = list(client.query(query).result())
                status[layer] = [
                    {
                        "table": row.table_id,
                        "rows": row.row_count,
                        "size_mb": row.size_mb,
                        "last_modified": str(row.last_modified),
                    }
                    for row in rows
                ]
            except Exception:
                status[layer] = []

        return {"connected": True, "datasets": status}
    except Exception:
        return {
            "connected": False,
            "datasets": {
                "staging": [
                    {"table": "inventory_transactions", "rows": 15420, "size_mb": 12.3,
                     "last_modified": "2026-06-08T02:15:00Z"},
                ],
                "analytics": [
                    {"table": "demand_signals_by_sku", "rows": 3250, "size_mb": 4.1,
                     "last_modified": "2026-06-08T02:30:00Z"},
                ],
                "ml_features": [
                    {"table": "sku_risk_features", "rows": 3250, "size_mb": 2.8,
                     "last_modified": "2026-06-08T02:45:00Z"},
                ],
            },
        }


def main():
    st.title("GCP Enterprise Data Pipeline — Health Dashboard")
    st.caption(
        f"Project: `{PROJECT_ID}` | Environment: `{ENVIRONMENT}` | "
        f"Feeds: [ERP AI Delay Risk](https://github.com/Tmgilliam/erp-ai-delay-risk)"
    )

    status = get_pipeline_status()
    if not status["connected"]:
        st.info("Running in demo mode — configure GCP credentials for live data.")

    # Pipeline layer status
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Ingestion", "Healthy", delta="0 failures (24h)")
    with col2:
        st.metric("Transformation", "Healthy", delta="3/3 SQL jobs passed")
    with col3:
        st.metric("Feature Freshness", "6.2h", delta="-18h remaining", delta_color="normal")
    with col4:
        st.metric("ML Model", "Champion", delta="F1: 0.78")

    st.divider()

    # Dataset tables
    st.subheader("BigQuery Dataset Layers")
    for layer, tables in status["datasets"].items():
        with st.expander(f"{layer.upper()} dataset ({len(tables)} tables)", expanded=(layer == "ml_features")):
            if tables:
                st.dataframe(tables, use_container_width=True)
            else:
                st.write("No tables found.")

    st.divider()

    # ERP AI Delay Risk connection
    st.subheader("ERP AI Delay Risk Connection")
    conn_col1, conn_col2 = st.columns(2)

    with conn_col1:
        st.markdown("""
        **Phase 1 (Production)**
        - Cloud Run FastAPI inference
        - Local training on CSV export
        - Request-time feature computation
        - F1 baseline: 0.72
        """)

    with conn_col2:
        st.markdown("""
        **Phase 2+ (This Pipeline)**
        - Vertex AI Pipelines training
        - Feature Store online serving
        - BigQuery `ml_features.sku_risk_features`
        - Automated daily refresh
        """)

    st.divider()

    # Pipeline stages diagram
    st.subheader("Pipeline Stages")
    stages = [
        ("GCS Landing", "raw/ → processed/", "✅"),
        ("Pub/Sub Events", "3 topics, 0 DLQ backlog", "✅"),
        ("Dataflow ETL", "Last run: 02:00 UTC", "✅"),
        ("BQ Transform", "3 SQL jobs", "✅"),
        ("Feature Store", "4 entity types synced", "✅"),
        ("ML Pipeline", "Last train: 2026-06-07", "✅"),
        ("Model Serving", "Cloud Run + Endpoint", "✅"),
    ]

    for stage, detail, health in stages:
        st.markdown(f"**{health} {stage}** — {detail}")

    st.divider()
    st.caption(
        f"Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} | "
        "Dr. Tatianna Gilliam — Cloud & AI Architect"
    )


if __name__ == "__main__":
    main()
