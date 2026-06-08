-- ml_features_sku_risk.sql
-- Combine demand, lead time, and inventory signals into ML feature table.
-- Source: analytics.demand_signals_by_sku, staging.inventory_transactions
-- Destination: ml_features.sku_risk_features
-- Consumer: Vertex AI Pipelines training, Feature Store sync, ERP AI Delay Risk

CREATE OR REPLACE TABLE `{project_id}.{dataset_ml_features}.sku_risk_features`
PARTITION BY feature_date
CLUSTER BY sku_id, vendor_id
AS
WITH vendor_lead_times AS (
  SELECT
    vendor_id,
    sku_id,
    APPROX_QUANTILES(
      DATE_DIFF(
        DATE(receipt_timestamp),
        DATE(po_date),
        DAY
      ),
      100
    )[OFFSET(50)] AS lead_time_p50,
    APPROX_QUANTILES(
      DATE_DIFF(
        DATE(receipt_timestamp),
        DATE(po_date),
        DAY
      ),
      100
    )[OFFSET(90)] AS lead_time_p90,
    AVG(CASE
      WHEN DATE_DIFF(DATE(receipt_timestamp), DATE(expected_date), DAY) > 0
        THEN 1.0 ELSE 0.0
    END) AS late_delivery_rate,
    COUNT(*) AS po_count_90d
  FROM `{project_id}.{dataset_staging}.purchase_order_receipts`
  WHERE po_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY vendor_id, sku_id
),

sku_vendor_map AS (
  SELECT
    sku_id,
    vendor_id,
    COUNT(*) AS transaction_count
  FROM `{project_id}.{dataset_staging}.inventory_transactions`
  WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND vendor_id IS NOT NULL
  GROUP BY sku_id, vendor_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sku_id ORDER BY transaction_count DESC
  ) = 1
)

SELECT
  d.sku_id,
  svm.vendor_id,
  CURRENT_DATE() AS feature_date,

  -- Demand features (from analytics layer)
  d.demand_7d,
  d.demand_30d,
  d.demand_90d,
  d.demand_cv_30d,
  d.demand_acceleration_ratio,

  -- Inventory features
  d.days_of_supply,
  d.stockout_flag,
  d.stockout_days_30d,
  d.days_since_last_activity,

  -- Lead time features (from vendor receipts)
  COALESCE(vlt.lead_time_p50, 14) AS lead_time_p50,
  COALESCE(vlt.lead_time_p90, 21) AS lead_time_p90,
  COALESCE(vlt.late_delivery_rate, 0.0) AS late_delivery_rate,
  COALESCE(vlt.po_count_90d, 0) AS po_count_90d,

  -- Composite risk signals (domain-informed, aligned with ERP AI Delay Risk features)
  SAFE_DIVIDE(d.demand_30d, NULLIF(d.days_of_supply, 0)) AS demand_pressure_ratio,

  CASE
    WHEN d.stockout_flag = 1 AND d.demand_acceleration_ratio > 1.2 THEN 1
    WHEN COALESCE(vlt.late_delivery_rate, 0) > 0.25 THEN 1
    WHEN d.days_of_supply < COALESCE(vlt.lead_time_p90, 21) THEN 1
    ELSE 0
  END AS supply_risk_flag,

  -- Normalized risk score (0-1) for Feature Store serving
  LEAST(1.0, GREATEST(0.0,
    0.3 * COALESCE(vlt.late_delivery_rate, 0)
    + 0.3 * SAFE_DIVIDE(d.stockout_days_30d, 30.0)
    + 0.2 * (1.0 - LEAST(d.days_of_supply / 30.0, 1.0))
    + 0.2 * LEAST(d.demand_cv_30d, 1.0)
  )) AS delay_risk_score,

  -- Lineage metadata
  CURRENT_TIMESTAMP() AS computed_at,
  '{pipeline_run_id}' AS pipeline_run_id,
  'v1.0' AS feature_schema_version

FROM `{project_id}.{dataset_analytics}.demand_signals_by_sku` d
LEFT JOIN sku_vendor_map svm ON d.sku_id = svm.sku_id
LEFT JOIN vendor_lead_times vlt
  ON d.sku_id = vlt.sku_id AND svm.vendor_id = vlt.vendor_id
WHERE d.signal_date = CURRENT_DATE();
