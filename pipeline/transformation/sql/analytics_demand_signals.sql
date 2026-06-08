-- analytics_demand_signals.sql
-- Aggregate demand metrics by SKU with rolling windows and stockout flags.
-- Source: staging.inventory_transactions
-- Destination: analytics.demand_signals_by_sku
-- Serves: ml_features SKU risk, ERP AI Delay Risk demand signal features

CREATE OR REPLACE TABLE `{project_id}.{dataset_analytics}.demand_signals_by_sku`
PARTITION BY signal_date
CLUSTER BY sku_id
AS
WITH daily_demand AS (
  SELECT
    sku_id,
    transaction_date,
    SUM(CASE
      WHEN transaction_type IN ('shipment', 'consumption', 'transfer_out')
        THEN ABS(signed_quantity)
      ELSE 0
    END) AS daily_demand_units,
    SUM(CASE
      WHEN transaction_type IN ('receipt', 'adjustment_in', 'transfer_in')
        THEN ABS(signed_quantity)
      ELSE 0
    END) AS daily_receipt_units,
    COUNT(DISTINCT transaction_id) AS daily_transaction_count
  FROM `{project_id}.{dataset_staging}.inventory_transactions`
  WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
  GROUP BY sku_id, transaction_date
),

running_inventory AS (
  SELECT
    sku_id,
    transaction_date,
    daily_demand_units,
    daily_receipt_units,
    daily_transaction_count,
    SUM(daily_receipt_units - daily_demand_units) OVER (
      PARTITION BY sku_id
      ORDER BY transaction_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_inventory_balance
  FROM daily_demand
),

sku_aggregates AS (
  SELECT
    sku_id,
    CURRENT_DATE() AS signal_date,

    -- Rolling demand windows
    SUM(daily_demand_units) AS demand_7d,
    SUM(CASE
      WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        THEN daily_demand_units ELSE 0
    END) AS demand_30d,
    SUM(daily_demand_units) AS demand_90d,

    -- Demand variability (coefficient of variation over 30d)
    SAFE_DIVIDE(
      STDDEV(CASE
        WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          THEN daily_demand_units
      END),
      NULLIF(AVG(CASE
        WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          THEN daily_demand_units
      END), 0)
    ) AS demand_cv_30d,

    -- Stockout detection
    COUNTIF(running_inventory_balance <= 0
      AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ) AS stockout_days_30d,

    MAX(CASE WHEN running_inventory_balance <= 0 THEN 1 ELSE 0 END) AS stockout_flag,

    -- Inventory coverage
    SAFE_DIVIDE(
      MAX(running_inventory_balance),
      NULLIF(AVG(CASE
        WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
          THEN daily_demand_units
      END), 0)
    ) AS days_of_supply,

    -- Activity signals
    COUNT(DISTINCT transaction_date) AS active_days_90d,
    MAX(transaction_date) AS last_transaction_date

  FROM running_inventory
  WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY sku_id
)

SELECT
  sku_id,
  signal_date,
  demand_7d,
  demand_30d,
  demand_90d,
  COALESCE(demand_cv_30d, 0) AS demand_cv_30d,
  stockout_days_30d,
  stockout_flag,
  COALESCE(days_of_supply, 0) AS days_of_supply,
  active_days_90d,
  last_transaction_date,
  DATE_DIFF(CURRENT_DATE(), last_transaction_date, DAY) AS days_since_last_activity,

  -- Demand trend: 7d vs prior 7d
  SAFE_DIVIDE(
    demand_7d,
    NULLIF(demand_30d - demand_7d, 0)
  ) AS demand_acceleration_ratio,

  CURRENT_TIMESTAMP() AS computed_at,
  '{pipeline_run_id}' AS pipeline_run_id

FROM sku_aggregates
WHERE demand_90d > 0 OR active_days_90d > 0;
