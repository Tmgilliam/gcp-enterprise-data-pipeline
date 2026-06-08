-- staging_inventory_transactions.sql
-- Clean and type-cast raw inventory data, add processing metadata.
-- Source: raw.inventory_transactions (external table over GCS)
-- Destination: staging.inventory_transactions
-- Serves: analytics demand signals, ml_features SKU risk features

CREATE OR REPLACE TABLE `{project_id}.{dataset_staging}.inventory_transactions`
PARTITION BY ingestion_date
CLUSTER BY sku_id, vendor_id
AS
SELECT
  -- Primary identifiers
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(sku_id AS STRING) AS sku_id,
  CAST(vendor_id AS STRING) AS vendor_id,
  CAST(warehouse_id AS STRING) AS warehouse_id,

  -- Transaction details
  CAST(quantity AS INT64) AS quantity,
  LOWER(TRIM(CAST(transaction_type AS STRING))) AS transaction_type,
  CAST(unit_cost AS FLOAT64) AS unit_cost,

  -- Timestamps
  TIMESTAMP(transaction_timestamp) AS transaction_timestamp,
  DATE(TIMESTAMP(transaction_timestamp)) AS transaction_date,

  -- Derived fields
  CASE
    WHEN LOWER(TRIM(CAST(transaction_type AS STRING))) IN ('receipt', 'adjustment_in', 'transfer_in')
      THEN ABS(CAST(quantity AS INT64))
    WHEN LOWER(TRIM(CAST(transaction_type AS STRING))) IN ('shipment', 'adjustment_out', 'transfer_out', 'consumption')
      THEN -ABS(CAST(quantity AS INT64))
    ELSE CAST(quantity AS INT64)
  END AS signed_quantity,

  -- Source metadata
  CAST(source_system AS STRING) AS source_system,
  CAST(source_file AS STRING) AS source_file,

  -- Processing metadata
  CURRENT_DATE() AS ingestion_date,
  CURRENT_TIMESTAMP() AS processed_at,
  '{pipeline_run_id}' AS pipeline_run_id,
  'v1.0' AS schema_version

FROM `{project_id}.{dataset_raw}.inventory_transactions_raw`

WHERE
  -- Data quality filters
  transaction_id IS NOT NULL
  AND sku_id IS NOT NULL
  AND transaction_timestamp IS NOT NULL
  AND quantity IS NOT NULL
  AND CAST(quantity AS INT64) != 0

  -- Exclude duplicate records (keep latest by transaction_timestamp)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY transaction_id
  ORDER BY TIMESTAMP(transaction_timestamp) DESC
) = 1;
