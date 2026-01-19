# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.ifood.silver_orders_events_dedup (
  event_id STRING,
  event_type STRING,
  order_id STRING,
  event_ts TIMESTAMP,
  customer_id INT,
  merchant_id INT,
  city STRING,
  payment_method STRING,
  order_status STRING,
  items_count INT,
  subtotal DOUBLE,
  delivery_fee DOUBLE,
  discount DOUBLE,
  total DOUBLE,
  updated_at TIMESTAMP,
  ingestion_ts TIMESTAMP,
  source_file STRING
)
USING DELTA
""")
print("OK - tabela silver_orders_events_dedup")


# COMMAND ----------

spark.sql("""
MERGE INTO workspace.ifood.silver_orders_events_dedup AS tgt
USING (
  SELECT *
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY b.event_id
        ORDER BY
          b.ingestion_ts DESC,
          b.updated_at DESC,
          b.event_ts DESC
      ) AS rn
    FROM workspace.ifood.bronze_orders_events b
  )
  WHERE rn = 1
) AS src
ON tgt.event_id = src.event_id

WHEN MATCHED AND src.ingestion_ts >= tgt.ingestion_ts THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *
""")
print("OK - MERGE silver_orders_events_dedup (source dedup)")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total
# MAGIC FROM workspace.ifood.silver_orders_events_dedup;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total
# MAGIC FROM workspace.ifood.silver_orders_events_dedup;
# MAGIC