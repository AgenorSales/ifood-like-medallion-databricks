# Databricks notebook source
spark.sql("""
CREATE TABLE workspace.ifood.silver_orders_state (
  order_id STRING,
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
  last_event_type STRING,
  last_event_ts TIMESTAMP,
  updated_at TIMESTAMP,
  ingestion_ts TIMESTAMP
)
USING DELTA
""")

print("OK - silver_orders_state criada com schema correto")


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW vw_latest_orders AS
SELECT
  order_id,
  customer_id,
  merchant_id,
  city,
  payment_method,
  order_status,
  items_count,
  subtotal,
  delivery_fee,
  discount,
  total,
  event_type AS last_event_type,
  event_ts   AS last_event_ts,
  updated_at,
  ingestion_ts
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY
        updated_at DESC,
        event_ts DESC,
        ingestion_ts DESC
    ) AS rn
  FROM workspace.ifood.silver_orders_events_dedup
  WHERE order_id IS NOT NULL
)
WHERE rn = 1
""")

print("OK - vw_latest_orders pronta")


# COMMAND ----------

spark.sql("""
MERGE INTO workspace.ifood.silver_orders_state AS tgt
USING vw_latest_orders AS src
ON tgt.order_id = src.order_id

WHEN MATCHED AND (
  src.updated_at > tgt.updated_at OR
  (src.updated_at = tgt.updated_at AND src.ingestion_ts > tgt.ingestion_ts)
) THEN UPDATE SET
  tgt.customer_id     = src.customer_id,
  tgt.merchant_id     = src.merchant_id,
  tgt.city            = src.city,
  tgt.payment_method  = src.payment_method,
  tgt.order_status    = src.order_status,
  tgt.items_count     = src.items_count,
  tgt.subtotal        = src.subtotal,
  tgt.delivery_fee    = src.delivery_fee,
  tgt.discount        = src.discount,
  tgt.total           = src.total,
  tgt.last_event_type = src.last_event_type,
  tgt.last_event_ts   = src.last_event_ts,
  tgt.updated_at      = src.updated_at,
  tgt.ingestion_ts    = src.ingestion_ts

WHEN NOT MATCHED THEN INSERT (
  order_id, customer_id, merchant_id, city, payment_method, order_status,
  items_count, subtotal, delivery_fee, discount, total,
  last_event_type, last_event_ts, updated_at, ingestion_ts
) VALUES (
  src.order_id, src.customer_id, src.merchant_id, src.city, src.payment_method, src.order_status,
  src.items_count, src.subtotal, src.delivery_fee, src.discount, src.total,
  src.last_event_type, src.last_event_ts, src.updated_at, src.ingestion_ts
)
""")

print("OK - MERGE silver_orders_state executado")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS pedidos
# MAGIC FROM workspace.ifood.silver_orders_state;
# MAGIC
# MAGIC SELECT order_status, count(*) AS qtd
# MAGIC FROM workspace.ifood.silver_orders_state
# MAGIC GROUP BY order_status
# MAGIC ORDER BY qtd DESC;
# MAGIC