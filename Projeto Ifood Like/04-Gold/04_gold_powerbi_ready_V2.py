# Databricks notebook source
spark.sql("""
CREATE OR REPLACE TABLE workspace.ifood.gold_orders_fact
USING DELTA
AS
SELECT
  s.order_id,
  s.customer_id,
  s.merchant_id,
  s.city,
  s.payment_method,
  s.order_status,

  -- datas importantes
  c.created_ts,
  s.last_event_ts,

  -- métricas
  s.items_count,
  s.subtotal,
  s.delivery_fee,
  s.discount,
  s.total,

  -- auditoria
  s.last_event_type,
  s.updated_at
FROM workspace.ifood.silver_orders_state s
LEFT JOIN (
  SELECT
    order_id,
    MIN(event_ts) AS created_ts
  FROM workspace.ifood.silver_orders_events_dedup
  GROUP BY order_id
) c
ON s.order_id = c.order_id
""")

print("OK - gold_orders_fact criada")


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.ifood.dim_city
USING DELTA
AS
SELECT DISTINCT
  city,
  'BR' AS country
FROM workspace.ifood.gold_orders_fact
WHERE city IS NOT NULL
""")

print("OK - dim_city")


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.ifood.dim_merchant
USING DELTA
AS
SELECT DISTINCT
  merchant_id,
  city
FROM workspace.ifood.gold_orders_fact
WHERE merchant_id IS NOT NULL
""")

print("OK - dim_merchant")


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.ifood.gold_orders_minute
USING DELTA
AS
SELECT
  date_trunc('minute', created_ts) AS minute_ts,
  city,
  COUNT(*)              AS orders_created,
  SUM(total)            AS gross_total,
  AVG(total)            AS avg_ticket
FROM workspace.ifood.gold_orders_fact
WHERE created_ts IS NOT NULL
GROUP BY date_trunc('minute', created_ts), city
""")

print("OK - gold_orders_minute")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- agregado operacional
# MAGIC SELECT *
# MAGIC FROM workspace.ifood.gold_orders_minute
# MAGIC LIMIT 20;