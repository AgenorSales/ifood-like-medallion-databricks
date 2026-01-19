# Databricks notebook source
base = "/Volumes/workspace/ifood/ifood_vol/ifood_like"

landing_dir = f"{base}/landing/orders"
cp_bronze   = f"{base}/_checkpoints/bronze_orders"


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.ifood.bronze_orders_events (
  event_id STRING,
  order_id STRING,
  event_type STRING,
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


# COMMAND ----------

dbutils.fs.rm(cp_bronze, True)
print("Checkpoint resetado:", cp_bronze)


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, expr

# SCHEMA EXATAMENTE NA ORDEM REAL DO CSV
schema_str = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("items_count", StringType(), True),
    StructField("subtotal", StringType(), True),
    StructField("delivery_fee", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("total", StringType(), True),
    StructField("updated_at", StringType(), True),
])

raw = (
    spark.readStream
        .schema(schema_str)
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .csv(landing_dir)
)

bronze_df = (
    raw
    # timestamps tolerantes
    .withColumn("event_ts", expr("try_to_timestamp(event_ts, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"))
    .withColumn("updated_at", expr("try_to_timestamp(updated_at, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"))

    # casts tolerantes
    .withColumn("customer_id", expr("try_cast(customer_id as int)"))
    .withColumn("merchant_id", expr("try_cast(merchant_id as int)"))
    .withColumn("items_count", expr("try_cast(items_count as int)"))
    .withColumn("subtotal", expr("try_cast(subtotal as double)"))
    .withColumn("delivery_fee", expr("try_cast(delivery_fee as double)"))
    .withColumn("discount", expr("try_cast(discount as double)"))
    .withColumn("total", expr("try_cast(total as double)"))

    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)


# COMMAND ----------

valid = (
    bronze_df
    .filter(col("event_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("event_ts").isNotNull())
    .filter(col("order_status").isNotNull())
)

query = (
    valid.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cp_bronze)
        .trigger(availableNow=True)
        .toTable("workspace.ifood.bronze_orders_events")
)

query.awaitTermination()
print("Bronze AvailableNow finalizado.")


# COMMAND ----------

dbutils.fs.rm(cp_bronze, True)
print("Checkpoint resetado")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS total,
# MAGIC   min(event_ts) AS min_event,
# MAGIC   max(event_ts) AS max_event
# MAGIC FROM workspace.ifood.bronze_orders_events;
# MAGIC