# Databricks notebook source
# ===== PATHS DO PROJETO (IGUAL AO 00_setup_paths) =====
base = "/Volumes/workspace/ifood/ifood_vol/ifood_like"

landing_dir = f"{base}/landing/orders"


# COMMAND ----------

import random, time
from datetime import datetime, timezone
from pyspark.sql import Row

cities = ["Sao Paulo", "Campinas", "Santos", "Guarulhos", "Osasco"]
payment = ["CARD", "PIX", "CASH"]
status_flow = ["PLACED", "CONFIRMED", "PREPARING", "DISPATCHED", "DELIVERED"]

order_seq = 100000
event_seq = 1
active_orders = {}

def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def next_order_id():
    global order_seq
    order_seq += 1
    return f"o-{order_seq}"

def next_event_id():
    global event_seq
    eid = f"e-{event_seq:06d}"
    event_seq += 1
    return eid

def make_created():
    order_id = next_order_id()
    customer_id = random.randint(100, 999)
    merchant_id = random.randint(5000, 5099)
    city = random.choice(cities)
    pay = random.choice(payment)
    items = random.randint(1, 6)
    subtotal = round(random.uniform(20, 180), 2)
    delivery_fee = round(random.uniform(4, 15), 2)
    discount = round(random.choice([0, 0, 0, 5, 10, 15]), 2)
    total = round(subtotal + delivery_fee - discount, 2)

    base = dict(
        order_id=order_id,
        customer_id=customer_id,
        merchant_id=merchant_id,
        city=city,
        payment_method=pay,
        items_count=items,
        subtotal=subtotal,
        delivery_fee=delivery_fee,
        discount=discount,
        total=total,
        order_status="PLACED"
    )

    active_orders[order_id] = base
    ts = iso_now()

    return Row(
        event_id=next_event_id(),
        event_type="CREATED",
        event_ts=ts,
        updated_at=ts,
        **base
    )

def make_update():
    if not active_orders:
        return make_created()

    order_id = random.choice(list(active_orders.keys()))
    base = active_orders[order_id].copy()

    # chance de cancelamento
    if random.random() < 0.05 and base["order_status"] not in ["DELIVERED", "CANCELLED"]:
        base["order_status"] = "CANCELLED"
        active_orders[order_id] = base
        ts = iso_now()

        return Row(
            event_id=next_event_id(),
            event_type="CANCELLED",
            event_ts=ts,
            updated_at=ts,
            **base
        )

    # avança status
    if base["order_status"] in status_flow:
        i = status_flow.index(base["order_status"])
        if i < len(status_flow) - 1:
            base["order_status"] = status_flow[i + 1]

    active_orders[order_id] = base
    ts = iso_now()

    return Row(
        event_id=next_event_id(),
        event_type="UPDATED_STATUS",
        event_ts=ts,
        updated_at=ts,
        **base
    )

batch_seconds = 5
rows_per_file = 50

while True:
    rows = [
        make_created() if random.random() < 0.35 else make_update()
        for _ in range(rows_per_file)
    ]

    df = spark.createDataFrame(rows)

    file_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    out_path = f"{landing_dir}/batch_{file_ts}"

    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(out_path))

    print(f"Gerado: {out_path} com {rows_per_file} eventos")
    time.sleep(batch_seconds)
