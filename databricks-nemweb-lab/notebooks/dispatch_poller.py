# Databricks notebook source
# NEMWEB Dispatch Data Poller
# Polls for real-time prices, demand, and interconnector flows every 5 minutes

import sys
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- Parameters ---
dbutils.widgets.text("catalog", "agl", "Catalog")
dbutils.widgets.text("schema", "nemweb", "Schema")
dbutils.widgets.text("duration_minutes", "60", "Duration (minutes)")
dbutils.widgets.text("poll_interval", "30", "Poll interval (seconds)")
dbutils.widgets.dropdown("include_interconnectors", "true", ["true", "false"], "Include interconnectors")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
duration_minutes = int(dbutils.widgets.get("duration_minutes"))
poll_interval = int(dbutils.widgets.get("poll_interval"))
include_interconnectors = dbutils.widgets.get("include_interconnectors") == "true"

# --- Setup imports ---
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_dispatch import DispatchPoller

# --- Schemas for explicit typing (avoids CANNOT_DETERMINE_TYPE errors) ---
region_schema = StructType([
    StructField("SETTLEMENTDATE", TimestampType(), True),
    StructField("RUNNO", StringType(), True),
    StructField("REGIONID", StringType(), True),
    StructField("INTERVENTION", StringType(), True),
    StructField("RRP", DoubleType(), True),
    StructField("EEP", DoubleType(), True),
    StructField("ROP", DoubleType(), True),
    StructField("APCFLAG", StringType(), True),
    StructField("MARKETSUSPENDEDFLAG", StringType(), True),
    StructField("TOTALDEMAND", DoubleType(), True),
    StructField("DEMANDFORECAST", DoubleType(), True),
    StructField("DISPATCHABLEGENERATION", DoubleType(), True),
    StructField("DISPATCHABLELOAD", DoubleType(), True),
    StructField("NETINTERCHANGE", DoubleType(), True),
    StructField("AVAILABLEGENERATION", DoubleType(), True),
    StructField("AVAILABLELOAD", DoubleType(), True),
    StructField("CLEAREDSUPPLY", DoubleType(), True),
    StructField("RAISE6SECRRP", DoubleType(), True),
    StructField("RAISE60SECRRP", DoubleType(), True),
    StructField("RAISE5MINRRP", DoubleType(), True),
    StructField("LOWER6SECRRP", DoubleType(), True),
    StructField("LOWER60SECRRP", DoubleType(), True),
    StructField("LOWER5MINRRP", DoubleType(), True),
])

interconnector_schema = StructType([
    StructField("SETTLEMENTDATE", TimestampType(), True),
    StructField("RUNNO", StringType(), True),
    StructField("INTERCONNECTORID", StringType(), True),
    StructField("INTERVENTION", StringType(), True),
    StructField("METEREDMWFLOW", DoubleType(), True),
    StructField("MWFLOW", DoubleType(), True),
    StructField("MWLOSSES", DoubleType(), True),
    StructField("MARGINALVALUE", DoubleType(), True),
    StructField("VIOLATIONDEGREE", DoubleType(), True),
    StructField("IMPORTLIMIT", DoubleType(), True),
    StructField("EXPORTLIMIT", DoubleType(), True),
    StructField("MARGINALLOSS", DoubleType(), True),
    StructField("EXPORTGENCONID", StringType(), True),
    StructField("IMPORTGENCONID", StringType(), True),
])

# --- Create schema and tables ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
region_table = f"{catalog}.{schema}.dispatch_region"
interconnector_table = f"{catalog}.{schema}.dispatch_interconnector"

print(f"Config: catalog={catalog}, schema={schema}, duration={duration_minutes}min, poll={poll_interval}s")
print(f"Tables: {region_table}, {interconnector_table if include_interconnectors else '(skipped)'}")
print("=" * 70)

# --- Callbacks ---
def save_region_data(data, interval_time):
    df = spark.createDataFrame(data, schema=region_schema)
    df.write.mode("append").saveAsTable(region_table)
    prices = {r["REGIONID"]: r["RRP"] for r in data}
    price_str = " | ".join(f"{k}: ${v:.2f}" for k, v in sorted(prices.items()))
    print(f"✓ {interval_time.strftime('%H:%M')} | {price_str}")

def save_interconnector_data(data, interval_time):
    df = spark.createDataFrame(data, schema=interconnector_schema)
    df.write.mode("append").saveAsTable(interconnector_table)

# --- Run poller ---
poller = DispatchPoller(
    on_data=save_region_data,
    on_interconnector=save_interconnector_data if include_interconnectors else None,
    poll_interval=poll_interval,
    include_interconnectors=include_interconnectors,
    debug=True,
)

poller.run(duration_minutes=duration_minutes)
print("=" * 70)
print("Done.")
