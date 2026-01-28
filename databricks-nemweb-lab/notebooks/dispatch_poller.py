# Databricks notebook source
# NEMWEB Dispatch Data Poller
# Polls for real-time prices, demand, and interconnector flows every 5 minutes

import sys
import os

# --- Parameters ---
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "nemweb_live", "Schema")
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

# --- Create schema and tables ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
region_table = f"{catalog}.{schema}.dispatch_region"
interconnector_table = f"{catalog}.{schema}.dispatch_interconnector"

print(f"Config: catalog={catalog}, schema={schema}, duration={duration_minutes}min, poll={poll_interval}s")
print(f"Tables: {region_table}, {interconnector_table if include_interconnectors else '(skipped)'}")
print("=" * 70)

# --- Callbacks ---
def save_region_data(data, interval_time):
    df = spark.createDataFrame(data)
    df.write.mode("append").saveAsTable(region_table)
    prices = {r["REGIONID"]: r["RRP"] for r in data}
    price_str = " | ".join(f"{k}: ${v:.2f}" for k, v in sorted(prices.items()))
    print(f"✓ {interval_time.strftime('%H:%M')} | {price_str}")

def save_interconnector_data(data, interval_time):
    df = spark.createDataFrame(data)
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
