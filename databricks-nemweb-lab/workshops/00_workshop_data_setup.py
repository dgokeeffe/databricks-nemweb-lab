# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop data setup
# MAGIC
# MAGIC This notebook prepares the schema and volume for the NEMWEB MMS pipeline.
# MAGIC The actual data loading is done by the MMS pipeline which runs after this notebook.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Creates the target catalog and schema
# MAGIC 2. Creates a volume for raw NEMWEB files
# MAGIC 3. Verifies the custom data source package is installed
# MAGIC
# MAGIC ## What the MMS pipeline loads
# MAGIC - `bronze_dispatch_regionsum` - 5-min regional demand/generation
# MAGIC - `bronze_dispatch_price` - 5-min spot prices
# MAGIC - `bronze_trading_price` - 30-min trading prices
# MAGIC - `bronze_dispatch_unit_scada` - Real-time unit generation
# MAGIC - `bronze_p5min_regionsolution` - Pre-dispatch forecasts
# MAGIC - `bronze_bidperoffer` - Generator bid availability
# MAGIC - `bronze_biddayoffer` - Generator price bands
# MAGIC - Plus silver/gold aggregations

# COMMAND ----------

from datetime import datetime, timedelta

# Configuration
dbutils.widgets.text("catalog", "daveok", "Catalog Name")
dbutils.widgets.text("schema", "ml_workshops", "Schema Name")
dbutils.widgets.text("days_history", "30", "Days of History")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DAYS_HISTORY = int(dbutils.widgets.get("days_history"))

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Days history: {DAYS_HISTORY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog, schema, and volume

# COMMAND ----------

# Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Create volume for raw files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify the custom data source package
# MAGIC
# MAGIC The nemweb_datasource wheel is installed via the job environment.
# MAGIC We verify it's importable here. The actual data source registration
# MAGIC happens in the Lakeflow pipeline (where it's supported).

# COMMAND ----------

# Verify the package is installed
try:
    from nemweb_datasource_arrow import NemwebArrowDataSource
    from nemweb_utils import get_version, get_nem_regions, TABLE_CONFIG

    print(f"nemweb_datasource version: {get_version()}")
    print(f"NEM regions: {get_nem_regions()}")
    print(f"Supported tables: {list(TABLE_CONFIG.keys())}")
    print("\nPackage verified - data source will be registered by the MMS pipeline")

except ImportError as e:
    print(f"ERROR: nemweb_datasource not installed: {e}")
    print("The wheel should be installed via the job environment configuration.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test connectivity to NEMWEB

# COMMAND ----------

import urllib.request

NEMWEB_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/"

try:
    request = urllib.request.Request(NEMWEB_URL, headers={"User-Agent": "DatabricksWorkshop/1.0"})
    with urllib.request.urlopen(request, timeout=10) as response:
        print(f"NEMWEB connectivity: OK (HTTP {response.status})")
except Exception as e:
    print(f"WARNING: NEMWEB connectivity issue: {e}")
    print("The pipeline may still work if files are cached in the volume.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC The schema and volume are ready. The MMS pipeline task will run next to load all NEMWEB tables.
# MAGIC
# MAGIC **Tables that will be created by the MMS pipeline:**
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `bronze_dispatch_regionsum` | 5-min regional demand/generation |
# MAGIC | Bronze | `bronze_dispatch_price` | 5-min spot prices (RRP) |
# MAGIC | Bronze | `bronze_trading_price` | 30-min trading period prices |
# MAGIC | Bronze | `bronze_dispatch_unit_scada` | Real-time unit SCADA generation |
# MAGIC | Bronze | `bronze_p5min_regionsolution` | Pre-dispatch regional forecasts |
# MAGIC | Bronze | `bronze_p5min_interconnectorsoln` | Interconnector flow forecasts |
# MAGIC | Bronze | `bronze_bidperoffer` | Generator bid availability |
# MAGIC | Bronze | `bronze_biddayoffer` | Generator daily price bands |
# MAGIC | Silver | `silver_dispatch_with_price` | Joined dispatch + price |
# MAGIC | Silver | `silver_unit_generation` | Cleansed unit generation |
# MAGIC | Gold | `gold_hourly_generation_by_unit` | Hourly generation aggregations |
