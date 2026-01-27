# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Understanding the NEMWEB Arrow Data Source
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll explore and use our production-ready custom PySpark data source
# MAGIC for AEMO NEMWEB electricity market data. The data source uses PyArrow for efficient
# MAGIC data transfer and supports multiple tables from the Australian National Electricity Market.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand how the Python Data Source API works in production
# MAGIC 2. Explore the schema and table configurations
# MAGIC 3. Use the data source with different options and modes
# MAGIC 4. Add support for a new MMS table
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC - [Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [MMS Data Model Report](https://nemweb.com.au/Reports/Current/MMSDataModelReport/)
# MAGIC - [PyArrow Documentation](https://arrow.apache.org/docs/python/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Explore the Data Source Code (5 minutes)
# MAGIC
# MAGIC Let's start by examining how the production data source is structured.
# MAGIC The source code is in `src/nemweb_datasource_arrow.py`.

# COMMAND ----------

from databricks.sdk.runtime import spark, display, dbutils
from pyspark.sql.functions import col, count, avg, max, min

# Import the production data source
from nemweb_datasource_arrow import (
    NemwebArrowDataSource,
    NemwebArrowReader,
    SCHEMAS,
    TABLE_TO_FOLDER,
)

# Register the data source
spark.dataSource.register(NemwebArrowDataSource)

print("Data source registered: nemweb_arrow")
print(f"\nSupported tables: {list(SCHEMAS.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore the Schema Definitions
# MAGIC
# MAGIC The data source supports multiple MMS tables, each with its own schema.
# MAGIC Schemas are defined in the `SCHEMAS` dictionary.

# COMMAND ----------

# Examine the DISPATCHREGIONSUM schema
print("DISPATCHREGIONSUM Schema:")
print("-" * 50)
for field_name, field_type in SCHEMAS["DISPATCHREGIONSUM"]["fields"]:
    print(f"  {field_name}: {type(field_type).__name__}")

print(f"\nRecord type filter: {SCHEMAS['DISPATCHREGIONSUM']['record_type']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.1: Explore DISPATCHPRICE Schema
# MAGIC
# MAGIC Examine the DISPATCHPRICE schema to understand the pricing data structure.

# COMMAND ----------

# TODO 1.1: Print the DISPATCHPRICE schema fields
# Hint: Use the same pattern as above but with "DISPATCHPRICE"

print("DISPATCHPRICE Schema:")
print("-" * 50)
# YOUR CODE HERE - iterate through SCHEMAS["DISPATCHPRICE"]["fields"]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding TABLE_TO_FOLDER Mapping
# MAGIC
# MAGIC NEMWEB organizes files into folders. Multiple tables can share the same
# MAGIC folder (e.g., DISPATCHREGIONSUM and DISPATCHPRICE both come from DispatchIS_Reports).

# COMMAND ----------

print("Table to Folder Mapping:")
print("-" * 50)
for table, (folder, prefix) in TABLE_TO_FOLDER.items():
    print(f"  {table}:")
    print(f"    Folder: {folder}")
    print(f"    File prefix: PUBLIC_{prefix}_YYYYMMDD.zip")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Using the Data Source (5 minutes)
# MAGIC
# MAGIC The data source supports three modes:
# MAGIC 1. **Volume mode**: Read from pre-downloaded files (fastest)
# MAGIC 2. **Auto-download mode**: Download to volume, then read (recommended for production)
# MAGIC 3. **HTTP mode**: Fetch directly via HTTP (for development)
# MAGIC
# MAGIC Let's try each mode!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 1: HTTP Mode (Development)
# MAGIC
# MAGIC Fetch data directly from NEMWEB - good for quick testing.

# COMMAND ----------

# Read a small date range via HTTP (uses sample data for speed)
df_http = (spark.read
    .format("nemweb_arrow")
    .option("table", "DISPATCHREGIONSUM")
    .option("regions", "NSW1")  # Single region for speed
    .option("start_date", "2024-07-01")
    .option("end_date", "2024-07-01")  # Single day
    .load())

print(f"HTTP mode: Retrieved {df_http.count()} rows")
display(df_http.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.2: Fetch DISPATCHPRICE Data
# MAGIC
# MAGIC Use the data source to fetch pricing data for Victoria.

# COMMAND ----------

# TODO 1.2: Read DISPATCHPRICE for VIC1 region
# Use HTTP mode with a single day date range

df_price = (spark.read
    .format("nemweb_arrow")
    # TODO: Add options for:
    # - table: "DISPATCHPRICE"
    # - regions: "VIC1"
    # - start_date and end_date: "2024-07-01"
    .load())

print(f"Price data rows: {df_price.count()}")
# display(df_price.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 2: Volume Mode with Auto-Download (Production)
# MAGIC
# MAGIC For production, download files once to a UC Volume, then read from there.
# MAGIC This avoids HTTP calls during Spark job execution.

# COMMAND ----------

# Configuration
CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "workspace"
SCHEMA = "nemweb_lab"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# Auto-download mode: Downloads to volume first, then reads
# This is the recommended production pattern
df_volume = (spark.read
    .format("nemweb_arrow")
    .option("volume_path", VOLUME_PATH)
    .option("table", "DISPATCHREGIONSUM")
    .option("start_date", "2024-07-01")
    .option("end_date", "2024-07-03")  # 3 days
    .option("auto_download", "true")
    .option("max_workers", "8")  # Parallel downloads
    .load())

print(f"Volume mode: Retrieved {df_volume.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze the Data
# MAGIC
# MAGIC Let's do some basic analysis of the electricity market data.

# COMMAND ----------

# Analyze demand by region
demand_summary = (df_volume
    .groupBy("REGIONID")
    .agg(
        count("*").alias("intervals"),
        avg("TOTALDEMAND").alias("avg_demand_mw"),
        max("TOTALDEMAND").alias("max_demand_mw"),
        min("TOTALDEMAND").alias("min_demand_mw")
    )
    .orderBy("REGIONID"))

display(demand_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Extend the Data Source (5 minutes)
# MAGIC
# MAGIC Now let's add support for a new MMS table: **DISPATCH_UNIT_SCADA**
# MAGIC
# MAGIC This table contains real-time generation output from individual power stations.
# MAGIC
# MAGIC > **Reference:** [MMS Data Model - DISPATCH_UNIT_SCADA](https://nemweb.com.au/Reports/Current/MMSDataModelReport/)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.3: Define Schema for DISPATCH_UNIT_SCADA
# MAGIC
# MAGIC The DISPATCH_UNIT_SCADA table has these key fields:
# MAGIC - SETTLEMENTDATE (Timestamp)
# MAGIC - DUID (String) - Dispatchable Unit ID (power station)
# MAGIC - SCADAVALUE (Double) - Current output in MW
# MAGIC
# MAGIC Add the schema to the SCHEMAS dictionary pattern.

# COMMAND ----------

from pyspark.sql.types import TimestampType, StringType, DoubleType

# TODO 1.3: Define the DISPATCH_UNIT_SCADA schema
# Follow the same pattern as DISPATCHREGIONSUM
DISPATCH_UNIT_SCADA_CONFIG = {
    "record_type": "DISPATCH,UNIT_SCADA",
    "fields": [
        # TODO: Add fields:
        # ("SETTLEMENTDATE", TimestampType()),
        # ("DUID", StringType()),
        # ("SCADAVALUE", DoubleType()),
    ],
}

# Verify your schema
print("DISPATCH_UNIT_SCADA Schema:")
for field_name, field_type in DISPATCH_UNIT_SCADA_CONFIG["fields"]:
    print(f"  {field_name}: {type(field_type).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.4: Add Table Mapping
# MAGIC
# MAGIC DISPATCH_UNIT_SCADA comes from the Dispatch_SCADA folder with
# MAGIC file prefix DISPATCHSCADA.

# COMMAND ----------

# TODO 1.4: Define the folder mapping for DISPATCH_UNIT_SCADA
# Pattern: (folder_name, file_prefix)
# Folder: "Dispatch_SCADA"
# Prefix: "DISPATCHSCADA"

DISPATCH_UNIT_SCADA_FOLDER = (
    # YOUR CODE HERE
    # ("Dispatch_SCADA", "DISPATCHSCADA")
)

print(f"Folder mapping: {DISPATCH_UNIT_SCADA_FOLDER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Run this cell to check your understanding:

# COMMAND ----------

def validate_exercise():
    """Validate Exercise 1 completion."""
    print("=" * 60)
    print("EXERCISE 1 VALIDATION")
    print("=" * 60)

    checks = {
        "1.1 - Explored DISPATCHPRICE schema": False,
        "1.2 - Fetched price data": False,
        "1.3 - Defined DISPATCH_UNIT_SCADA schema": False,
        "1.4 - Defined folder mapping": False,
    }

    # Check 1.1: DISPATCHPRICE exploration
    if "DISPATCHPRICE" in SCHEMAS:
        checks["1.1 - Explored DISPATCHPRICE schema"] = True

    # Check 1.2: Price data fetched
    try:
        if df_price.count() > 0:
            checks["1.2 - Fetched price data"] = True
    except:
        pass

    # Check 1.3: DISPATCH_UNIT_SCADA schema
    if (DISPATCH_UNIT_SCADA_CONFIG.get("fields") and
        len(DISPATCH_UNIT_SCADA_CONFIG["fields"]) >= 3):
        field_names = [f[0] for f in DISPATCH_UNIT_SCADA_CONFIG["fields"]]
        if "SETTLEMENTDATE" in field_names and "DUID" in field_names:
            checks["1.3 - Defined DISPATCH_UNIT_SCADA schema"] = True

    # Check 1.4: Folder mapping
    if DISPATCH_UNIT_SCADA_FOLDER and len(DISPATCH_UNIT_SCADA_FOLDER) == 2:
        if "Dispatch_SCADA" in DISPATCH_UNIT_SCADA_FOLDER[0]:
            checks["1.4 - Defined folder mapping"] = True

    # Print results
    print()
    for check, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {check}")

    all_passed = all(checks.values())
    print()
    if all_passed:
        print("=" * 60)
        print("CONGRATULATIONS! Exercise 1 complete!")
        print("=" * 60)
    else:
        failed = [k for k, v in checks.items() if not v]
        print(f"Complete the remaining tasks: {', '.join(failed)}")

    return all_passed

validate_exercise()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've learned how the production NEMWEB data source works:
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `SCHEMAS` | Schema definitions for each MMS table |
# MAGIC | `TABLE_TO_FOLDER` | Maps tables to NEMWEB folder/file structure |
# MAGIC | `NemwebArrowReader` | Handles partitioning and data fetching |
# MAGIC | `NemwebArrowDataSource` | Main entry point registered with Spark |
# MAGIC
# MAGIC ### Key Options
# MAGIC | Option | Description |
# MAGIC |--------|-------------|
# MAGIC | `volume_path` | UC Volume for file storage |
# MAGIC | `table` | MMS table name (DISPATCHREGIONSUM, DISPATCHPRICE, etc.) |
# MAGIC | `auto_download` | Download files to volume before reading |
# MAGIC | `regions` | Filter by NEM region(s) |
# MAGIC | `start_date` / `end_date` | Date range to fetch |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Exercise 2** to integrate this data source with Lakeflow Pipelines.
