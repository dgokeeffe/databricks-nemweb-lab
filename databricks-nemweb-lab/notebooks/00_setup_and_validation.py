# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup and Validation
# MAGIC
# MAGIC **Time:** 5 minutes (validation) + ~5 minutes (data pre-load)
# MAGIC
# MAGIC This notebook validates your environment and pre-loads NEMWEB data for the lab exercises.
# MAGIC
# MAGIC ## Requirements
# MAGIC - **Serverless compute** (recommended) - Environment Version 4 has Spark 4.0
# MAGIC - Or DBR 15.4+ cluster (Python Data Source API requires Spark 4.0)
# MAGIC - Internet access to nemweb.com.au
# MAGIC - Unity Catalog enabled workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cluster Runtime Validation

# COMMAND ----------

import sys
print(f"Python version: {sys.version}")

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark_version = spark.version
print(f"Spark version: {spark_version}")

# Validate Spark 4.0+ for Python Data Source API
major_version = int(spark_version.split(".")[0])
if major_version < 4:
    raise RuntimeError(
        f"This lab requires Spark 4.0+. Current version: {spark_version}\n"
        "Options:\n"
        "  1. Use Serverless compute (Environment Version 4)\n"
        "  2. Use a cluster with DBR 15.4+"
    )
else:
    print("✓ Spark version is compatible with Python Data Source API")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Python Data Source API Availability

# COMMAND ----------

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    print("✓ Python Data Source API is available")
except ImportError as e:
    raise ImportError(
        "Python Data Source API not available. "
        "This feature requires DBR 15.4+ / Spark 4.0+"
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Network Connectivity to NEMWEB

# COMMAND ----------

import urllib.request
import urllib.error

NEMWEB_TEST_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/"

try:
    request = urllib.request.Request(
        NEMWEB_TEST_URL,
        headers={"User-Agent": "DatabricksNemwebLab/1.0"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        status = response.status
        if status == 200:
            print(f"✓ NEMWEB connectivity verified (HTTP {status})")
        else:
            print(f"⚠ NEMWEB returned unexpected status: {status}")
except urllib.error.URLError as e:
    raise RuntimeError(
        f"Cannot connect to NEMWEB: {e}\n"
        "Ensure your cluster has internet access and nemweb.com.au is not blocked."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lab Source Code Installation

# COMMAND ----------

import sys
import os
from pathlib import Path

# Add the src directory to Python path
# Get the notebook path and navigate to src
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    src_path = "/Workspace" + str(Path(notebook_path).parent.parent / "src")
except:
    # Fallback for local development
    src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath("."))), "src")

if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(f"Source path: {src_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Import of Lab Modules

# COMMAND ----------

# Add src directory to path for imports
import sys
sys.path.append("../src")

try:
    from nemweb_utils import fetch_nemweb_data, get_nemweb_schema, get_nem_regions
    print("✓ nemweb_utils imported successfully")

    regions = get_nem_regions()
    print(f"  Available NEM regions: {regions}")

except ImportError as e:
    print(f"⚠ Could not import lab modules: {e}")
    print("  This is OK - exercises include inline code")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register Custom Data Source

# COMMAND ----------

try:
    from nemweb_datasource import NemwebDataSource
    spark.dataSource.register(NemwebDataSource)
    print("✓ NEMWEB custom data source registered")
except ImportError as e:
    print(f"⚠ Could not register data source: {e}")
    print("  You'll register it manually in exercise 01")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Environment Summary

# COMMAND ----------

print("=" * 60)
print("ENVIRONMENT VALIDATION COMPLETE")
print("=" * 60)
print(f"Python:        {sys.version.split()[0]}")
print(f"Spark:         {spark.version}")
print(f"Cluster:       {spark.sparkContext.applicationId}")
print(f"NEMWEB Access: Verified")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Data Pre-Loading for Optimization Exercise
# MAGIC
# MAGIC The following section pre-loads NEMWEB data into a Delta table for use in Exercise 03 (Optimization Comparison).
# MAGIC This avoids slow HTTP fetching during the interactive exercise.
# MAGIC
# MAGIC **Run this section once before the workshop to prepare the data.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Lab configuration
CATALOG = "main"
SCHEMA = "nemweb_lab"
RAW_TABLE = "nemweb_raw"

# Data range - adjust based on how much data you want
# More data = better optimization comparison, but longer load time
START_DATE = "2024-01-01"
END_DATE = "2024-06-30"  # 6 months
REGIONS = "NSW1,QLD1,SA1,VIC1,TAS1"

print(f"Target table: {CATALOG}.{SCHEMA}.{RAW_TABLE}")
print(f"Date range: {START_DATE} to {END_DATE}")
print(f"Regions: {REGIONS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"✓ Using schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if Data Already Exists

# COMMAND ----------

table_exists = spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{RAW_TABLE}")

if table_exists:
    existing_count = spark.table(f"{CATALOG}.{SCHEMA}.{RAW_TABLE}").count()
    print(f"⚠ Table {RAW_TABLE} already exists with {existing_count:,} rows")
    print("  Set FORCE_RELOAD = True below to reload data")
    FORCE_RELOAD = False
else:
    print(f"Table {RAW_TABLE} does not exist - will create and load data")
    FORCE_RELOAD = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load NEMWEB Data Using Custom Data Source
# MAGIC
# MAGIC This fetches data from NEMWEB and stores it in a Delta table.
# MAGIC Takes ~3-10 minutes depending on date range and network speed.

# COMMAND ----------

if FORCE_RELOAD or not table_exists:
    print("Loading NEMWEB data...")
    print(f"This may take several minutes for {START_DATE} to {END_DATE}")
    print("-" * 50)

    import time
    start_time = time.time()

    # Ensure data source is registered
    try:
        from nemweb_datasource import NemwebDataSource
        spark.dataSource.register(NemwebDataSource)
    except:
        pass  # Already registered

    # Fetch data using custom data source
    nemweb_df = (
        spark.read
        .format("nemweb")
        .option("table", "DISPATCHREGIONSUM")
        .option("regions", REGIONS)
        .option("start_date", START_DATE)
        .option("end_date", END_DATE)
        .load()
    )

    # Drop existing table if reloading
    if FORCE_RELOAD and table_exists:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{RAW_TABLE}")

    # Write to Delta table
    nemweb_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{RAW_TABLE}")

    elapsed = time.time() - start_time
    row_count = spark.table(f"{CATALOG}.{SCHEMA}.{RAW_TABLE}").count()

    print("-" * 50)
    print(f"✓ Loaded {row_count:,} rows in {elapsed:.1f} seconds")
    print(f"✓ Data saved to: {CATALOG}.{SCHEMA}.{RAW_TABLE}")
else:
    print(f"✓ Using existing data in {CATALOG}.{SCHEMA}.{RAW_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Pre-loaded Data

# COMMAND ----------

# Show data summary
raw_df = spark.table(f"{CATALOG}.{SCHEMA}.{RAW_TABLE}")

print(f"Table: {CATALOG}.{SCHEMA}.{RAW_TABLE}")
print(f"Row count: {raw_df.count():,}")
print(f"\nSchema:")
raw_df.printSchema()

print("\nSample data:")
raw_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Statistics

# COMMAND ----------

from pyspark.sql.functions import min, max, count, countDistinct

stats = raw_df.agg(
    min("SETTLEMENTDATE").alias("min_date"),
    max("SETTLEMENTDATE").alias("max_date"),
    countDistinct("REGIONID").alias("regions"),
    count("*").alias("total_rows")
).collect()[0]

print("Data Statistics:")
print("-" * 40)
print(f"Date range:    {stats['min_date']} to {stats['max_date']}")
print(f"Regions:       {stats['regions']}")
print(f"Total rows:    {stats['total_rows']:,}")
print(f"Rows per day:  ~{stats['total_rows'] // 180:,} (approx)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!

# COMMAND ----------

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"""
Environment:
  - Spark {spark.version} ✓
  - Python Data Source API ✓
  - NEMWEB connectivity ✓
  - Custom data source registered ✓

Pre-loaded Data:
  - Table: {CATALOG}.{SCHEMA}.{RAW_TABLE}
  - Rows: {spark.table(f'{CATALOG}.{SCHEMA}.{RAW_TABLE}').count():,}
  - Ready for Exercise 03 (Optimization Comparison)

Next Steps:
  1. Open notebook 01_custom_source_exercise.py
  2. Follow the exercises in order
  3. Exercise 03 will use the pre-loaded data above
""")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### "Python Data Source API not available"
# MAGIC - Ensure your cluster is running DBR 15.4 or later
# MAGIC - Restart the cluster after changing runtime version
# MAGIC
# MAGIC ### "Cannot connect to NEMWEB"
# MAGIC - Check cluster has internet access
# MAGIC - Verify firewall rules allow HTTPS to nemweb.com.au
# MAGIC - Try accessing https://www.nemweb.com.au in a browser
# MAGIC
# MAGIC ### "Data loading is slow"
# MAGIC - NEMWEB is rate-limited; fetching is sequential
# MAGIC - Reduce date range for faster loading
# MAGIC - Pre-load data before the workshop session
# MAGIC
# MAGIC ### "Could not import lab modules"
# MAGIC - Verify the lab files are uploaded to your workspace
# MAGIC - The exercise notebooks include inline code as fallback
