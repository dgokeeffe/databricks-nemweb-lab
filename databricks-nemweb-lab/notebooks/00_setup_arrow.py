# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup - Arrow Version
# MAGIC
# MAGIC **Time:** ~5 minutes (validation) + ~5 minutes (data download)
# MAGIC
# MAGIC This notebook uses the Arrow-based custom data source which provides:
# MAGIC - Zero-copy transfer to Spark via PyArrow RecordBatch
# MAGIC - Native timestamp handling (no Python datetime serialization issues)
# MAGIC - Works on both Classic and Serverless compute
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Validates cluster runtime (Spark 4.0+ required)
# MAGIC 2. Tests network connectivity to NEMWEB
# MAGIC 3. Builds and installs the lab package
# MAGIC 4. Downloads NEMWEB data to UC Volume (parallel)
# MAGIC 5. Loads data into Delta table using Arrow-based custom data source
# MAGIC
# MAGIC ## Requirements
# MAGIC - **Serverless compute** (recommended) or DBR 15.4+ cluster
# MAGIC - Internet access to nemweb.com.au
# MAGIC - Unity Catalog enabled workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Lab configuration - modify CATALOG if needed for your workspace
CATALOG = "main"
SCHEMA = "nemweb_lab"
RAW_TABLE = "nemweb_raw_arrow"  # Different table to avoid conflicts
VOLUME_NAME = "raw_files"

# Date range for data loading (last 6 months)
from datetime import datetime, timedelta

END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

# Derived paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"

print("Lab Configuration (Arrow Version)")
print("=" * 50)
print(f"Catalog:      {CATALOG}")
print(f"Schema:       {SCHEMA}")
print(f"Table:        {RAW_TABLE}")
print(f"Volume:       {VOLUME_PATH}")
print(f"Date range:   {START_DATE} to {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Cluster Runtime

# COMMAND ----------

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print(f"Python version: {sys.version.split()[0]}")
print(f"Spark version:  {spark.version}")

# Validate Spark 4.0+ for Python Data Source API
major_version = int(spark.version.split(".")[0])
if major_version < 4:
    raise RuntimeError(
        f"This lab requires Spark 4.0+. Current version: {spark.version}\n"
        "Use Serverless compute or DBR 15.4+ cluster."
    )

print("✓ Spark version compatible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Python Data Source API and PyArrow

# COMMAND ----------

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    print("✓ Python Data Source API available")
except ImportError as e:
    raise ImportError(
        "Python Data Source API not available. Requires DBR 15.4+ / Spark 4.0+"
    ) from e

try:
    import pyarrow as pa
    print(f"✓ PyArrow version: {pa.__version__}")
except ImportError as e:
    raise ImportError("PyArrow not available - required for Arrow data source") from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Network Connectivity

# COMMAND ----------

import urllib.request
import urllib.error

NEMWEB_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/"

try:
    request = urllib.request.Request(NEMWEB_URL, headers={"User-Agent": "DatabricksLab/1.0"})
    with urllib.request.urlopen(request, timeout=10) as response:
        print(f"✓ NEMWEB connectivity verified (HTTP {response.status})")
except urllib.error.URLError as e:
    raise RuntimeError(f"Cannot connect to NEMWEB: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build and Install Lab Package

# COMMAND ----------

import os
import subprocess
import glob

# Get workspace paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
workspace_root = f"/Workspace{repo_root}"
src_path = f"{workspace_root}/src"

print(f"Repository: {workspace_root}")
print(f"Source:     {src_path}")

# COMMAND ----------

# Build wheel
print("Building wheel...")
result = subprocess.run(
    f"cd {workspace_root} && uv build src --wheel",
    shell=True, capture_output=True, text=True
)

if result.returncode != 0:
    print(f"Build output: {result.stdout}")
    print(f"Build errors: {result.stderr}")
    raise RuntimeError("Wheel build failed")

# Find wheel
wheels = glob.glob(f"{src_path}/dist/nemweb_datasource-*.whl")
if not wheels:
    raise FileNotFoundError("No wheel found after build")

latest_wheel = sorted(wheels)[-1]
print(f"✓ Built: {os.path.basename(latest_wheel)}")

# COMMAND ----------

# Install wheel
%pip install $latest_wheel --force-reinstall -q

# COMMAND ----------

# Restart Python to load new package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Package Installation

# COMMAND ----------

# Re-import after restart
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Verify imports
from nemweb_utils import get_version, get_nem_regions
from nemweb_ingest import download_nemweb_files
from nemweb_datasource_arrow import NemwebArrowDataSource

print(f"✓ Package version: {get_version()}")
print(f"✓ NEM regions: {get_nem_regions()}")

# Register Arrow data source
spark.dataSource.register(NemwebArrowDataSource)
print("✓ Arrow data source registered (format: 'nemweb_arrow')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Schema and Volume

# COMMAND ----------

# Re-define config after restart
CATALOG = "main"
SCHEMA = "nemweb_lab"
RAW_TABLE = "nemweb_raw_arrow"
VOLUME_NAME = "raw_files"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"

from datetime import datetime, timedelta
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

# Create catalog, schema, volume
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

print(f"✓ Schema: {CATALOG}.{SCHEMA}")
print(f"✓ Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Check Existing Data

# COMMAND ----------

table_exists = spark.catalog.tableExists(TABLE_PATH)

if table_exists:
    row_count = spark.table(TABLE_PATH).count()
    print(f"Table {RAW_TABLE} exists with {row_count:,} rows")
    print("Set FORCE_RELOAD = True to reload")
    FORCE_RELOAD = False
else:
    print(f"Table {RAW_TABLE} does not exist - will load data")
    FORCE_RELOAD = True

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Download NEMWEB Files to UC Volume
# MAGIC
# MAGIC Downloads ZIP files in parallel (8 threads) to the UC Volume.

# COMMAND ----------

if FORCE_RELOAD or not table_exists:
    from nemweb_ingest import download_nemweb_files

    print(f"Downloading NEMWEB data: {START_DATE} to {END_DATE}")
    print("=" * 50)

    results = download_nemweb_files(
        volume_path=VOLUME_PATH,
        table="DISPATCHREGIONSUM",
        start_date=START_DATE,
        end_date=END_DATE,
        max_workers=8,
        skip_existing=True
    )
else:
    print("✓ Skipping download - data exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Load Data Using Arrow Custom Data Source
# MAGIC
# MAGIC This uses the Arrow-based data source which:
# MAGIC - Returns PyArrow RecordBatch objects for zero-copy transfer
# MAGIC - Handles timestamps natively in Arrow format
# MAGIC - Works on both Serverless and Classic compute

# COMMAND ----------

if FORCE_RELOAD or not table_exists:
    import time

    start_time = time.time()

    print("Loading data using Arrow data source...")
    print("=" * 50)

    # Read using Arrow-based custom data source
    df = (spark.read
          .format("nemweb_arrow")
          .option("volume_path", VOLUME_PATH)
          .option("table", "DISPATCHREGIONSUM")
          .load())

    # Write to Delta
    if table_exists:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_PATH}")

    df.write.format("delta").mode("overwrite").saveAsTable(TABLE_PATH)

    elapsed = time.time() - start_time
    row_count = spark.table(TABLE_PATH).count()

    print("=" * 50)
    print(f"✓ Loaded {row_count:,} rows in {elapsed:.1f}s")
else:
    print("✓ Using existing data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Verify Loaded Data

# COMMAND ----------

from databricks.sdk.runtime import display
from pyspark.sql.functions import min, max, countDistinct

df = spark.table(TABLE_PATH)

print(f"Table: {TABLE_PATH}")
print(f"Rows:  {df.count():,}")
print()

# Statistics
stats = df.agg(
    min("SETTLEMENTDATE").alias("min_date"),
    max("SETTLEMENTDATE").alias("max_date"),
    countDistinct("REGIONID").alias("regions")
).collect()[0]

print(f"Date range: {stats['min_date']} to {stats['max_date']}")
print(f"Regions:    {stats['regions']}")
print()

# Verify timestamp type is correct
print("Schema:")
df.printSchema()
print()

print("Sample data:")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!

# COMMAND ----------

from nemweb_utils import get_version

print("=" * 60)
print("SETUP COMPLETE (Arrow Version)")
print("=" * 60)
print(f"""
Environment:
  Spark:    {spark.version}
  Package:  v{get_version()}

Data:
  Table:    {TABLE_PATH}
  Rows:     {spark.table(TABLE_PATH).count():,}

Arrow Data Source Benefits:
  - Zero-copy transfer via PyArrow RecordBatch
  - Native timestamp handling (no Python datetime issues)
  - Works on Serverless and Classic compute

Next Steps:
  1. Open 01_custom_source_exercise.py
  2. Follow exercises in order
  3. Exercise 03 uses the pre-loaded data
""")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC **"Python Data Source API not available"**
# MAGIC - Use Serverless compute or DBR 15.4+ cluster
# MAGIC
# MAGIC **"Cannot connect to NEMWEB"**
# MAGIC - Check cluster has internet access
# MAGIC - Verify nemweb.com.au is not blocked
# MAGIC
# MAGIC **"Wheel build failed"**
# MAGIC - Ensure uv is available: `%sh which uv`
# MAGIC - Check src directory exists
# MAGIC
# MAGIC **Data loading slow**
# MAGIC - Downloads are parallel (8 threads)
# MAGIC - Skip existing files with `skip_existing=True`
# MAGIC - Reduce date range for faster loading
