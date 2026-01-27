# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup and Validation
# MAGIC
# MAGIC **Time:** ~10 minutes
# MAGIC
# MAGIC This notebook validates your environment and pre-loads NEMWEB data for the lab exercises.
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Validates cluster runtime (Spark 4.0+ required)
# MAGIC 2. Tests network connectivity to NEMWEB
# MAGIC 3. Builds and installs the lab package
# MAGIC 4. Downloads and loads data using the Arrow custom data source
# MAGIC
# MAGIC ## Requirements
# MAGIC - **Serverless compute** (recommended) or DBR 15.4+ cluster
# MAGIC - Internet access to nemweb.com.au
# MAGIC - Unity Catalog enabled workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Parameters can be passed via job configuration or set interactively.

# COMMAND ----------

from datetime import datetime, timedelta

# Create widgets with defaults - these can be overridden by job parameters
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("schema", "nemweb_lab", "Schema Name")
dbutils.widgets.text("table", "nemweb_raw", "Raw Table Name")
dbutils.widgets.text("volume", "raw_files", "Volume Name")
dbutils.widgets.text("days_history", "180", "Days of History")
dbutils.widgets.dropdown("force_reload", "false", ["true", "false"], "Force Reload")
dbutils.widgets.dropdown("include_current", "false", ["true", "false"], "Include Recent (CURRENT)")

# Get configuration from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_TABLE = dbutils.widgets.get("table")
VOLUME_NAME = dbutils.widgets.get("volume")
DAYS_HISTORY = int(dbutils.widgets.get("days_history"))
FORCE_RELOAD_PARAM = dbutils.widgets.get("force_reload") == "true"
INCLUDE_CURRENT = dbutils.widgets.get("include_current")

# Date range for data loading
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=DAYS_HISTORY)).strftime("%Y-%m-%d")

# Derived paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
ARTIFACTS_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/artifacts"

print("Lab Configuration")
print("=" * 50)
print(f"Catalog:        {CATALOG}")
print(f"Schema:         {SCHEMA}")
print(f"Table:          {RAW_TABLE}")
print(f"Volume:         {VOLUME_PATH}")
print(f"Date range:     {START_DATE} to {END_DATE} ({DAYS_HISTORY} days)")
print(f"Force reload:   {FORCE_RELOAD_PARAM}")
print(f"Include recent: {INCLUDE_CURRENT} (fetch last ~7 days from CURRENT)")

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

print("Runtime compatible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Python Data Source API and PyArrow

# COMMAND ----------

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    print("Python Data Source API available")
except ImportError as e:
    raise ImportError(
        "Python Data Source API not available. Requires DBR 15.4+ / Spark 4.0+"
    ) from e

try:
    import pyarrow as pa
    print(f"PyArrow version: {pa.__version__}")
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
        print(f"NEMWEB connectivity verified (HTTP {response.status})")
except urllib.error.URLError as e:
    raise RuntimeError(f"Cannot connect to NEMWEB: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Install Lab Package
# MAGIC
# MAGIC The wheel is pre-built and deployed with the bundle to the artifacts folder.
# MAGIC If the source version doesn't match the wheel version, it will automatically
# MAGIC rebuild from source to ensure you're using the latest code.

# COMMAND ----------

import os
import glob
import re
import subprocess
from pathlib import Path

# Get workspace paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
workspace_root = f"/Workspace{repo_root}"
artifacts_path = f"{workspace_root}/artifacts"
src_path = f"{workspace_root}/src"

print(f"Repository: {workspace_root}")
print(f"Artifacts:  {artifacts_path}")

# Read version from pyproject.toml
pyproject_path = f"{src_path}/pyproject.toml"
source_version = None
if os.path.exists(pyproject_path):
    with open(pyproject_path, 'r') as f:
        content = f.read()
        match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if match:
            source_version = match.group(1)
            print(f"Source version: {source_version}")

# Find pre-built wheel from artifacts (deployed with bundle)
wheel_pattern = f"{artifacts_path}/nemweb_datasource-*.whl"
wheels = glob.glob(wheel_pattern)

should_rebuild = False
latest_wheel = None

if wheels:
    # Use latest version (sorted by name)
    latest_wheel = sorted(wheels)[-1]
    wheel_name = os.path.basename(latest_wheel)
    
    # Extract version from wheel filename (e.g., nemweb_datasource-2.10.5-py3-none-any.whl)
    wheel_version_match = re.search(r'nemweb_datasource-([\d.]+)-', wheel_name)
    wheel_version = wheel_version_match.group(1) if wheel_version_match else None
    
    print(f"Found pre-built wheel: {latest_wheel}")
    if wheel_version:
        print(f"Wheel version: {wheel_version}")
    
    # Rebuild if source version doesn't match wheel version
    if source_version and wheel_version and source_version != wheel_version:
        print(f"Version mismatch: source={source_version}, wheel={wheel_version}")
        print("Rebuilding from source to get latest version...")
        should_rebuild = True
    elif not os.path.exists(latest_wheel):
        print("Wheel file not found, rebuilding...")
        should_rebuild = True
else:
    print("No pre-built wheel found")
    should_rebuild = True

# Build from source if needed
if should_rebuild:
    print("Building wheel from source...")
    result = subprocess.run(
        f"cd {src_path} && rm -f dist/*.whl && uv build --wheel --out-dir dist",
        shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Build errors: {result.stderr}")
        raise RuntimeError("Wheel build failed")
    
    # Copy to artifacts for consistency
    dist_wheels = glob.glob(f"{src_path}/dist/nemweb_datasource-*.whl")
    if not dist_wheels:
        raise FileNotFoundError("No wheel found after build")
    
    latest_wheel = sorted(dist_wheels)[-1]
    wheel_name = os.path.basename(latest_wheel)
    
    # Copy to artifacts directory
    os.makedirs(artifacts_path, exist_ok=True)
    artifacts_wheel = f"{artifacts_path}/{wheel_name}"
    import shutil
    shutil.copy2(latest_wheel, artifacts_wheel)
    latest_wheel = artifacts_wheel
    
    print(f"Built and copied: {wheel_name}")

# COMMAND ----------

# Install wheel
%pip install $latest_wheel --force-reinstall -q

# COMMAND ----------

# Restart Python to load new package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Package and Register Data Source

# COMMAND ----------

# Re-import after restart
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Verify imports
from nemweb_utils import get_version, get_nem_regions
from nemweb_datasource_arrow import NemwebArrowDataSource

print(f"Package version: {get_version()}")
print(f"NEM regions: {get_nem_regions()}")

# Register Arrow data source
spark.dataSource.register(NemwebArrowDataSource)
print("Arrow data source registered (format: 'nemweb_arrow')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Schema and Volume

# COMMAND ----------

# Re-get config from widgets after restart (widgets persist across restart)
from datetime import datetime, timedelta

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_TABLE = dbutils.widgets.get("table")
VOLUME_NAME = dbutils.widgets.get("volume")
DAYS_HISTORY = int(dbutils.widgets.get("days_history"))
FORCE_RELOAD_PARAM = dbutils.widgets.get("force_reload") == "true"
INCLUDE_CURRENT = dbutils.widgets.get("include_current")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
ARTIFACTS_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/artifacts"

END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=DAYS_HISTORY)).strftime("%Y-%m-%d")

# Create catalog, schema, volumes
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.artifacts")

print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Volume: {VOLUME_PATH}")
print(f"Artifacts: {ARTIFACTS_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Deploy Wheel to UC Volume for Base Environment
# MAGIC
# MAGIC This copies the pre-built wheel to a UC Volume so it can be used in a serverless base environment.
# MAGIC
# MAGIC **To set up the base environment:**
# MAGIC 1. Go to **Settings > Compute > Base environments**
# MAGIC 2. Click **Manage > Create new environment**
# MAGIC 3. Select the `environment.yml` from the artifacts volume
# MAGIC 4. Click the star icon to set as default

# COMMAND ----------

import os
import shutil
import glob

# Get paths (reuse variables from earlier section)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
workspace_root = f"/Workspace{repo_root}"
artifacts_path = f"{workspace_root}/artifacts"

# Find the wheel (should already be built/copied in section 5)
wheel_pattern = f"{artifacts_path}/nemweb_datasource-*.whl"
wheels = glob.glob(wheel_pattern)

if wheels:
    # Use latest version (sorted by name)
    latest_wheel = sorted(wheels)[-1]
    wheel_name = os.path.basename(latest_wheel)

    # Deploy to UC Volume (keeping original name)
    dest_wheel_volume = f"{ARTIFACTS_VOLUME}/{wheel_name}"
    shutil.copy2(latest_wheel, dest_wheel_volume)
    print(f"Deployed wheel to Volume: {dest_wheel_volume}")

    # Create environment.yml for base environment pointing to Volume
    env_content = f"""environment_version: '4'
dependencies:
  - {dest_wheel_volume}
"""
    env_path = f"{ARTIFACTS_VOLUME}/environment.yml"
    with open(env_path, 'w') as f:
        f.write(env_content)
    print(f"Environment spec: {env_path}")

    print()
    print("Base environment ready!")
    print(f"  Wheel: {dest_wheel_volume}")
    print(f"  Spec:  {env_path}")
else:
    print(f"Warning: No wheel found matching {wheel_pattern}")
    print("The wheel should have been built in section 5 above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Check Existing Data

# COMMAND ----------

table_exists = spark.catalog.tableExists(TABLE_PATH)

if table_exists:
    row_count = spark.table(TABLE_PATH).count()
    print(f"Table {RAW_TABLE} exists with {row_count:,} rows")
    if FORCE_RELOAD_PARAM:
        print("Force reload enabled via parameter - will reload data")
    else:
        print("Set force_reload widget to 'true' to reload")
else:
    print(f"Table {RAW_TABLE} does not exist - will load data")

# Determine if we should reload
FORCE_RELOAD = FORCE_RELOAD_PARAM or not table_exists

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Download and Load Data Using Arrow Data Source
# MAGIC
# MAGIC The Arrow data source handles everything in one step:
# MAGIC - Downloads ZIP files to UC Volume (parallel, 8 threads)
# MAGIC - Parses NEMWEB multi-record CSV format
# MAGIC - Returns PyArrow RecordBatch for zero-copy transfer to Spark
# MAGIC - Works on both Serverless and Classic compute
# MAGIC
# MAGIC We load **two tables** from the same DISPATCHIS files:
# MAGIC - **DISPATCHREGIONSUM**: Demand, generation, and interconnector data
# MAGIC - **DISPATCHPRICE**: Regional Reference Price (RRP) data

# COMMAND ----------

if FORCE_RELOAD or not table_exists:
    import time

    start_time = time.time()

    print(f"Loading NEMWEB data: {START_DATE} to {END_DATE}")
    print("=" * 50)
    print("This will download files to Volume and load into Delta tables.")
    print("Downloads are parallel (8 threads) and skip existing files.")
    print()

    # -------------------------------------------------------------------------
    # Load DISPATCHREGIONSUM (demand/supply data)
    # -------------------------------------------------------------------------
    print("Loading DISPATCHREGIONSUM (demand data)...")
    df_regionsum = (spark.read
          .format("nemweb_arrow")
          .option("volume_path", VOLUME_PATH)
          .option("table", "DISPATCHREGIONSUM")
          .option("start_date", START_DATE)
          .option("end_date", END_DATE)
          .option("auto_download", "true")
          .option("max_workers", "8")
          .option("skip_existing", "true")
          .option("include_current", INCLUDE_CURRENT)
          .load())

    # Write DISPATCHREGIONSUM table
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_PATH}")
    df_regionsum.write.format("delta").mode("overwrite").saveAsTable(TABLE_PATH)
    regionsum_count = spark.table(TABLE_PATH).count()
    print(f"  Loaded {regionsum_count:,} rows to {TABLE_PATH}")

    # -------------------------------------------------------------------------
    # Load DISPATCHPRICE (price data) - same files, different record type
    # -------------------------------------------------------------------------
    print("\nLoading DISPATCHPRICE (price data)...")
    PRICE_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nemweb_prices"

    df_price = (spark.read
          .format("nemweb_arrow")
          .option("volume_path", VOLUME_PATH)
          .option("table", "DISPATCHPRICE")
          .option("start_date", START_DATE)
          .option("end_date", END_DATE)
          .option("auto_download", "true")  # Files already downloaded
          .option("max_workers", "8")
          .option("skip_existing", "true")
          .option("include_current", INCLUDE_CURRENT)
          .load())

    spark.sql(f"DROP TABLE IF EXISTS {PRICE_TABLE_PATH}")
    df_price.write.format("delta").mode("overwrite").saveAsTable(PRICE_TABLE_PATH)
    price_count = spark.table(PRICE_TABLE_PATH).count()
    print(f"  Loaded {price_count:,} rows to {PRICE_TABLE_PATH}")

    # -------------------------------------------------------------------------
    # Load NEM Registry (generator metadata from OpenNEM)
    # -------------------------------------------------------------------------
    print("\nLoading NEM Registry (generator metadata)...")
    REGISTRY_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nem_registry"

    from nem_registry import NemRegistryDataSource
    spark.dataSource.register(NemRegistryDataSource)

    df_registry = spark.read.format("nem_registry").load()

    spark.sql(f"DROP TABLE IF EXISTS {REGISTRY_TABLE_PATH}")
    df_registry.write.format("delta").mode("overwrite").saveAsTable(REGISTRY_TABLE_PATH)
    registry_count = spark.table(REGISTRY_TABLE_PATH).count()
    print(f"  Loaded {registry_count:,} generator units to {REGISTRY_TABLE_PATH}")

    elapsed = time.time() - start_time

    print()
    print("=" * 50)
    print(f"Data loading complete in {elapsed:.1f}s")
    print(f"  - {TABLE_PATH}: {regionsum_count:,} rows (demand)")
    print(f"  - {PRICE_TABLE_PATH}: {price_count:,} rows (prices)")
    print(f"  - {REGISTRY_TABLE_PATH}: {registry_count:,} units (generators)")
else:
    print("Using existing data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verify Loaded Data

# COMMAND ----------

from databricks.sdk.runtime import display
from pyspark.sql.functions import min, max, countDistinct, avg, round as spark_round

PRICE_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nemweb_prices"
REGISTRY_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nem_registry"

print("=" * 60)
print("LOADED TABLES")
print("=" * 60)

for table_name, table_path in [
    ("Demand/Supply (DISPATCHREGIONSUM)", TABLE_PATH),
    ("Prices (DISPATCHPRICE)", PRICE_TABLE_PATH),
    ("Generator Registry (OpenNEM)", REGISTRY_TABLE_PATH),
]:
    if spark.catalog.tableExists(table_path):
        df = spark.table(table_path)
        count = df.count()
        print(f"\n{table_name}")
        print(f"  Table: {table_path}")
        print(f"  Rows:  {count:,}")
    else:
        print(f"\n{table_name}: {table_path} - NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Price Data Preview

# COMMAND ----------

# Show price table statistics
if spark.catalog.tableExists(PRICE_TABLE_PATH):
    price_df = spark.table(PRICE_TABLE_PATH)

    stats = price_df.agg(
        min("SETTLEMENTDATE").alias("min_date"),
        max("SETTLEMENTDATE").alias("max_date"),
        countDistinct("REGIONID").alias("regions"),
        spark_round(avg("RRP"), 2).alias("avg_price"),
    ).collect()[0]

    print(f"Date range: {stats['min_date']} to {stats['max_date']}")
    print(f"Regions:    {stats['regions']}")
    print(f"Avg RRP:    ${stats['avg_price']}/MWh")
    print()

    print("Sample price data:")
    display(price_df.orderBy("SETTLEMENTDATE").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!

# COMMAND ----------

from nemweb_utils import get_version

PRICE_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nemweb_prices"
REGISTRY_TABLE_PATH = f"{CATALOG}.{SCHEMA}.nem_registry"

# Get row counts
raw_count = spark.table(TABLE_PATH).count() if spark.catalog.tableExists(TABLE_PATH) else 0
price_count = spark.table(PRICE_TABLE_PATH).count() if spark.catalog.tableExists(PRICE_TABLE_PATH) else 0
registry_count = spark.table(REGISTRY_TABLE_PATH).count() if spark.catalog.tableExists(REGISTRY_TABLE_PATH) else 0

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"""
Environment:
  Spark:    {spark.version}
  Package:  v{get_version()}

Data Tables:
  {TABLE_PATH}
    - Rows: {raw_count:,}
    - Contains: TOTALDEMAND, AVAILABLEGENERATION, NETINTERCHANGE

  {PRICE_TABLE_PATH}
    - Rows: {price_count:,}
    - Contains: RRP (Regional Reference Price), EEP

  {REGISTRY_TABLE_PATH}
    - Rows: {registry_count:,}
    - Contains: DUID, station_name, fuel_category, capacity_mw, lat/lng
    - Source: OpenNEM (github.com/opennem/opennem)

Base Environment (for workspace-wide use):
    Wheel: {ARTIFACTS_VOLUME}/nemweb_datasource-*.whl
    Spec:  {ARTIFACTS_VOLUME}/environment.yml

  To set as workspace default:
  1. Settings > Compute > Base environments > Manage
  2. Create new environment, select environment.yml
  3. Click star icon to set as default

Next Steps:
  1. Open 01_custom_source_exercise.py for Data Source API
  2. Open databricks-nemweb-analyst-lab/ for price analysis
  3. Exercise 03 uses the pre-loaded nemweb_raw data
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
# MAGIC - Downloads are parallel (8 threads by default)
# MAGIC - Existing files are skipped automatically
# MAGIC - Reduce date range for faster loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Default Base Environment (Optional)
# MAGIC
# MAGIC To make the NEMWEB data source available to **all notebooks** in your workspace without needing to run setup each time, configure a workspace base environment.
# MAGIC
# MAGIC ### Step-by-Step Instructions
# MAGIC
# MAGIC 1. **Open Workspace Settings**
# MAGIC    - Click your username in the top-right corner
# MAGIC    - Select **Settings**
# MAGIC
# MAGIC 2. **Navigate to Compute Settings**
# MAGIC    - In the left sidebar, under **Workspace admin**, click **Compute**
# MAGIC
# MAGIC 3. **Manage Base Environments**
# MAGIC    - Find **Base environments for serverless compute**
# MAGIC    - Click **Manage**
# MAGIC
# MAGIC 4. **Create New Environment**
# MAGIC    - Click **Create new environment**
# MAGIC    - Enter a name (e.g., `nemweb-lab`)
# MAGIC    - Click the folder icon to browse for the YAML file
# MAGIC    - Choose one of these locations:
# MAGIC      - **Volume**: `Volumes > [catalog] > nemweb_lab > artifacts > environment.yml`
# MAGIC      - **Workspace**: `Workspace > Users > [you] > [repo] > artifacts > environment.yml`
# MAGIC    - Click **Create**
# MAGIC
# MAGIC 5. **Set as Default**
# MAGIC    - Wait for the environment status to show **Ready to use**
# MAGIC    - Click the **star icon** (☆) next to your environment to set it as the workspace default
# MAGIC
# MAGIC ### Verify Configuration
# MAGIC
# MAGIC After setting the default, any new serverless notebook should automatically have access to the NEMWEB data source:
# MAGIC
# MAGIC ```python
# MAGIC from nemweb_datasource_arrow import NemwebArrowDataSource
# MAGIC spark.dataSource.register(NemwebArrowDataSource)
# MAGIC
# MAGIC df = spark.read.format("nemweb_arrow").option("table", "DISPATCHREGIONSUM").load()
# MAGIC ```
# MAGIC
# MAGIC ### File Locations
# MAGIC
# MAGIC **Option 1: UC Volume** (recommended)
# MAGIC | File | Path |
# MAGIC |------|------|
# MAGIC | Environment YAML | `/Volumes/{catalog}/nemweb_lab/artifacts/environment.yml` |
# MAGIC | Wheel | `/Volumes/{catalog}/nemweb_lab/artifacts/nemweb_datasource-{version}-py3-none-any.whl` |
# MAGIC
# MAGIC **Option 2: Workspace Files** (if Volume doesn't work)
# MAGIC | File | Path |
# MAGIC |------|------|
# MAGIC | Environment YAML | `/Workspace/.../databricks-nemweb-lab/artifacts/environment.yml` |
# MAGIC | Wheel | `/Workspace/.../databricks-nemweb-lab/artifacts/nemweb_datasource-{version}-py3-none-any.whl` |
# MAGIC
# MAGIC > **Note:** The environment.yml references the versioned wheel file. When updating the package,
# MAGIC > re-run this setup notebook to rebuild the wheel and update the environment.yml.
