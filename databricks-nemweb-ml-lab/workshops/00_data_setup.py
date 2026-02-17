# Databricks notebook source
# MAGIC %md
# MAGIC # ML Workshop - Data Setup
# MAGIC
# MAGIC This notebook downloads real NEMWEB energy market data and prepares it for the ML workshop.
# MAGIC Run this **before** the workshop to ensure data is available.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Creates the target catalog, schema, and UC Volumes
# MAGIC 2. Downloads NEMWEB ZIP files from the AEMO archive (30 days by default)
# MAGIC 3. Extracts and filters CSVs for Auto Loader streaming
# MAGIC 4. Verifies the extracted data is ready for the pipeline
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `nemweb_datasource` wheel installed (via job environment or cluster library)
# MAGIC - Unity Catalog enabled workspace

# COMMAND ----------

from datetime import datetime, timedelta

# Configuration widgets
dbutils.widgets.text("catalog", "daveok", "Catalog Name")
dbutils.widgets.text("schema", "ml_workshops", "Schema Name")
dbutils.widgets.text("days_history", "30", "Days of History")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DAYS_HISTORY = int(dbutils.widgets.get("days_history"))

print(f"Configuration:")
print(f"  Catalog:      {CATALOG}")
print(f"  Schema:       {SCHEMA}")
print(f"  Days history: {DAYS_HISTORY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog, schema, and volumes

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.extracted_csv")

ZIP_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
CSV_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/extracted_csv"

print(f"Schema:     {CATALOG}.{SCHEMA}")
print(f"Raw files:  {ZIP_PATH}")
print(f"CSVs:       {CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import download utilities from the wheel
# MAGIC
# MAGIC The `nemweb_ingest` module handles parallel downloads from the NEMWEB archive
# MAGIC with retry logic and rate limiting.

# COMMAND ----------

from nemweb_ingest import (
    download_all_tables,
    extract_all_tables,
    get_supported_tables,
)

supported = get_supported_tables()
print(f"Supported tables ({len(supported)}):")
for t in supported:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate date range
# MAGIC
# MAGIC NEMWEB ARCHIVE data becomes available ~8 days after the trading day.
# MAGIC We download from the archive (not CURRENT) for reliable bulk data.

# COMMAND ----------

end_date = datetime.now() - timedelta(days=8)
start_date = end_date - timedelta(days=DAYS_HISTORY)

START_DATE = start_date.strftime("%Y-%m-%d")
END_DATE = end_date.strftime("%Y-%m-%d")

print(f"Date range: {START_DATE} to {END_DATE}")
print(f"  ({DAYS_HISTORY} days of data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download NEMWEB ZIP files
# MAGIC
# MAGIC Downloads ZIP files from NEMWEB ARCHIVE to the `raw_files` volume.
# MAGIC Tables that share the same source file (e.g., DISPATCHREGIONSUM and DISPATCHPRICE
# MAGIC both come from DispatchIS_Reports) share downloaded files.

# COMMAND ----------

download_results = download_all_tables(
    volume_path=ZIP_PATH,
    start_date=START_DATE,
    end_date=END_DATE,
    max_workers=8,
    skip_existing=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract CSVs for Auto Loader
# MAGIC
# MAGIC Extracts and filters CSV records from the ZIP files.
# MAGIC Each table gets its own subdirectory in the `extracted_csv` volume.

# COMMAND ----------

extract_results = extract_all_tables(
    zip_path=ZIP_PATH,
    csv_path=CSV_PATH,
    skip_existing=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify extracted data

# COMMAND ----------

import os

print("Extracted CSV summary:")
print("-" * 60)

total_files = 0
total_size_mb = 0

for table in get_supported_tables():
    table_dir = os.path.join(CSV_PATH, table.lower())
    if os.path.exists(table_dir):
        files = [f for f in os.listdir(table_dir) if f.endswith(".csv")]
        if files:
            size_mb = sum(
                os.path.getsize(os.path.join(table_dir, f)) for f in files
            ) / (1024 * 1024)
            total_files += len(files)
            total_size_mb += size_mb
            print(f"  {table:30s} {len(files):5d} files  {size_mb:8.1f} MB")
        else:
            print(f"  {table:30s}     0 files")
    else:
        print(f"  {table:30s}   (not found)")

print("-" * 60)
print(f"  {'TOTAL':30s} {total_files:5d} files  {total_size_mb:8.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC The extracted CSV files are ready for the streaming pipeline at:
# MAGIC ```
# MAGIC /Volumes/{CATALOG}/{SCHEMA}/extracted_csv/<table_name>/
# MAGIC ```
# MAGIC
# MAGIC **Next step:** Run the ML pipeline to create bronze/silver/gold Delta tables.
