# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop data setup
# MAGIC
# MAGIC This notebook downloads NEMWEB data and extracts it to CSV format for the streaming pipeline.
# MAGIC Uses the `nemweb_ingest` module from the installed wheel.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Creates the target catalog, schema, and volumes
# MAGIC 2. Downloads NEMWEB ZIP files from the archive
# MAGIC 3. Extracts and filters CSVs for Auto Loader streaming

# COMMAND ----------

from datetime import datetime, timedelta

# Configuration
dbutils.widgets.text("catalog", "daveok", "Catalog Name")
dbutils.widgets.text("schema", "ml_workshops", "Schema Name")
dbutils.widgets.text("days_history", "7", "Days of History")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DAYS_HISTORY = int(dbutils.widgets.get("days_history"))

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Days history: {DAYS_HISTORY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog, schema, and volumes

# COMMAND ----------

# Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Create volumes for raw files and extracted CSVs
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.extracted_csv")

ZIP_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
CSV_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/extracted_csv"

print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Raw files volume: {ZIP_PATH}")
print(f"Extracted CSV volume: {CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import download utilities from wheel

# COMMAND ----------

from nemweb_ingest import (
    download_all_tables,
    extract_all_tables,
    get_supported_tables,
)

print(f"Supported tables: {get_supported_tables()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate date range

# COMMAND ----------

# Calculate date range (ending 8+ days ago to ensure ARCHIVE availability)
end_date = datetime.now() - timedelta(days=8)
start_date = end_date - timedelta(days=DAYS_HISTORY)

START_DATE = start_date.strftime("%Y-%m-%d")
END_DATE = end_date.strftime("%Y-%m-%d")

print(f"Date range: {START_DATE} to {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download all tables
# MAGIC
# MAGIC Downloads ZIP files from NEMWEB ARCHIVE to the raw_files volume.
# MAGIC Tables that share the same source file (e.g., DISPATCHREGIONSUM and DISPATCHPRICE
# MAGIC both come from DispatchIS_Reports) will share the downloaded files.

# COMMAND ----------

download_results = download_all_tables(
    volume_path=ZIP_PATH,
    start_date=START_DATE,
    end_date=END_DATE,
    max_workers=8,
    skip_existing=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract CSVs for Auto Loader
# MAGIC
# MAGIC Extracts and filters the CSV records from the ZIP files.
# MAGIC Each table gets its own subdirectory in the extracted_csv volume.

# COMMAND ----------

extract_results = extract_all_tables(
    zip_path=ZIP_PATH,
    csv_path=CSV_PATH,
    skip_existing=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify extracted data

# COMMAND ----------

import os

print("Extracted CSV files:")
for table in get_supported_tables():
    table_dir = os.path.join(CSV_PATH, table.lower())
    if os.path.exists(table_dir):
        files = [f for f in os.listdir(table_dir) if f.endswith('.csv')]
        if files:
            total_size = sum(os.path.getsize(os.path.join(table_dir, f)) for f in files)
            print(f"  {table}: {len(files)} files, {total_size / 1024 / 1024:.1f} MB")
    else:
        print(f"  {table}: No files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC The extracted CSV files are ready for the streaming pipeline in:
# MAGIC - `/Volumes/{CATALOG}/{SCHEMA}/extracted_csv/<table_name>/`
# MAGIC
# MAGIC The MMS pipeline will use Auto Loader (cloudFiles) to stream these files into Delta tables.
