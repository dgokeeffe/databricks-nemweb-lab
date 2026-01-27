# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 1 - Understanding the NEMWEB Arrow Data Source
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 1.

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
# MAGIC ## Solution 1.1: Explore DISPATCHPRICE Schema

# COMMAND ----------

# SOLUTION 1.1: Print the DISPATCHPRICE schema fields
print("DISPATCHPRICE Schema:")
print("-" * 50)
for field_name, field_type in SCHEMAS["DISPATCHPRICE"]["fields"]:
    print(f"  {field_name}: {type(field_type).__name__}")

print(f"\nRecord type filter: {SCHEMAS['DISPATCHPRICE']['record_type']}")

# COMMAND ----------

# Understanding TABLE_TO_FOLDER Mapping
print("Table to Folder Mapping:")
print("-" * 50)
for table, (folder, prefix) in TABLE_TO_FOLDER.items():
    print(f"  {table}:")
    print(f"    Folder: {folder}")
    print(f"    File prefix: PUBLIC_{prefix}_YYYYMMDD.zip")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 1.2: Fetch DISPATCHPRICE Data

# COMMAND ----------

# SOLUTION 1.2: Read DISPATCHPRICE for VIC1 region
df_price = (spark.read
    .format("nemweb_arrow")
    .option("table", "DISPATCHPRICE")
    .option("regions", "VIC1")
    .option("start_date", "2024-07-01")
    .option("end_date", "2024-07-01")
    .load())

print(f"Price data rows: {df_price.count()}")
display(df_price.limit(5))

# COMMAND ----------

# Analyze price data
print("Victoria Price Statistics:")
price_stats = df_price.select(
    avg("RRP").alias("avg_price_mwh"),
    max("RRP").alias("max_price_mwh"),
    min("RRP").alias("min_price_mwh"),
)
display(price_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Mode with Auto-Download

# COMMAND ----------

# Configuration
CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "workspace"
SCHEMA = "nemweb_lab"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# Auto-download mode: Downloads to volume first, then reads
df_volume = (spark.read
    .format("nemweb_arrow")
    .option("volume_path", VOLUME_PATH)
    .option("table", "DISPATCHREGIONSUM")
    .option("start_date", "2024-07-01")
    .option("end_date", "2024-07-03")
    .option("auto_download", "true")
    .option("max_workers", "8")
    .load())

print(f"Volume mode: Retrieved {df_volume.count()} rows")

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
# MAGIC ## Solution 1.3: Define DISPATCH_UNIT_SCADA Schema

# COMMAND ----------

from pyspark.sql.types import TimestampType, StringType, DoubleType

# SOLUTION 1.3: Complete DISPATCH_UNIT_SCADA schema
DISPATCH_UNIT_SCADA_CONFIG = {
    "record_type": "DISPATCH,UNIT_SCADA",
    "fields": [
        ("SETTLEMENTDATE", TimestampType()),
        ("DUID", StringType()),
        ("SCADAVALUE", DoubleType()),
    ],
}

# Verify schema
print("DISPATCH_UNIT_SCADA Schema:")
print("-" * 50)
for field_name, field_type in DISPATCH_UNIT_SCADA_CONFIG["fields"]:
    print(f"  {field_name}: {type(field_type).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 1.4: Add Table Mapping

# COMMAND ----------

# SOLUTION 1.4: Define the folder mapping for DISPATCH_UNIT_SCADA
DISPATCH_UNIT_SCADA_FOLDER = ("Dispatch_SCADA", "DISPATCHSCADA")

print(f"Folder mapping: {DISPATCH_UNIT_SCADA_FOLDER}")
print(f"  Folder: {DISPATCH_UNIT_SCADA_FOLDER[0]}")
print(f"  File prefix: PUBLIC_{DISPATCH_UNIT_SCADA_FOLDER[1]}_YYYYMMDD.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Add DISPATCH_UNIT_SCADA to Production Code
# MAGIC
# MAGIC To actually add this table to the data source, you would add these
# MAGIC to `nemweb_datasource_arrow.py`:

# COMMAND ----------

# This shows what you would add to the production code:
print("""
# Add to SCHEMAS dictionary in nemweb_datasource_arrow.py:

SCHEMAS = {
    ...existing schemas...

    "DISPATCH_UNIT_SCADA": {
        "record_type": "DISPATCH,UNIT_SCADA",
        "fields": [
            ("SETTLEMENTDATE", TimestampType()),
            ("DUID", StringType()),
            ("SCADAVALUE", DoubleType()),
        ],
    },
}

# Add to TABLE_TO_FOLDER dictionary:

TABLE_TO_FOLDER = {
    ...existing mappings...

    "DISPATCH_UNIT_SCADA": ("Dispatch_SCADA", "DISPATCHSCADA"),
}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

def validate_exercise():
    """Validate Exercise 1 completion."""
    print("=" * 60)
    print("EXERCISE 1 VALIDATION")
    print("=" * 60)

    checks = {
        "1.1 - Explored DISPATCHPRICE schema": True,  # We did it above
        "1.2 - Fetched price data": False,
        "1.3 - Defined DISPATCH_UNIT_SCADA schema": False,
        "1.4 - Defined folder mapping": False,
    }

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
        print("All checks passed!")
        print("=" * 60)

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
