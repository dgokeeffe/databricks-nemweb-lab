# Databricks notebook source
# MAGIC %md
# MAGIC # Test New NEMWEB Tables
# MAGIC
# MAGIC This notebook tests all the newly added tables in the NEMWEB datasource:
# MAGIC 1. DISPATCH_UNIT_SCADA - Real-time unit generation
# MAGIC 2. P5MIN_REGIONSOLUTION - 5-minute price forecasts
# MAGIC 3. P5MIN_INTERCONNECTORSOLN - Interconnector flow forecasts
# MAGIC 4. BIDPEROFFER_D - Generator bid availability per period
# MAGIC 5. BIDDAYOFFER_D - Generator daily price bands

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os

# Add src to path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_datasource_arrow import NemwebArrowDataSource, SCHEMAS, TABLE_TO_FOLDER

# Register datasource
spark.dataSource.register(NemwebArrowDataSource)
print("Datasource registered: nemweb_arrow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List All Supported Tables

# COMMAND ----------

print("Supported Tables:")
print("=" * 80)
for table in SCHEMAS:
    folder, prefix = TABLE_TO_FOLDER.get(table, ("N/A", "N/A"))
    fields = SCHEMAS[table]["fields"]
    record_type = SCHEMAS[table]["record_type"]
    print(f"\n{table}")
    print(f"  Record type: {record_type}")
    print(f"  Source: {folder}/{prefix}")
    print(f"  Fields: {len(fields)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: DISPATCH_UNIT_SCADA (Real-time Unit Generation)

# COMMAND ----------

print("Testing DISPATCH_UNIT_SCADA...")
try:
    df_scada = (spark.read.format("nemweb_arrow")
                .option("table", "DISPATCH_UNIT_SCADA")
                .option("include_current", "true")
                .load())

    print(f"Schema:")
    df_scada.printSchema()

    print(f"\nRow count: {df_scada.count()}")
    print(f"\nSample data (top generators by output):")
    display(df_scada.orderBy(df_scada.SCADAVALUE.desc()).limit(10))

    print("\n✓ DISPATCH_UNIT_SCADA: PASSED")
except Exception as e:
    print(f"\n✗ DISPATCH_UNIT_SCADA: FAILED - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: P5MIN_REGIONSOLUTION (5-min Price Forecasts)

# COMMAND ----------

print("Testing P5MIN_REGIONSOLUTION...")
try:
    df_p5 = (spark.read.format("nemweb_arrow")
             .option("table", "P5MIN_REGIONSOLUTION")
             .option("include_current", "true")
             .load())

    print(f"Schema:")
    df_p5.printSchema()

    print(f"\nRow count: {df_p5.count()}")
    print(f"\nSample forecast data:")
    display(df_p5.select("RUN_DATETIME", "INTERVAL_DATETIME", "REGIONID", "RRP", "TOTALDEMAND").limit(20))

    print("\n✓ P5MIN_REGIONSOLUTION: PASSED")
except Exception as e:
    print(f"\n✗ P5MIN_REGIONSOLUTION: FAILED - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: P5MIN_INTERCONNECTORSOLN (Interconnector Forecasts)

# COMMAND ----------

print("Testing P5MIN_INTERCONNECTORSOLN...")
try:
    df_ic = (spark.read.format("nemweb_arrow")
             .option("table", "P5MIN_INTERCONNECTORSOLN")
             .option("include_current", "true")
             .load())

    print(f"Schema:")
    df_ic.printSchema()

    print(f"\nRow count: {df_ic.count()}")
    print(f"\nInterconnector flows:")
    display(df_ic.select("RUN_DATETIME", "INTERCONNECTORID", "MWFLOW", "EXPORTLIMIT", "IMPORTLIMIT").limit(20))

    print("\n✓ P5MIN_INTERCONNECTORSOLN: PASSED")
except Exception as e:
    print(f"\n✗ P5MIN_INTERCONNECTORSOLN: FAILED - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: BIDPEROFFER_D (Generator Bid Availability)

# COMMAND ----------

print("Testing BIDPEROFFER_D...")
try:
    df_bids = (spark.read.format("nemweb_arrow")
               .option("table", "BIDPEROFFER_D")
               .option("include_current", "true")
               .load())

    print(f"Schema:")
    df_bids.printSchema()

    print(f"\nRow count: {df_bids.count()}")
    print(f"\nSample bid data:")
    display(df_bids.select(
        "DUID", "BIDTYPE", "PERIODID", "MAXAVAIL",
        "BANDAVAIL1", "BANDAVAIL2", "BANDAVAIL3",
        "ROCUP", "ROCDOWN"
    ).limit(20))

    # Show unique generators
    print(f"\nUnique generators (DUIDs): {df_bids.select('DUID').distinct().count()}")
    print(f"Unique bid types: {df_bids.select('BIDTYPE').distinct().collect()}")

    print("\n✓ BIDPEROFFER_D: PASSED")
except Exception as e:
    print(f"\n✗ BIDPEROFFER_D: FAILED - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: BIDDAYOFFER_D (Generator Daily Price Bands)

# COMMAND ----------

print("Testing BIDDAYOFFER_D...")
try:
    df_dayoffer = (spark.read.format("nemweb_arrow")
                   .option("table", "BIDDAYOFFER_D")
                   .option("include_current", "true")
                   .load())

    print(f"Schema:")
    df_dayoffer.printSchema()

    print(f"\nRow count: {df_dayoffer.count()}")
    print(f"\nSample price band data:")
    display(df_dayoffer.select(
        "DUID", "BIDTYPE", "PARTICIPANTID",
        "PRICEBAND1", "PRICEBAND2", "PRICEBAND3", "PRICEBAND4", "PRICEBAND5",
        "PRICEBAND6", "PRICEBAND7", "PRICEBAND8", "PRICEBAND9", "PRICEBAND10"
    ).limit(20))

    print("\n✓ BIDDAYOFFER_D: PASSED")
except Exception as e:
    print(f"\n✗ BIDDAYOFFER_D: FAILED - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Verify Existing Tables Still Work

# COMMAND ----------

print("Testing existing tables...")

# DISPATCHREGIONSUM
try:
    df = spark.read.format("nemweb_arrow").option("table", "DISPATCHREGIONSUM").option("include_current", "true").load()
    print(f"✓ DISPATCHREGIONSUM: {df.count()} rows")
except Exception as e:
    print(f"✗ DISPATCHREGIONSUM: {e}")

# DISPATCHPRICE
try:
    df = spark.read.format("nemweb_arrow").option("table", "DISPATCHPRICE").option("include_current", "true").load()
    print(f"✓ DISPATCHPRICE: {df.count()} rows")
except Exception as e:
    print(f"✗ DISPATCHPRICE: {e}")

# TRADINGPRICE
try:
    df = spark.read.format("nemweb_arrow").option("table", "TRADINGPRICE").option("include_current", "true").load()
    print(f"✓ TRADINGPRICE: {df.count()} rows")
except Exception as e:
    print(f"✗ TRADINGPRICE: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("TEST SUMMARY")
print("=" * 80)
print("""
New Tables Added:
  - DISPATCH_UNIT_SCADA: Real-time generation per power station
  - P5MIN_REGIONSOLUTION: 5-minute regional price/demand forecasts
  - P5MIN_INTERCONNECTORSOLN: 5-minute interconnector flow forecasts
  - BIDPEROFFER_D: Generator availability per trading period
  - BIDDAYOFFER_D: Generator daily price bands

Total Supported Tables: 8
""")
