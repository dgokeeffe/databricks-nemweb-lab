# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3: Data Layout Optimization Comparison
# MAGIC
# MAGIC **Time:** 10 minutes
# MAGIC
# MAGIC In this exercise, you'll compare two Delta Lake optimization approaches for time-series data:
# MAGIC 1. **Liquid Clustering** - Modern approach (DBR 13.3+)
# MAGIC 2. **Generated Columns + Partitioning** - Traditional approach
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand when to use liquid clustering vs. partitioning
# MAGIC 2. Measure query performance differences (files scanned, bytes read)
# MAGIC 3. Apply the right optimization for NEMWEB time-series data
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **00_setup_and_validation.py** first to pre-load NEMWEB data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC We'll read from the pre-loaded NEMWEB data created in setup.

# COMMAND ----------

from databricks.sdk.runtime import spark, display

# Must match the values from 00_setup_and_validation.py
SOURCE_CATALOG = "main"
SOURCE_SCHEMA = "nemweb_lab"
SOURCE_TABLE = "nemweb_raw"

# Target schema for optimization comparison tables
TARGET_SCHEMA = "nemweb_optimization_lab"

# Verify source table exists
source_table_path = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
if not spark.catalog.tableExists(source_table_path):
    raise RuntimeError(
        f"Source table {source_table_path} not found!\n"
        "Please run 00_setup_and_validation.py first to pre-load data."
    )

# Create target schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SOURCE_CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE {SOURCE_CATALOG}.{TARGET_SCHEMA}")

row_count = spark.table(source_table_path).count()
print(f"✓ Source table: {source_table_path}")
print(f"✓ Row count: {row_count:,}")
print(f"✓ Target schema: {SOURCE_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Pre-loaded NEMWEB Data

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when

# Read from pre-loaded table
nemweb_raw = spark.table(source_table_path)

# Prepare data with standardized column names
nemweb_data = (
    nemweb_raw
    .select(
        col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
        col("REGIONID").alias("region_id"),
        col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
        col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
        col("NETINTERCHANGE").cast("double").alias("net_interchange_mw")
    )
    .withColumn("_loaded_at", current_timestamp())
    # Generate a synthetic RRP for price spike queries (real RRP requires DISPATCHPRICE table)
    .withColumn("rrp",
        when(col("total_demand_mw") > 8000, 300 + (col("total_demand_mw") - 8000) * 0.5)
        .otherwise(30 + col("total_demand_mw") * 0.01)
    )
)

# Cache for reuse
nemweb_data.cache()
print(f"Prepared {nemweb_data.count():,} rows for optimization comparison")
nemweb_data.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 1: Liquid Clustering
# MAGIC
# MAGIC Cluster by the columns most commonly used in query filters.

# COMMAND ----------

# Drop if exists for clean comparison
spark.sql("DROP TABLE IF EXISTS nemweb_liquid_clustered")

# Create table with liquid clustering on settlement_date and region_id
nemweb_data.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "false") \
    .clusterBy("settlement_date", "region_id") \
    .saveAsTable("nemweb_liquid_clustered")

print("✓ Created: nemweb_liquid_clustered")
print("  Clustering keys: settlement_date, region_id")

# COMMAND ----------

# Run OPTIMIZE to apply clustering
spark.sql("OPTIMIZE nemweb_liquid_clustered")
print("✓ OPTIMIZE complete - data is now clustered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 2: Generated Columns + Partitioning
# MAGIC
# MAGIC Traditional approach: generate date columns and partition by them.

# COMMAND ----------

# Drop if exists
spark.sql("DROP TABLE IF EXISTS nemweb_partitioned")

# Create table with generated columns for partitioning
spark.sql("""
CREATE TABLE nemweb_partitioned (
    settlement_date TIMESTAMP,
    region_id STRING,
    total_demand_mw DOUBLE,
    available_generation_mw DOUBLE,
    net_interchange_mw DOUBLE,
    rrp DOUBLE,
    _loaded_at TIMESTAMP,
    -- Generated columns for partition pruning
    settlement_year INT GENERATED ALWAYS AS (YEAR(settlement_date)),
    settlement_month INT GENERATED ALWAYS AS (MONTH(settlement_date)),
    settlement_day INT GENERATED ALWAYS AS (DAY(settlement_date))
)
PARTITIONED BY (settlement_year, settlement_month)
""")

print("✓ Created: nemweb_partitioned")
print("  Partitioned by: settlement_year, settlement_month")
print("  Generated columns: settlement_year, settlement_month, settlement_day")

# COMMAND ----------

# Insert data (generated columns are auto-computed)
nemweb_data.select(
    "settlement_date", "region_id", "total_demand_mw",
    "available_generation_mw", "net_interchange_mw", "rrp", "_loaded_at"
).write.mode("append").insertInto("nemweb_partitioned")

print("✓ Data inserted - generated columns computed automatically")

# Optimize each partition
spark.sql("OPTIMIZE nemweb_partitioned")
print("✓ OPTIMIZE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison
# MAGIC
# MAGIC Now let's run identical queries against both tables and compare metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Function: Capture Query Metrics

# COMMAND ----------

import time

def run_query_with_metrics(query: str, description: str) -> dict:
    """Run a query and capture performance metrics."""
    # Clear cache to ensure fair comparison
    spark.catalog.clearCache()

    # Execute and time
    start = time.time()
    df = spark.sql(query)
    result = df.collect()  # Force execution
    elapsed = time.time() - start

    return {
        "description": description,
        "elapsed_seconds": round(elapsed, 3),
        "row_count": len(result)
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Single Day Filter (Narrow Time Range)
# MAGIC
# MAGIC Filter for a specific day - tests partition/cluster pruning effectiveness.

# COMMAND ----------

# Use a date in the middle of the data range
query_liquid = """
SELECT region_id,
       AVG(total_demand_mw) as avg_demand,
       MAX(rrp) as max_price,
       COUNT(*) as intervals
FROM nemweb_liquid_clustered
WHERE settlement_date >= '2024-03-15'
  AND settlement_date < '2024-03-16'
GROUP BY region_id
ORDER BY region_id
"""

query_partitioned = """
SELECT region_id,
       AVG(total_demand_mw) as avg_demand,
       MAX(rrp) as max_price,
       COUNT(*) as intervals
FROM nemweb_partitioned
WHERE settlement_date >= '2024-03-15'
  AND settlement_date < '2024-03-16'
GROUP BY region_id
ORDER BY region_id
"""

print("=" * 60)
print("QUERY 1: Single Day Aggregation")
print("=" * 60)

result_liquid = run_query_with_metrics(query_liquid, "Liquid Clustered")
result_partitioned = run_query_with_metrics(query_partitioned, "Partitioned + Generated")

print(f"\nLiquid Clustered:        {result_liquid['elapsed_seconds']:.3f}s ({result_liquid['row_count']} rows)")
print(f"Partitioned + Generated: {result_partitioned['elapsed_seconds']:.3f}s ({result_partitioned['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Single Region, Full History
# MAGIC
# MAGIC Filter for one region across all time - tests region-based filtering.

# COMMAND ----------

query_liquid_2 = """
SELECT DATE(settlement_date) as date,
       AVG(total_demand_mw) as avg_demand,
       AVG(rrp) as avg_price
FROM nemweb_liquid_clustered
WHERE region_id = 'NSW1'
GROUP BY DATE(settlement_date)
ORDER BY date
"""

query_partitioned_2 = """
SELECT DATE(settlement_date) as date,
       AVG(total_demand_mw) as avg_demand,
       AVG(rrp) as avg_price
FROM nemweb_partitioned
WHERE region_id = 'NSW1'
GROUP BY DATE(settlement_date)
ORDER BY date
"""

print("=" * 60)
print("QUERY 2: Single Region Full History")
print("=" * 60)

result_liquid_2 = run_query_with_metrics(query_liquid_2, "Liquid Clustered")
result_partitioned_2 = run_query_with_metrics(query_partitioned_2, "Partitioned + Generated")

print(f"\nLiquid Clustered:        {result_liquid_2['elapsed_seconds']:.3f}s ({result_liquid_2['row_count']} rows)")
print(f"Partitioned + Generated: {result_partitioned_2['elapsed_seconds']:.3f}s ({result_partitioned_2['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Month Range + Region Filter (Combined)
# MAGIC
# MAGIC Filter by both time range and region - tests combined pruning.

# COMMAND ----------

query_liquid_3 = """
SELECT region_id,
       HOUR(settlement_date) as hour_of_day,
       AVG(total_demand_mw) as avg_demand,
       PERCENTILE_APPROX(rrp, 0.95) as p95_price
FROM nemweb_liquid_clustered
WHERE settlement_date >= '2024-02-01'
  AND settlement_date < '2024-04-01'
  AND region_id IN ('NSW1', 'VIC1')
GROUP BY region_id, HOUR(settlement_date)
ORDER BY region_id, hour_of_day
"""

query_partitioned_3 = """
SELECT region_id,
       HOUR(settlement_date) as hour_of_day,
       AVG(total_demand_mw) as avg_demand,
       PERCENTILE_APPROX(rrp, 0.95) as p95_price
FROM nemweb_partitioned
WHERE settlement_date >= '2024-02-01'
  AND settlement_date < '2024-04-01'
  AND region_id IN ('NSW1', 'VIC1')
GROUP BY region_id, HOUR(settlement_date)
ORDER BY region_id, hour_of_day
"""

print("=" * 60)
print("QUERY 3: Month Range + Region Filter")
print("=" * 60)

result_liquid_3 = run_query_with_metrics(query_liquid_3, "Liquid Clustered")
result_partitioned_3 = run_query_with_metrics(query_partitioned_3, "Partitioned + Generated")

print(f"\nLiquid Clustered:        {result_liquid_3['elapsed_seconds']:.3f}s ({result_liquid_3['row_count']} rows)")
print(f"Partitioned + Generated: {result_partitioned_3['elapsed_seconds']:.3f}s ({result_partitioned_3['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Price Spike Detection (No Time/Region Filter)
# MAGIC
# MAGIC Scan for price spikes - tests full table scan performance.

# COMMAND ----------

query_liquid_4 = """
SELECT settlement_date, region_id, rrp, total_demand_mw
FROM nemweb_liquid_clustered
WHERE rrp > 300
ORDER BY rrp DESC
LIMIT 100
"""

query_partitioned_4 = """
SELECT settlement_date, region_id, rrp, total_demand_mw
FROM nemweb_partitioned
WHERE rrp > 300
ORDER BY rrp DESC
LIMIT 100
"""

print("=" * 60)
print("QUERY 4: Price Spike Detection (Full Scan)")
print("=" * 60)

result_liquid_4 = run_query_with_metrics(query_liquid_4, "Liquid Clustered")
result_partitioned_4 = run_query_with_metrics(query_partitioned_4, "Partitioned + Generated")

print(f"\nLiquid Clustered:        {result_liquid_4['elapsed_seconds']:.3f}s ({result_liquid_4['row_count']} rows)")
print(f"Partitioned + Generated: {result_partitioned_4['elapsed_seconds']:.3f}s ({result_partitioned_4['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

from pyspark.sql import Row

summary_data = [
    Row(query="Q1: Single Day", liquid=result_liquid['elapsed_seconds'], partitioned=result_partitioned['elapsed_seconds']),
    Row(query="Q2: Single Region History", liquid=result_liquid_2['elapsed_seconds'], partitioned=result_partitioned_2['elapsed_seconds']),
    Row(query="Q3: Month + Region", liquid=result_liquid_3['elapsed_seconds'], partitioned=result_partitioned_3['elapsed_seconds']),
    Row(query="Q4: Price Spikes (Full)", liquid=result_liquid_4['elapsed_seconds'], partitioned=result_partitioned_4['elapsed_seconds']),
]

summary_df = spark.createDataFrame(summary_data)
summary_df = summary_df.withColumn("winner",
    when(col("liquid") < col("partitioned"), "Liquid Clustering")
    .when(col("partitioned") < col("liquid"), "Partitioned")
    .otherwise("Tie")
)

print("\n" + "=" * 60)
print("PERFORMANCE SUMMARY (seconds)")
print("=" * 60)
summary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Statistics Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Liquid clustered table details
# MAGIC DESCRIBE DETAIL nemweb_liquid_clustered

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Partitioned table details
# MAGIC DESCRIBE DETAIL nemweb_partitioned

# COMMAND ----------

# Get file counts and sizes
liquid_detail = spark.sql("DESCRIBE DETAIL nemweb_liquid_clustered").collect()[0]
partitioned_detail = spark.sql("DESCRIBE DETAIL nemweb_partitioned").collect()[0]

print("\nTable Statistics:")
print("-" * 50)
print(f"{'Metric':<25} {'Liquid':<15} {'Partitioned':<15}")
print("-" * 50)
print(f"{'Num Files':<25} {liquid_detail['numFiles']:<15} {partitioned_detail['numFiles']:<15}")
print(f"{'Size (MB)':<25} {liquid_detail['sizeInBytes']/1024/1024:<15.2f} {partitioned_detail['sizeInBytes']/1024/1024:<15.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Scenario | Recommended Approach | Why |
# MAGIC |----------|---------------------|-----|
# MAGIC | New tables | **Liquid Clustering** | Simpler, flexible, modern |
# MAGIC | Time-series with varied queries | **Liquid Clustering** | Handles both time and dimension filters |
# MAGIC | Very high cardinality partition keys | **Liquid Clustering** | Avoids small file problem |
# MAGIC | Strict partition pruning needed | **Generated + Partitioning** | Explicit partition boundaries |
# MAGIC | Existing partitioned tables | Keep partitioning OR migrate | Migration requires rewrite |
# MAGIC
# MAGIC ### For NEMWEB Data
# MAGIC
# MAGIC 1. **Liquid clustering** excels when queries filter on multiple dimensions (time + region)
# MAGIC 2. **Partitioning** is effective for pure time-range queries with known boundaries
# MAGIC 3. Both approaches benefit from running `OPTIMIZE` regularly
# MAGIC 4. On serverless, **data layout optimization is your primary performance lever**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Uncache source data
nemweb_data.unpersist()

# Uncomment to clean up test tables
# spark.sql("DROP TABLE IF EXISTS nemweb_liquid_clustered")
# spark.sql("DROP TABLE IF EXISTS nemweb_partitioned")
# spark.sql(f"DROP SCHEMA IF EXISTS {SOURCE_CATALOG}.{TARGET_SCHEMA}")
# print("Cleanup complete")
