# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3b: Data Layout Optimization Comparison
# MAGIC
# MAGIC **Time:** 10 minutes
# MAGIC
# MAGIC In this exercise, you'll compare two Delta Lake optimization approaches for time-series data:
# MAGIC 1. **Liquid Clustering** - Modern approach (DBR 13.3+)
# MAGIC 2. **Generated Columns + Partitioning** - Traditional approach
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand when to use liquid clustering vs. partitioning
# MAGIC 2. Implement both approaches for the same dataset
# MAGIC 3. Measure query performance differences
# MAGIC 4. Apply the right optimization for NEMWEB time-series data
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **00_setup_and_validation.py** first to pre-load NEMWEB data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql import Row
import time

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
print(f"Source table: {source_table_path}")
print(f"Row count: {row_count:,}")
print(f"Target schema: {SOURCE_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Source Data

# COMMAND ----------

# Read from pre-loaded table and prepare with standardized columns
nemweb_raw = spark.table(source_table_path)

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
    # Generate synthetic RRP for price queries (real RRP requires DISPATCHPRICE table)
    .withColumn("rrp",
        when(col("total_demand_mw") > 8000, 300 + (col("total_demand_mw") - 8000) * 0.5)
        .otherwise(30 + col("total_demand_mw") * 0.01)
    )
)

# Cache for reuse
nemweb_data.cache()
print(f"Prepared {nemweb_data.count():,} rows for optimization comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Liquid Clustered Table (3 minutes)
# MAGIC
# MAGIC Liquid clustering organizes data by clustering keys for efficient data skipping.
# MAGIC
# MAGIC **Reference:** [Liquid Clustering Docs](https://docs.databricks.com/en/delta/clustering.html)
# MAGIC
# MAGIC ### TODO 3b.1: Choose clustering keys
# MAGIC
# MAGIC For NEMWEB time-series data, which columns should we cluster by?
# MAGIC Consider: What columns appear most often in WHERE clauses?

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS nemweb_liquid_clustered")

# TODO 3b.1: Add .clusterBy() with columns that queries filter on most
# Look at the benchmark queries below - what columns appear in WHERE clauses?
# Docs: https://docs.databricks.com/en/delta/clustering.html

nemweb_data.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "false") \
    # Add .clusterBy(...) here
    .saveAsTable("nemweb_liquid_clustered")

print("Created: nemweb_liquid_clustered")

# COMMAND ----------

# Run OPTIMIZE to apply clustering
spark.sql("OPTIMIZE nemweb_liquid_clustered")
print("OPTIMIZE complete - data is now clustered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Create Partitioned Table with Generated Columns (4 minutes)
# MAGIC
# MAGIC Traditional approach: extract date parts into generated columns, then partition.
# MAGIC
# MAGIC **Reference:** [Generated Columns](https://docs.databricks.com/en/delta/generated-columns.html)
# MAGIC
# MAGIC ### TODO 3b.2: Complete the CREATE TABLE statement
# MAGIC
# MAGIC Add generated columns for year and month, then partition by them.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS nemweb_partitioned")

# TODO 3b.2: Create a partitioned table with generated columns
# Use GENERATED ALWAYS AS to compute year/month from settlement_date
# Then PARTITION BY those generated columns
# Docs: https://docs.databricks.com/en/delta/generated-columns.html

spark.sql("""
CREATE TABLE nemweb_partitioned (
    settlement_date TIMESTAMP,
    region_id STRING,
    total_demand_mw DOUBLE,
    available_generation_mw DOUBLE,
    net_interchange_mw DOUBLE,
    rrp DOUBLE,
    _loaded_at TIMESTAMP
    -- Add generated columns and PARTITIONED BY clause
)
""")

print("Created: nemweb_partitioned")

# COMMAND ----------

# Insert data (generated columns are auto-computed)
nemweb_data.select(
    "settlement_date", "region_id", "total_demand_mw",
    "available_generation_mw", "net_interchange_mw", "rrp", "_loaded_at"
).write.mode("append").insertInto("nemweb_partitioned")

print("Data inserted - generated columns computed automatically")

spark.sql("OPTIMIZE nemweb_partitioned")
print("OPTIMIZE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Performance Comparison (3 minutes)
# MAGIC
# MAGIC ### Helper Function

# COMMAND ----------

def run_query_with_metrics(query: str, description: str) -> dict:
    """Run a query and capture performance metrics."""
    spark.catalog.clearCache()

    start = time.time()
    df = spark.sql(query)
    result = df.collect()
    elapsed = time.time() - start

    return {
        "description": description,
        "elapsed_seconds": round(elapsed, 3),
        "row_count": len(result)
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3b.3: Write benchmark queries
# MAGIC
# MAGIC Complete the queries below to test both tables. Each query pair should be identical
# MAGIC except for the table name.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1: Single Day Filter
# MAGIC
# MAGIC Filter for March 15, 2024. Test partition/cluster pruning.

# COMMAND ----------

# TODO 3b.3a: Complete the WHERE clause for single day filter
query_liquid_1 = """
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

# Same query for partitioned table
query_partitioned_1 = """
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

result_liquid_1 = run_query_with_metrics(query_liquid_1, "Liquid Clustered")
result_partitioned_1 = run_query_with_metrics(query_partitioned_1, "Partitioned")

print(f"\nLiquid Clustered:  {result_liquid_1['elapsed_seconds']:.3f}s ({result_liquid_1['row_count']} rows)")
print(f"Partitioned:       {result_partitioned_1['elapsed_seconds']:.3f}s ({result_partitioned_1['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Single Region, Full History
# MAGIC
# MAGIC Get daily averages for NSW1 across all dates.

# COMMAND ----------

# TODO 3b.3b: Write queries that filter by a single region
# Get daily averages for one region across all dates
# This tests how well each approach handles non-time-based filters

query_liquid_2 = """
-- Your query filtering nemweb_liquid_clustered by region_id
"""

query_partitioned_2 = """
-- Same query on nemweb_partitioned
"""

print("=" * 60)
print("QUERY 2: Single Region Full History")
print("=" * 60)

result_liquid_2 = run_query_with_metrics(query_liquid_2, "Liquid Clustered")
result_partitioned_2 = run_query_with_metrics(query_partitioned_2, "Partitioned")

print(f"\nLiquid Clustered:  {result_liquid_2['elapsed_seconds']:.3f}s ({result_liquid_2['row_count']} rows)")
print(f"Partitioned:       {result_partitioned_2['elapsed_seconds']:.3f}s ({result_partitioned_2['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Price Spike Detection (Full Scan)
# MAGIC
# MAGIC Find high price events. Neither clustering nor partitioning helps here.

# COMMAND ----------

query_liquid_3 = """
SELECT settlement_date, region_id, rrp, total_demand_mw
FROM nemweb_liquid_clustered
WHERE rrp > 300
ORDER BY rrp DESC
LIMIT 100
"""

query_partitioned_3 = """
SELECT settlement_date, region_id, rrp, total_demand_mw
FROM nemweb_partitioned
WHERE rrp > 300
ORDER BY rrp DESC
LIMIT 100
"""

print("=" * 60)
print("QUERY 3: Price Spike Detection (Full Scan)")
print("=" * 60)

result_liquid_3 = run_query_with_metrics(query_liquid_3, "Liquid Clustered")
result_partitioned_3 = run_query_with_metrics(query_partitioned_3, "Partitioned")

print(f"\nLiquid Clustered:  {result_liquid_3['elapsed_seconds']:.3f}s ({result_liquid_3['row_count']} rows)")
print(f"Partitioned:       {result_partitioned_3['elapsed_seconds']:.3f}s ({result_partitioned_3['row_count']} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

summary_data = [
    Row(query="Q1: Single Day", liquid=result_liquid_1['elapsed_seconds'], partitioned=result_partitioned_1['elapsed_seconds']),
    Row(query="Q2: Single Region", liquid=result_liquid_2['elapsed_seconds'], partitioned=result_partitioned_2['elapsed_seconds']),
    Row(query="Q3: Price Spikes", liquid=result_liquid_3['elapsed_seconds'], partitioned=result_partitioned_3['elapsed_seconds']),
]

summary_df = spark.createDataFrame(summary_data)
summary_df = summary_df.withColumn("winner",
    when(col("liquid") < col("partitioned"), "Liquid")
    .when(col("partitioned") < col("liquid"), "Partitioned")
    .otherwise("Tie")
)

print("\n" + "=" * 60)
print("PERFORMANCE SUMMARY (seconds)")
print("=" * 60)
summary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Analysis Questions
# MAGIC
# MAGIC ### TODO 3b.4: Answer these questions based on your results
# MAGIC
# MAGIC 1. **Which approach performed better for time-based queries (Q1)?**
# MAGIC    - Your answer:
# MAGIC
# MAGIC 2. **Which approach performed better for region-based queries (Q2)?**
# MAGIC    - Your answer:
# MAGIC
# MAGIC 3. **Why did neither approach help with Q3 (price spikes)?**
# MAGIC    - Your answer:
# MAGIC
# MAGIC 4. **For NEMWEB data, which approach would you recommend and why?**
# MAGIC    - Your answer:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Scenario | Recommended Approach | Why |
# MAGIC |----------|---------------------|-----|
# MAGIC | New tables | **Liquid Clustering** | Simpler, flexible, modern |
# MAGIC | Time + dimension queries | **Liquid Clustering** | Handles both filter types |
# MAGIC | High cardinality keys | **Liquid Clustering** | Avoids small file problem |
# MAGIC | Strict partition pruning | **Partitioning** | Explicit boundaries |
# MAGIC | Existing partitioned tables | Keep or migrate | Migration requires rewrite |
# MAGIC
# MAGIC ## References
# MAGIC
# MAGIC - [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
# MAGIC - [Generated Columns](https://docs.databricks.com/en/delta/generated-columns.html)
# MAGIC - [Partitioning Best Practices](https://docs.databricks.com/en/tables/partitions.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

nemweb_data.unpersist()

# Uncomment to clean up test tables
# spark.sql("DROP TABLE IF EXISTS nemweb_liquid_clustered")
# spark.sql("DROP TABLE IF EXISTS nemweb_partitioned")
# spark.sql(f"DROP SCHEMA IF EXISTS {SOURCE_CATALOG}.{TARGET_SCHEMA}")
