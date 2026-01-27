# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3: Delta Lake Optimization Techniques
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC This is the **final main exercise** of the lab. You'll master Delta Lake optimization
# MAGIC techniques that are critical for production workloads on Databricks.
# MAGIC
# MAGIC ## Topics Covered
# MAGIC 1. **Liquid Clustering** - Modern data layout (DBR 13.3+)
# MAGIC 2. **Generated Columns + Partitioning** - Traditional approach
# MAGIC 3. **OPTIMIZE** - File compaction and data layout
# MAGIC 4. **VACUUM** - Storage cleanup
# MAGIC 5. **Predictive Optimization** - Automatic maintenance
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand when to use liquid clustering vs. partitioning
# MAGIC 2. Implement both approaches for the same dataset
# MAGIC 3. Measure query performance differences
# MAGIC 4. Configure Delta table maintenance for production
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **00_setup_and_validation.py** first to pre-load NEMWEB data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Parameters should match those used in 00_setup_and_validation.py

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql import Row
from databricks.sdk.runtime import spark, display
import time

# Create widgets with defaults matching setup notebook
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("schema", "nemweb_lab", "Source Schema")
dbutils.widgets.text("table", "nemweb_raw", "Source Table")
dbutils.widgets.text("target_schema", "nemweb_optimization_lab", "Target Schema")

# Get configuration from widgets
SOURCE_CATALOG = dbutils.widgets.get("catalog")
SOURCE_SCHEMA = dbutils.widgets.get("schema")
SOURCE_TABLE = dbutils.widgets.get("table")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")

print("Configuration")
print("=" * 50)
print(f"Catalog:       {SOURCE_CATALOG}")
print(f"Source Schema: {SOURCE_SCHEMA}")
print(f"Source Table:  {SOURCE_TABLE}")
print(f"Target Schema: {TARGET_SCHEMA}")

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
#
# Add .clusterBy("column1", "column2") before .saveAsTable()

(nemweb_data.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "false")
    # TODO: Add .clusterBy(...) here
    .saveAsTable("nemweb_liquid_clustered"))

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
#
# Requirements:
#   - Filter WHERE region_id = 'NSW1'
#   - Group by DATE(settlement_date)
#   - Calculate AVG(total_demand_mw) and AVG(rrp)
#   - Order by date
#
# Hint: Use DATE(settlement_date) to extract the date part

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
# MAGIC ### Understanding the Results
# MAGIC
# MAGIC Before answering, consider what each query tests:
# MAGIC
# MAGIC | Query | Filter Type | What It Tests |
# MAGIC |-------|-------------|---------------|
# MAGIC | Q1: Single Day | Time-based (`settlement_date`) | Partition pruning vs. cluster data skipping |
# MAGIC | Q2: Single Region | Dimension-based (`region_id`) | How well each handles non-partition filters |
# MAGIC | Q3: Price Spikes | Value-based (`rrp > 300`) | Neither is optimized for this - full scan |
# MAGIC
# MAGIC **Key insight:** Liquid clustering can skip data for ANY clustering key, while partitioning
# MAGIC only helps when you filter on the partition columns.
# MAGIC
# MAGIC ### TODO 3b.4: Answer these questions based on your results
# MAGIC
# MAGIC 1. **Which approach performed better for time-based queries (Q1)?**
# MAGIC    - Your answer:
# MAGIC    - *Hint: Both should perform similarly since both optimize for time-based access*
# MAGIC
# MAGIC 2. **Which approach performed better for region-based queries (Q2)?**
# MAGIC    - Your answer:
# MAGIC    - *Hint: Liquid clustering includes region_id as a clustering key*
# MAGIC
# MAGIC 3. **Why did neither approach help with Q3 (price spikes)?**
# MAGIC    - Your answer:
# MAGIC    - *Hint: What column are we filtering on? Is it a clustering/partition key?*
# MAGIC
# MAGIC 4. **For NEMWEB data, which approach would you recommend and why?**
# MAGIC    - Your answer:
# MAGIC    - *Consider: What queries will users typically run? Time-based? Region-based? Both?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Delta Table Maintenance
# MAGIC
# MAGIC ### OPTIMIZE - File Compaction
# MAGIC
# MAGIC The `OPTIMIZE` command compacts small files and applies clustering/Z-ORDER.
# MAGIC
# MAGIC ```sql
# MAGIC -- Basic OPTIMIZE (compacts files, applies clustering if defined)
# MAGIC OPTIMIZE nemweb_liquid_clustered
# MAGIC
# MAGIC -- OPTIMIZE with Z-ORDER (legacy alternative to liquid clustering)
# MAGIC OPTIMIZE nemweb_legacy ZORDER BY (settlement_date, region_id)
# MAGIC ```
# MAGIC
# MAGIC **When to use:**
# MAGIC - After batch ingestion jobs
# MAGIC - When you see many small files (check with DESCRIBE DETAIL)
# MAGIC - For liquid clustered tables: data gets better organized with each OPTIMIZE
# MAGIC
# MAGIC ### VACUUM - Storage Cleanup
# MAGIC
# MAGIC The `VACUUM` command removes old files that are no longer referenced.
# MAGIC
# MAGIC ```sql
# MAGIC -- Remove files older than 7 days (default retention)
# MAGIC VACUUM nemweb_liquid_clustered
# MAGIC
# MAGIC -- Remove files older than 24 hours (requires safety check override)
# MAGIC VACUUM nemweb_liquid_clustered RETAIN 24 HOURS
# MAGIC ```
# MAGIC
# MAGIC **Important:** VACUUM deletes time travel history! Set retention carefully.
# MAGIC
# MAGIC ### Predictive Optimization (Unity Catalog)
# MAGIC
# MAGIC For managed tables in Unity Catalog, enable automatic OPTIMIZE and VACUUM:
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable at catalog level
# MAGIC ALTER CATALOG my_catalog SET (
# MAGIC   'predictiveOptimization' = 'ENABLE'
# MAGIC )
# MAGIC
# MAGIC -- Or enable per table
# MAGIC ALTER TABLE nemweb_bronze SET TBLPROPERTIES (
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Automatic file compaction when needed
# MAGIC - No manual OPTIMIZE scheduling required
# MAGIC - Intelligent scheduling based on table activity

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
# MAGIC | Production maintenance | **Predictive Optimization** | Automatic OPTIMIZE/VACUUM |
# MAGIC
# MAGIC ### Delta Optimization Checklist
# MAGIC
# MAGIC - [ ] Choose data layout strategy (liquid clustering preferred for new tables)
# MAGIC - [ ] Run OPTIMIZE after batch loads or enable predictive optimization
# MAGIC - [ ] Configure VACUUM retention based on time travel needs
# MAGIC - [ ] Monitor file count and sizes with DESCRIBE DETAIL
# MAGIC
# MAGIC ## References
# MAGIC
# MAGIC - [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
# MAGIC - [Generated Columns](https://docs.databricks.com/en/delta/generated-columns.html)
# MAGIC - [OPTIMIZE Command](https://docs.databricks.com/en/delta/optimize.html)
# MAGIC - [VACUUM Command](https://docs.databricks.com/en/delta/vacuum.html)
# MAGIC - [Predictive Optimization](https://docs.databricks.com/en/optimizations/predictive-optimization.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

nemweb_data.unpersist()

# Uncomment to clean up test tables
# spark.sql("DROP TABLE IF EXISTS nemweb_liquid_clustered")
# spark.sql("DROP TABLE IF EXISTS nemweb_partitioned")
# spark.sql(f"DROP SCHEMA IF EXISTS {SOURCE_CATALOG}.{TARGET_SCHEMA}")
