# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2: Lakeflow Declarative Pipeline
# MAGIC
# MAGIC **Time:** 10 minutes
# MAGIC
# MAGIC In this exercise, you'll create a Lakeflow Declarative Pipeline that processes
# MAGIC the NEMWEB data loaded in Exercise 00.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Create a Lakeflow pipeline as a plain Python file
# MAGIC 2. Define data quality expectations
# MAGIC 3. Implement bronze-silver-gold medallion architecture
# MAGIC 4. Deploy and run the pipeline
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **00_setup_and_validation.py** first to load data into Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background: Lakeflow Declarative Pipelines
# MAGIC
# MAGIC Lakeflow (formerly DLT) provides:
# MAGIC - **Declarative ETL**: Define "what" not "how"
# MAGIC - **Data Quality**: Built-in expectations framework
# MAGIC - **Automatic Lineage**: Full data lineage tracking
# MAGIC - **Change Data Capture**: Automatic incremental processing
# MAGIC
# MAGIC **Key Change:** Pipelines are now plain Python files using `pyspark.pipelines`.
# MAGIC No more notebook format required!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Your Pipeline File (5 minutes)
# MAGIC
# MAGIC Create a new file at `pipelines/my_nemweb_pipeline.py` with the following structure.
# MAGIC
# MAGIC ### TODO 2.1: Create the pipeline file
# MAGIC
# MAGIC Create the file with your editor or run:
# MAGIC ```python
# MAGIC # You can copy the template below to create your file
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Template
# MAGIC
# MAGIC Copy this to `pipelines/my_nemweb_pipeline.py`:
# MAGIC
# MAGIC ```python
# MAGIC """
# MAGIC NEMWEB Lakeflow Pipeline - Exercise 2
# MAGIC
# MAGIC A bronze-silver-gold pipeline for Australian electricity market data.
# MAGIC """
# MAGIC
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min, count
# MAGIC
# MAGIC # Configuration - set these in the pipeline settings or use defaults
# MAGIC SOURCE_CATALOG = "workspace"
# MAGIC SOURCE_SCHEMA = "nemweb_lab"
# MAGIC SOURCE_TABLE = "nemweb_dispatch_regionsum"
# MAGIC
# MAGIC
# MAGIC # =============================================================================
# MAGIC # BRONZE LAYER - Raw data with ingestion timestamp
# MAGIC # =============================================================================
# MAGIC
# MAGIC @dp.table(
# MAGIC     name="nemweb_bronze",
# MAGIC     comment="Raw NEMWEB dispatch data with ingestion metadata",
# MAGIC     table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def nemweb_bronze():
# MAGIC     """
# MAGIC     Bronze layer: Read from pre-loaded Delta table.
# MAGIC
# MAGIC     TODO 2.1a: Complete the read from the source table
# MAGIC
# MAGIC     Hint: Use spark.read.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
# MAGIC     """
# MAGIC     from databricks.sdk.runtime import spark
# MAGIC
# MAGIC     source_path = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
# MAGIC
# MAGIC     return (
# MAGIC         spark.read.table(source_path)
# MAGIC         # TODO: Add .withColumn("_ingested_at", current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC # =============================================================================
# MAGIC # SILVER LAYER - Cleansed with data quality expectations
# MAGIC # =============================================================================
# MAGIC
# MAGIC @dp.table(
# MAGIC     name="nemweb_silver",
# MAGIC     comment="Cleansed NEMWEB data with quality checks"
# MAGIC )
# MAGIC @dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
# MAGIC # TODO 2.2: Add more expectations below
# MAGIC # @dp.expect_or_drop("valid_demand", "total_demand_mw > 0")
# MAGIC # @dp.expect_or_drop("valid_timestamp", "settlement_date IS NOT NULL")
# MAGIC def nemweb_silver():
# MAGIC     """
# MAGIC     Silver layer: Apply data quality rules and standardize column names.
# MAGIC
# MAGIC     TODO 2.2: Add data quality expectations using @dp.expect_or_drop decorator
# MAGIC     """
# MAGIC     return (
# MAGIC         dp.read("nemweb_bronze")
# MAGIC         .select(
# MAGIC             col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
# MAGIC             col("REGIONID").alias("region_id"),
# MAGIC             col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
# MAGIC             col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
# MAGIC             col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
# MAGIC             col("_ingested_at")
# MAGIC         )
# MAGIC         .withColumn("demand_generation_ratio",
# MAGIC                     col("total_demand_mw") / col("available_generation_mw"))
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC # =============================================================================
# MAGIC # GOLD LAYER - Business aggregations
# MAGIC # =============================================================================
# MAGIC
# MAGIC @dp.table(
# MAGIC     name="nemweb_gold_hourly",
# MAGIC     comment="Hourly regional demand aggregations"
# MAGIC )
# MAGIC def nemweb_gold_hourly():
# MAGIC     """
# MAGIC     Gold layer: Hourly aggregations for reporting.
# MAGIC
# MAGIC     TODO 2.3: Add aggregations for max_demand_mw, min_demand_mw, interval_count
# MAGIC     """
# MAGIC     return (
# MAGIC         dp.read("nemweb_silver")
# MAGIC         .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
# MAGIC         .groupBy("region_id", "hour")
# MAGIC         .agg(
# MAGIC             avg("total_demand_mw").alias("avg_demand_mw"),
# MAGIC             # TODO 2.3: Add max, min, count aggregations
# MAGIC             # max("total_demand_mw").alias("max_demand_mw"),
# MAGIC             # min("total_demand_mw").alias("min_demand_mw"),
# MAGIC             # count("*").alias("interval_count"),
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Deploy Your Pipeline (3 minutes)
# MAGIC
# MAGIC ### Option A: Using Databricks UI
# MAGIC
# MAGIC 1. Go to **Workflows** > **Pipelines** > **Create pipeline**
# MAGIC 2. Name: `nemweb-exercise-pipeline`
# MAGIC 3. Source code: Select your `pipelines/my_nemweb_pipeline.py`
# MAGIC 4. Target schema: `workspace.nemweb_lab_exercise`
# MAGIC 5. Click **Create** then **Start**
# MAGIC
# MAGIC ### Option B: Using Databricks CLI
# MAGIC
# MAGIC ```bash
# MAGIC # Create pipeline from YAML config
# MAGIC databricks pipelines create --json '{
# MAGIC   "name": "nemweb-exercise-pipeline",
# MAGIC   "target": "nemweb_lab_exercise",
# MAGIC   "continuous": false,
# MAGIC   "development": true,
# MAGIC   "libraries": [
# MAGIC     {"file": {"path": "/Workspace/Users/.../pipelines/my_nemweb_pipeline.py"}}
# MAGIC   ]
# MAGIC }'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Validate Without Running Pipeline
# MAGIC
# MAGIC Let's test the pipeline logic locally before deploying.

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, min, count, expr, current_timestamp
from databricks.sdk.runtime import spark, display

# Configuration - should match your setup
CATALOG = dbutils.widgets.get("catalog") if "dbutils" in dir() else "workspace"
SCHEMA = dbutils.widgets.get("schema") if "dbutils" in dir() else "nemweb_lab"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.nemweb_dispatch_regionsum"

# Verify source exists
if not spark.catalog.tableExists(SOURCE_TABLE):
    print(f"Source table {SOURCE_TABLE} not found!")
    print("Run 00_setup_and_validation.py first to load data.")
else:
    print(f"Source table: {SOURCE_TABLE}")
    print(f"Row count: {spark.table(SOURCE_TABLE).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Bronze Layer

# COMMAND ----------

# Simulate bronze layer
bronze_df = (
    spark.table(SOURCE_TABLE)
    .withColumn("_ingested_at", current_timestamp())
)

print("Bronze layer preview:")
display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Silver Layer with Data Quality

# COMMAND ----------

# Simulate silver layer with quality checks
silver_df = (
    bronze_df
    .select(
        col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
        col("REGIONID").alias("region_id"),
        col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
        col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
        col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
        col("_ingested_at")
    )
    .withColumn("demand_generation_ratio",
                col("total_demand_mw") / col("available_generation_mw"))
)

# Apply expectations manually for testing
valid_regions = ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]
quality_checked = (
    silver_df
    .filter(col("region_id").isin(valid_regions))
    .filter(col("total_demand_mw") > 0)
    .filter(col("settlement_date").isNotNull())
)

print(f"Silver layer:")
print(f"  Before quality checks: {silver_df.count():,} rows")
print(f"  After quality checks:  {quality_checked.count():,} rows")
print(f"  Rows dropped:          {silver_df.count() - quality_checked.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Gold Layer Aggregations

# COMMAND ----------

# Simulate gold layer aggregation
gold_df = (
    quality_checked
    .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
    .groupBy("region_id", "hour")
    .agg(
        avg("total_demand_mw").alias("avg_demand_mw"),
        max("total_demand_mw").alias("max_demand_mw"),
        min("total_demand_mw").alias("min_demand_mw"),
        count("*").alias("interval_count"),
    )
)

print(f"Gold layer (hourly aggregations): {gold_df.count():,} rows")
display(gold_df.orderBy("hour", "region_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You learned how to create a Lakeflow pipeline:
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `@dp.table()` | Declare a managed table |
# MAGIC | `@dp.expect_or_drop()` | Data quality expectations |
# MAGIC | `dp.read("table")` | Read from upstream table |
# MAGIC | Bronze layer | Raw data with metadata |
# MAGIC | Silver layer | Cleansed with quality rules |
# MAGIC | Gold layer | Business aggregations |
# MAGIC
# MAGIC **Key Insight:** Lakeflow pipelines are now plain Python files using `pyspark.pipelines`.
# MAGIC No notebook format required!
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Exercise 3** for cluster sizing and optimization.
