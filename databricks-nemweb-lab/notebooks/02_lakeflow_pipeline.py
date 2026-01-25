# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2: Lakeflow Declarative Pipeline Integration
# MAGIC
# MAGIC **Time:** 10 minutes
# MAGIC
# MAGIC In this exercise, you'll integrate your custom NEMWEB data source into a
# MAGIC Lakeflow Declarative Pipeline with data quality expectations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Configure custom data sources in Lakeflow pipelines
# MAGIC 2. Define data quality expectations
# MAGIC 3. Implement bronze-silver-gold medallion architecture
# MAGIC 4. Monitor pipeline execution and data quality metrics

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
# MAGIC Custom data sources work seamlessly - just use your format name!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Lakeflow

# COMMAND ----------

# New syntax: pyspark.pipelines replaces dlt (DBR 15.4+/Spark 4.0+)
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

# Import our custom data source (from Exercise 1)
# This would normally be: from nemweb_datasource import NemwebDataSource

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Bronze Layer - Raw Ingestion
# MAGIC
# MAGIC The bronze layer captures raw data from the source with minimal transformation.
# MAGIC
# MAGIC ### TODO 2.1: Complete the bronze table definition

# COMMAND ----------

@dp.table(
    name="nemweb_bronze",
    comment="Raw NEMWEB dispatch region data from custom data source",
    table_properties={
        "quality": "bronze",
        "source": "nemweb.com.au"
    }
)
def nemweb_bronze():
    """
    TODO 2.1: Read from your custom NEMWEB data source (from Exercise 1).
    Add an _ingested_at timestamp column.
    Docs: https://docs.databricks.com/en/pyspark/datasources.html#use-a-custom-data-source
    """
    return (
        spark.read
        # Your code here
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Data Quality Expectations
# MAGIC
# MAGIC Lakeflow expectations define data quality rules. Failed rows can be:
# MAGIC - **warn**: Log and continue
# MAGIC - **drop**: Remove invalid rows
# MAGIC - **fail**: Stop pipeline execution
# MAGIC
# MAGIC ### TODO 2.2: Add data quality expectations

# COMMAND ----------

@dp.table(
    name="nemweb_silver",
    comment="Cleansed NEMWEB data with quality checks"
)
@dp.expect_or_drop("valid_region", "REGIONID IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
# TODO 2.2: Add 2-3 more @dp.expect_or_drop decorators for data quality
# Consider: What makes demand data invalid? What are impossible values?
# Docs: https://docs.databricks.com/aws/en/ldp/expectations 
def nemweb_silver():
    """
    Cleanse and validate NEMWEB data.

    Transformations:
    - Apply data quality expectations
    - Cast types explicitly
    - Add derived columns
    """
    return (
        dp.read("nemweb_bronze")
        .select(
            col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
            col("REGIONID").alias("region_id"),
            col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
            col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
            col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
            col("DISPATCHINTERVAL").alias("dispatch_interval"),
            col("_ingested_at")
        )
        .withColumn("demand_generation_ratio",
                    col("total_demand_mw") / col("available_generation_mw"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Gold Layer - Aggregations
# MAGIC
# MAGIC The gold layer provides business-ready aggregations.
# MAGIC
# MAGIC ### TODO 2.3: Complete the gold table aggregation

# COMMAND ----------

@dp.table(
    name="nemweb_gold_hourly",
    comment="Hourly regional demand aggregations"
)
def nemweb_gold_hourly():
    """
    TODO 2.3: Add aggregations for hourly demand metrics.
    Include: average, max, min demand, and count of intervals.
    Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions
    """
    return (
        dp.read("nemweb_silver")
        .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
        .groupBy("region_id", "hour")
        .agg(
            # Your aggregations here
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Gold Layer - Cross-Region Analysis

# COMMAND ----------

@dp.table(
    name="nemweb_gold_regional_summary",
    comment="Daily regional summary with interconnector flows"
)
def nemweb_gold_regional_summary():
    """
    Daily summary of regional demand and interconnector flows.
    """
    return (
        dp.read("nemweb_silver")
        .withColumn("date", expr("date(settlement_date)"))
        .groupBy("region_id", "date")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            max("total_demand_mw").alias("peak_demand_mw"),
            avg("net_interchange_mw").alias("avg_net_interchange_mw"),
            avg("demand_generation_ratio").alias("avg_demand_gen_ratio"),
            count("*").alias("interval_count")
        )
        .withColumn("is_net_exporter",
                    col("avg_net_interchange_mw") < 0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC
# MAGIC To run this as a Lakeflow pipeline, create a pipeline configuration:

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "nemweb-ingestion-pipeline",
# MAGIC   "target": "nemweb_data",
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "num_workers": 2
# MAGIC     }
# MAGIC   ],
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/path/to/02_lakeflow_pipeline"}}
# MAGIC   ],
# MAGIC   "configuration": {
# MAGIC     "nemweb.start_date": "2024-01-01",
# MAGIC     "nemweb.end_date": "2024-01-31"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC This cell simulates the pipeline logic for validation (won't work as actual DLT):

# COMMAND ----------

def validate_pipeline_logic():
    """Validate pipeline logic without running actual DLT."""

    # Test bronze table logic
    print("Testing bronze layer...")

    # Simulate data
    test_data = [
        ("2024-01-01 00:05:00", "NSW1", "7500.5", "8000.0", "-200.5"),
        ("2024-01-01 00:10:00", "VIC1", "5200.3", "5500.0", "150.2"),
        ("2024-01-01 00:10:00", "INVALID", "-100", "0", "0"),  # Should be dropped
    ]

    df = spark.createDataFrame(test_data, [
        "SETTLEMENTDATE", "REGIONID", "TOTALDEMAND",
        "AVAILABLEGENERATION", "NETINTERCHANGE"
    ])

    print(f"  Input rows: {df.count()}")

    # Apply silver transformations
    silver_df = (df
        .filter(col("REGIONID").isin("NSW1", "QLD1", "SA1", "VIC1", "TAS1"))
        .filter(col("TOTALDEMAND").cast("double") > 0)
    )

    print(f"  After quality checks: {silver_df.count()} rows")
    print("✓ Pipeline logic validated")

    return True

validate_pipeline_logic()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Data Quality
# MAGIC
# MAGIC After running the pipeline, check data quality metrics:
# MAGIC
# MAGIC ```sql
# MAGIC -- View expectation results
# MAGIC SELECT * FROM event_log
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress:data_quality IS NOT NULL
# MAGIC ORDER BY timestamp DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Your pipeline is configured! Now proceed to:
# MAGIC - **Notebook 03**: Cluster sizing analysis to optimize performance
