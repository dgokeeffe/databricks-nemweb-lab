# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 2 - Lakeflow Pipeline Integration
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 2.

# COMMAND ----------

# New syntax: pyspark.pipelines replaces dlt (DBR 15.4+/Spark 4.0+)
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min, count, sum, when

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 2.1: Bronze Layer - Raw Ingestion

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
    Ingest raw NEMWEB data using custom data source.

    SOLUTION 2.1: Complete bronze table
    """
    return (
        spark.read
        .format("nemweb")  # Our custom data source
        .option("regions", "NSW1,QLD1,SA1,VIC1,TAS1")  # All 5 NEM regions
        .option("start_date", spark.conf.get("nemweb.start_date", "2024-01-01"))
        .option("end_date", spark.conf.get("nemweb.end_date", "2024-01-31"))
        .load()
        .withColumn("_ingested_at", current_timestamp())  # Add metadata
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 2.2: Silver Layer with Data Quality

# COMMAND ----------

@dp.table(
    name="nemweb_silver",
    comment="Cleansed NEMWEB data with quality checks"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_demand", "total_demand_mw > 0")  # SOLUTION 2.2a
@dp.expect_or_drop("valid_timestamp", "settlement_date IS NOT NULL")  # SOLUTION 2.2b
@dp.expect_or_drop("reasonable_demand", "total_demand_mw < 20000")  # SOLUTION 2.2c
def nemweb_silver():
    """
    Cleanse and validate NEMWEB data.

    Expectations:
    - valid_region: Only accept known NEM regions
    - valid_demand: Demand must be positive
    - valid_timestamp: Must have settlement date
    - reasonable_demand: Demand under 20,000 MW (Australia's total capacity ~60GW)
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
                    when(col("available_generation_mw") > 0,
                         col("total_demand_mw") / col("available_generation_mw")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 2.3: Gold Layer - Hourly Aggregations

# COMMAND ----------

@dp.table(
    name="nemweb_gold_hourly",
    comment="Hourly regional demand aggregations"
)
def nemweb_gold_hourly():
    """
    Aggregate demand metrics by hour and region.

    SOLUTION 2.3: Complete aggregations
    """
    return (
        dp.read("nemweb_silver")
        .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
        .groupBy("region_id", "hour")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            max("total_demand_mw").alias("max_demand_mw"),
            min("total_demand_mw").alias("min_demand_mw"),
            count("*").alias("interval_count"),
            avg("demand_generation_ratio").alias("avg_demand_gen_ratio")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Gold Tables

# COMMAND ----------

@dp.table(
    name="nemweb_gold_daily",
    comment="Daily regional summary"
)
def nemweb_gold_daily():
    """Daily aggregations for regional analysis."""
    return (
        dp.read("nemweb_silver")
        .withColumn("date", expr("date(settlement_date)"))
        .groupBy("region_id", "date")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            max("total_demand_mw").alias("peak_demand_mw"),
            min("total_demand_mw").alias("min_demand_mw"),
            avg("net_interchange_mw").alias("avg_net_interchange_mw"),
            count("*").alias("interval_count")
        )
        .withColumn("is_net_exporter", col("avg_net_interchange_mw") < 0)
    )


@dp.table(
    name="nemweb_gold_nem_totals",
    comment="NEM-wide totals across all regions"
)
def nemweb_gold_nem_totals():
    """Total NEM demand by hour."""
    return (
        dp.read("nemweb_silver")
        .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
        .groupBy("hour")
        .agg(
            sum("total_demand_mw").alias("total_nem_demand_mw"),
            sum("available_generation_mw").alias("total_available_generation_mw"),
            count("*").alias("region_intervals")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation (Non-DLT Test)

# COMMAND ----------

def validate_pipeline_logic():
    """Validate pipeline logic without running actual DLT."""

    # Create test data
    test_data = [
        ("2024-01-01 00:05:00", "NSW1", "7500.5", "8000.0", "-200.5", "1"),
        ("2024-01-01 00:10:00", "VIC1", "5200.3", "5500.0", "150.2", "2"),
        ("2024-01-01 00:10:00", "INVALID", "-100", "0", "0", "2"),  # Invalid region
        ("2024-01-01 00:15:00", "SA1", "-500", "2000.0", "100.0", "3"),  # Negative demand
        (None, "QLD1", "6000.0", "6500.0", "-50.0", "1"),  # Null timestamp
    ]

    df = spark.createDataFrame(test_data, [
        "SETTLEMENTDATE", "REGIONID", "TOTALDEMAND",
        "AVAILABLEGENERATION", "NETINTERCHANGE", "DISPATCHINTERVAL"
    ])

    print(f"Input rows: {df.count()}")

    # Apply quality checks (simulating expectations)
    valid_regions = ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]

    silver_df = (df
        .filter(col("REGIONID").isin(valid_regions))  # valid_region
        .filter(col("TOTALDEMAND").cast("double") > 0)  # valid_demand
        .filter(col("SETTLEMENTDATE").isNotNull())  # valid_timestamp
        .filter(col("TOTALDEMAND").cast("double") < 20000)  # reasonable_demand
    )

    print(f"After quality checks: {silver_df.count()} rows")
    print(f"Dropped: {df.count() - silver_df.count()} rows")

    # Test aggregation
    gold_df = (silver_df
        .select(
            col("REGIONID").alias("region_id"),
            col("TOTALDEMAND").cast("double").alias("total_demand_mw")
        )
        .groupBy("region_id")
        .agg(
            avg("total_demand_mw").alias("avg_demand"),
            max("total_demand_mw").alias("max_demand"),
            count("*").alias("count")
        )
    )

    print("\nAggregated results:")
    gold_df.show()

    print("\nPipeline logic validated!")
    return True

validate_pipeline_logic()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration Example

# COMMAND ----------

pipeline_config = {
    "name": "nemweb-ingestion-pipeline",
    "target": "nemweb_data",
    "catalog": "main",
    "development": False,
    "continuous": False,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [
        {"notebook": {"path": "/path/to/02_lakeflow_pipeline_solution"}}
    ],
    "configuration": {
        "nemweb.start_date": "2024-01-01",
        "nemweb.end_date": "2024-06-30",
        "spark.sql.shuffle.partitions": "auto"
    }
}

import json
print("Pipeline Configuration:")
print(json.dumps(pipeline_config, indent=2))
