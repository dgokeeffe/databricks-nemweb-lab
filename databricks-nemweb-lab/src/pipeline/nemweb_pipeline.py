"""
NEMWEB Pipeline Definitions for Apache Spark Declarative Pipelines

This file defines the pipeline datasets using the pyspark.pipelines API.
Can be validated locally with: spark-pipelines dry-run

Usage:
    cd src/pipeline
    spark-pipelines dry-run
    spark-pipelines run
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min, count

# Import spark from Databricks SDK for IDE support and local development
# This works in Databricks (shadows the global) and locally with SSH tunnel
from databricks.sdk.runtime import spark


# =============================================================================
# Bronze Layer - Raw Ingestion
# =============================================================================

@dp.table(
    name="nemweb_bronze",
    comment="Raw NEMWEB dispatch region data"
)
def nemweb_bronze():
    """
    Ingest raw NEMWEB data.

    In production, this would use the custom nemweb data source.
    For local testing, we use sample data.
    """
    # For local testing, create sample data
    # In Databricks, this would be: spark.read.format("nemweb").load()
    return (
        spark.createDataFrame([
            ("2024-01-01 00:05:00", "1", "NSW1", "1", "0", 7500.5, 8000.0, 7800.0, 7600.0, 7900.0, 7500.0, -200.5),
            ("2024-01-01 00:05:00", "1", "VIC1", "1", "0", 5200.3, 5500.0, 5300.0, 5100.0, 5400.0, 5100.0, 150.2),
            ("2024-01-01 00:05:00", "1", "QLD1", "1", "0", 6100.8, 6800.0, 6500.0, 6000.0, 6600.0, 5900.0, -50.5),
            ("2024-01-01 00:05:00", "1", "SA1", "1", "0", 1800.2, 2100.0, 1900.0, 1700.0, 2000.0, 1650.0, 100.3),
            ("2024-01-01 00:05:00", "1", "TAS1", "1", "0", 1100.5, 1400.0, 1200.0, 1050.0, 1300.0, 1000.0, -50.0),
        ], [
            "SETTLEMENTDATE", "RUNNO", "REGIONID", "DISPATCHINTERVAL", "INTERVENTION",
            "TOTALDEMAND", "AVAILABLEGENERATION", "AVAILABLELOAD", "DEMANDFORECAST",
            "DISPATCHABLEGENERATION", "DISPATCHABLELOAD", "NETINTERCHANGE"
        ])
        .withColumn("_ingested_at", current_timestamp())
    )


# =============================================================================
# Silver Layer - Cleansed Data
# =============================================================================

@dp.table(
    name="nemweb_silver",
    comment="Cleansed NEMWEB data with quality checks"
)
def nemweb_silver():
    """
    Cleanse and validate NEMWEB data.

    Transformations:
    - Cast types explicitly
    - Rename columns to snake_case
    - Add derived columns
    """
    return (
        spark.table("nemweb_bronze")
        .select(
            col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
            col("REGIONID").alias("region_id"),
            col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
            col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
            col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
            col("DISPATCHINTERVAL").alias("dispatch_interval"),
            col("_ingested_at")
        )
        # Filter invalid data
        .filter(col("region_id").isin("NSW1", "QLD1", "SA1", "VIC1", "TAS1"))
        .filter(col("total_demand_mw") > 0)
        .filter(col("settlement_date").isNotNull())
        .filter(col("total_demand_mw") < 20000)
        # Add derived columns
        .withColumn("demand_generation_ratio",
                    col("total_demand_mw") / col("available_generation_mw"))
    )


# =============================================================================
# Gold Layer - Aggregations
# =============================================================================

@dp.table(
    name="nemweb_gold_hourly",
    comment="Hourly regional demand aggregations"
)
def nemweb_gold_hourly():
    """
    Aggregate demand metrics by hour and region.
    """
    return (
        spark.table("nemweb_silver")
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


@dp.materialized_view(
    name="nemweb_gold_daily",
    comment="Daily regional summary"
)
def nemweb_gold_daily():
    """Daily aggregations for regional analysis."""
    return (
        spark.table("nemweb_silver")
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
