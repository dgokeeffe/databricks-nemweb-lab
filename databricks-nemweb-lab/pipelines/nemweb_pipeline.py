"""
NEMWEB Pipeline Definitions for Apache Spark Declarative Pipelines

This file defines the pipeline datasets using the pyspark.pipelines API.
Reads from pre-loaded Delta tables created by 00_setup_and_validation.py.

Source Tables (DISPATCHREGIONSUM schema from AEMO MMS Data Model):
  - SETTLEMENTDATE: Dispatch interval timestamp (5-minute granularity)
  - REGIONID: NEM region (NSW1, QLD1, SA1, VIC1, TAS1)
  - TOTALDEMAND: Regional demand in MW
  - AVAILABLEGENERATION: Available generation capacity in MW
  - NETINTERCHANGE: Net flow into region (negative = exporting)
  - Plus: RUNNO, DISPATCHINTERVAL, INTERVENTION, AVAILABLELOAD,
          DEMANDFORECAST, DISPATCHABLEGENERATION, DISPATCHABLELOAD

Reference: https://nemweb.com.au/Reports/Current/MMSDataModelReport/

Usage:
    cd pipelines
    spark-pipelines dry-run
    spark-pipelines run
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min, count

# Import spark from Databricks SDK for IDE support and local development
# This works in Databricks (shadows the global) and locally with SSH tunnel
from databricks.sdk.runtime import spark


# =============================================================================
# Configuration - matches 00_setup_and_validation.py defaults
# =============================================================================
SOURCE_CATALOG = "workspace"
SOURCE_SCHEMA = "nemweb_lab"
SOURCE_TABLE = "nemweb_dispatch_regionsum"


# =============================================================================
# Bronze Layer - Raw Ingestion with Metadata
# =============================================================================

@dp.table(
    name="nemweb_bronze",
    comment="Raw NEMWEB dispatch region data with ingestion metadata"
)
def nemweb_bronze():
    """
    Ingest raw NEMWEB data from pre-loaded Delta table.

    Source: workspace.nemweb_lab.nemweb_dispatch_regionsum
    Created by: 00_setup_and_validation.py

    Columns from DISPATCHREGIONSUM (AEMO MMS Data Model):
      - SETTLEMENTDATE: Dispatch interval timestamp
      - REGIONID: NEM region identifier
      - TOTALDEMAND: Total regional demand (MW)
      - AVAILABLEGENERATION: Available generation capacity (MW)
      - NETINTERCHANGE: Net interconnector flow (MW, negative = export)
    """
    source_path = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"

    return (
        spark.read.table(source_path)
        .withColumn("_ingested_at", current_timestamp())
    )


# =============================================================================
# Silver Layer - Cleansed Data with Quality Checks
# =============================================================================

@dp.table(
    name="nemweb_silver",
    comment="Cleansed NEMWEB data with quality checks"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_demand", "total_demand_mw > 0 AND total_demand_mw < 20000")
@dp.expect_or_drop("valid_timestamp", "settlement_date IS NOT NULL")
def nemweb_silver():
    """
    Cleanse and validate NEMWEB data.

    Transformations:
    - Cast SETTLEMENTDATE string to timestamp
    - Rename columns to snake_case convention
    - Add demand/generation ratio metric
    - Apply data quality expectations (drop invalid rows)
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


# =============================================================================
# Gold Layer - Business Aggregations
# =============================================================================

@dp.table(
    name="nemweb_gold_hourly",
    comment="Hourly regional demand aggregations"
)
def nemweb_gold_hourly():
    """
    Aggregate demand metrics by hour and region.

    Use case: Operational dashboards, hourly demand patterns
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


@dp.materialized_view(
    name="nemweb_gold_daily",
    comment="Daily regional summary"
)
def nemweb_gold_daily():
    """
    Daily aggregations for regional analysis.

    Use case: Daily reporting, capacity planning, export/import analysis
    """
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


# =============================================================================
# SQL Equivalents
# =============================================================================
#
# The Python definitions above are equivalent to these SQL statements.
# You can use either approach in Lakeflow pipelines.
#
# -----------------------------------------------------------------------------
# Bronze Layer
# -----------------------------------------------------------------------------
# CREATE OR REFRESH STREAMING TABLE nemweb_bronze
# COMMENT 'Raw NEMWEB dispatch region data with ingestion metadata'
# AS SELECT
#     *,
#     current_timestamp() AS _ingested_at
# FROM workspace.nemweb_lab.nemweb_dispatch_regionsum;
#
# -----------------------------------------------------------------------------
# Silver Layer
# -----------------------------------------------------------------------------
# CREATE OR REFRESH STREAMING TABLE nemweb_silver (
#     CONSTRAINT valid_region EXPECT (region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')) ON VIOLATION DROP ROW,
#     CONSTRAINT valid_demand EXPECT (total_demand_mw > 0 AND total_demand_mw < 20000) ON VIOLATION DROP ROW,
#     CONSTRAINT valid_timestamp EXPECT (settlement_date IS NOT NULL) ON VIOLATION DROP ROW
# )
# COMMENT 'Cleansed NEMWEB data with quality checks'
# AS SELECT
#     CAST(SETTLEMENTDATE AS TIMESTAMP) AS settlement_date,
#     REGIONID AS region_id,
#     CAST(TOTALDEMAND AS DOUBLE) AS total_demand_mw,
#     CAST(AVAILABLEGENERATION AS DOUBLE) AS available_generation_mw,
#     CAST(NETINTERCHANGE AS DOUBLE) AS net_interchange_mw,
#     DISPATCHINTERVAL AS dispatch_interval,
#     _ingested_at,
#     CAST(TOTALDEMAND AS DOUBLE) / CAST(AVAILABLEGENERATION AS DOUBLE) AS demand_generation_ratio
# FROM STREAM(LIVE.nemweb_bronze);
#
# -----------------------------------------------------------------------------
# Gold Layer - Hourly
# -----------------------------------------------------------------------------
# CREATE OR REFRESH MATERIALIZED VIEW nemweb_gold_hourly
# COMMENT 'Hourly regional demand aggregations'
# AS SELECT
#     region_id,
#     date_trunc('hour', settlement_date) AS hour,
#     AVG(total_demand_mw) AS avg_demand_mw,
#     MAX(total_demand_mw) AS max_demand_mw,
#     MIN(total_demand_mw) AS min_demand_mw,
#     COUNT(*) AS interval_count,
#     AVG(demand_generation_ratio) AS avg_demand_gen_ratio
# FROM LIVE.nemweb_silver
# GROUP BY region_id, date_trunc('hour', settlement_date);
#
# -----------------------------------------------------------------------------
# Gold Layer - Daily
# -----------------------------------------------------------------------------
# CREATE OR REFRESH MATERIALIZED VIEW nemweb_gold_daily
# COMMENT 'Daily regional summary'
# AS SELECT
#     region_id,
#     DATE(settlement_date) AS date,
#     AVG(total_demand_mw) AS avg_demand_mw,
#     MAX(total_demand_mw) AS peak_demand_mw,
#     MIN(total_demand_mw) AS min_demand_mw,
#     AVG(net_interchange_mw) AS avg_net_interchange_mw,
#     COUNT(*) AS interval_count,
#     AVG(net_interchange_mw) < 0 AS is_net_exporter
# FROM LIVE.nemweb_silver
# GROUP BY region_id, DATE(settlement_date);
