# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB ML Workshop Pipeline
# MAGIC
# MAGIC Streaming pipeline that ingests NEMWEB energy market data for the ML workshop.
# MAGIC Uses Auto Loader to stream extracted CSV files into bronze/silver/gold Delta tables.
# MAGIC
# MAGIC ## Architecture
# MAGIC - **Bronze**: Streaming tables from Auto Loader (raw NEMWEB data, snake_case columns)
# MAGIC - **Silver**: Streaming tables with joins (dispatch + price ready for ML)
# MAGIC - **Gold**: Materialized views for exploration and aggregations
# MAGIC
# MAGIC ## Tables
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `bronze_dispatch_regionsum` | 5-min regional demand/generation |
# MAGIC | Bronze | `bronze_dispatch_price` | 5-min regional spot prices |
# MAGIC | Bronze | `bronze_dispatch_unit_scada` | 5-min unit-level generation |
# MAGIC | Silver | `silver_dispatch_with_price` | Demand + price joined (ML-ready) |
# MAGIC | Gold | `gold_regional_demand_hourly` | Hourly demand by region |
# MAGIC | Gold | `gold_regional_price_summary` | Hourly price statistics |

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = spark.conf.get("pipeline.catalog", "daveok")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

CSV_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/extracted_csv"

print(f"Pipeline config: catalog={CATALOG}, schema={SCHEMA}")
print(f"CSV source path: {CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions
# MAGIC
# MAGIC NEMWEB CSV format: row type prefix (I=header, D=data row).

# COMMAND ----------

SCHEMA_DISPATCH_REGIONSUM = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("SETTLEMENTDATE", StringType()),
    StructField("RUNNO", StringType()),
    StructField("REGIONID", StringType()),
    StructField("DISPATCHINTERVAL", StringType()),
    StructField("INTERVENTION", StringType()),
    StructField("TOTALDEMAND", DoubleType()),
    StructField("AVAILABLEGENERATION", DoubleType()),
    StructField("AVAILABLELOAD", DoubleType()),
    StructField("DEMANDFORECAST", DoubleType()),
    StructField("DISPATCHABLEGENERATION", DoubleType()),
    StructField("DISPATCHABLELOAD", DoubleType()),
    StructField("NETINTERCHANGE", DoubleType()),
])

SCHEMA_DISPATCH_PRICE = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("SETTLEMENTDATE", StringType()),
    StructField("RUNNO", StringType()),
    StructField("REGIONID", StringType()),
    StructField("DISPATCHINTERVAL", StringType()),
    StructField("INTERVENTION", StringType()),
    StructField("RRP", DoubleType()),
    StructField("EEP", DoubleType()),
    StructField("ROP", DoubleType()),
    StructField("APCFLAG", StringType()),
    StructField("MARKETSUSPENDEDFLAG", StringType()),
])

SCHEMA_DISPATCH_UNIT_SCADA = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("SETTLEMENTDATE", StringType()),
    StructField("DUID", StringType()),
    StructField("SCADAVALUE", DoubleType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Streaming Tables with Auto Loader

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_regionsum",
    comment="5-minute regional demand, generation, and interchange from NEMWEB"
)
def bronze_dispatch_regionsum():
    """Regional dispatch summary - primary ML target for load forecasting."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/dispatchregionsum")
        .schema(SCHEMA_DISPATCH_REGIONSUM)
        .load(f"{CSV_PATH}/dispatchregionsum/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("settlement_date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "settlement_date",
            col("REGIONID").alias("region_id"),
            col("TOTALDEMAND").alias("total_demand_mw"),
            col("AVAILABLEGENERATION").alias("available_generation_mw"),
            col("DEMANDFORECAST").alias("demand_forecast_mw"),
            col("NETINTERCHANGE").alias("net_interchange_mw"),
            col("DISPATCHINTERVAL").alias("dispatch_interval"),
            "_ingested_at",
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_price",
    comment="5-minute regional spot prices from NEMWEB"
)
def bronze_dispatch_price():
    """Regional spot prices - key feature for price-responsive demand modelling."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/dispatchprice")
        .schema(SCHEMA_DISPATCH_PRICE)
        .load(f"{CSV_PATH}/dispatchprice/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("settlement_date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "settlement_date",
            col("REGIONID").alias("region_id"),
            col("RRP").alias("rrp"),
            col("ROP").alias("rop"),
            col("APCFLAG").alias("apc_flag"),
            "_ingested_at",
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_unit_scada",
    comment="5-minute real-time generation per power station from NEMWEB"
)
def bronze_dispatch_unit_scada():
    """Unit-level generation - useful for generator dispatch pattern analysis."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/dispatch_unit_scada")
        .schema(SCHEMA_DISPATCH_UNIT_SCADA)
        .load(f"{CSV_PATH}/dispatch_unit_scada/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("settlement_date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "settlement_date",
            col("DUID").alias("duid"),
            col("SCADAVALUE").alias("scada_value_mw"),
            "_ingested_at",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Joined and Enriched Tables

# COMMAND ----------

@dp.table(
    name="silver_dispatch_with_price",
    comment="Joined dispatch demand and price data - ML-ready for load forecasting"
)
def silver_dispatch_with_price():
    """Join regional demand with spot prices for ML training.

    This is the primary table used in the workshop for load forecasting.
    Each row has demand, generation, interchange, and price for a 5-min interval.
    """
    dispatch = spark.readStream.table("bronze_dispatch_regionsum")
    price = spark.readStream.table("bronze_dispatch_price")

    return (
        dispatch.alias("d")
        .join(
            price.alias("p"),
            (col("d.settlement_date") == col("p.settlement_date"))
            & (col("d.region_id") == col("p.region_id")),
            "inner",
        )
        .select(
            col("d.settlement_date"),
            col("d.region_id"),
            col("d.total_demand_mw"),
            col("d.available_generation_mw"),
            col("d.demand_forecast_mw"),
            col("d.net_interchange_mw"),
            col("p.rrp"),
            col("p.rop"),
            current_timestamp().alias("_processed_at"),
        )
    )

# COMMAND ----------

@dp.table(
    name="silver_unit_generation",
    comment="Unit generation with positive values only"
)
@dp.expect_or_drop("positive_generation", "scada_value_mw >= 0")
def silver_unit_generation():
    """Cleansed unit generation data - generators only (no negative/load values)."""
    return (
        spark.readStream.table("bronze_dispatch_unit_scada")
        .select(
            "settlement_date",
            "duid",
            "scada_value_mw",
            "_ingested_at",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Materialized Views for Exploration

# COMMAND ----------

@dp.materialized_view(
    name="gold_regional_demand_hourly",
    comment="Hourly demand and price summary by NEM region"
)
def gold_regional_demand_hourly():
    """Hourly aggregation of demand and price - good for quick exploration."""
    from pyspark.sql.functions import avg, min as _min, max as _max, date_trunc, count

    return (
        spark.read.table("silver_dispatch_with_price")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("region_id", "hour")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            _min("total_demand_mw").alias("min_demand_mw"),
            _max("total_demand_mw").alias("max_demand_mw"),
            avg("rrp").alias("avg_rrp"),
            _min("rrp").alias("min_rrp"),
            _max("rrp").alias("max_rrp"),
            avg("available_generation_mw").alias("avg_available_gen_mw"),
            count("*").alias("interval_count"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_regional_price_summary",
    comment="Daily price statistics by NEM region"
)
def gold_regional_price_summary():
    """Daily price summary - useful for market modelling overview."""
    from pyspark.sql.functions import (
        avg, min as _min, max as _max, stddev, date_trunc, count
    )

    return (
        spark.read.table("silver_dispatch_with_price")
        .withColumn("day", date_trunc("day", col("settlement_date")))
        .groupBy("region_id", "day")
        .agg(
            avg("rrp").alias("avg_rrp"),
            _min("rrp").alias("min_rrp"),
            _max("rrp").alias("max_rrp"),
            stddev("rrp").alias("stddev_rrp"),
            avg("total_demand_mw").alias("avg_demand_mw"),
            _max("total_demand_mw").alias("peak_demand_mw"),
            count("*").alias("interval_count"),
        )
    )
