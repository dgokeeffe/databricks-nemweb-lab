# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB MMS Streaming Pipeline
# MAGIC
# MAGIC This pipeline uses Auto Loader to stream NEMWEB CSV files from the volume into
# MAGIC Delta streaming tables. The setup notebook must run first to download and extract
# MAGIC the CSV files.
# MAGIC
# MAGIC ## Architecture
# MAGIC - **Bronze**: Streaming tables using Auto Loader (cloudFiles) via @dp.table()
# MAGIC - **Silver**: Streaming tables with joins and transformations
# MAGIC - **Gold**: Materialized views for aggregations
# MAGIC
# MAGIC Note: In Lakeflow, @dp.table() creates a streaming table when the function returns
# MAGIC a streaming DataFrame (spark.readStream). There is no separate @dp.table decorator.
# MAGIC
# MAGIC ## Bronze Tables
# MAGIC - `bronze_dispatch_regionsum`: Regional demand/generation (5-min)
# MAGIC - `bronze_dispatch_price`: Regional spot prices (5-min)
# MAGIC - `bronze_trading_price`: Trading period prices (30-min)
# MAGIC - `bronze_dispatch_unit_scada`: Real-time unit generation (5-min)
# MAGIC - `bronze_p5min_regionsolution`: Pre-dispatch forecasts (5-min)
# MAGIC - `bronze_p5min_interconnectorsoln`: Interconnector forecasts (5-min)
# MAGIC - `bronze_bidperoffer`: Generator bid availability (daily)
# MAGIC - `bronze_biddayoffer`: Generator price bands (daily)

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

# Get catalog and schema from pipeline configuration
CATALOG = spark.conf.get("pipeline.catalog", "daveok")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

# Volume path for extracted CSV files
CSV_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/extracted_csv"

print(f"Pipeline config: catalog={CATALOG}, schema={SCHEMA}")
print(f"CSV path: {CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions
# MAGIC
# MAGIC NEMWEB CSV files have a specific format with row type prefix (I=header, D=data).

# COMMAND ----------

# Schema for DISPATCHREGIONSUM
# Note: NEMWEB CSVs have format: D,DISPATCH,REGIONSUM,<version>,<settlementdate>,...
SCHEMA_DISPATCH_REGIONSUM = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
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

# Schema for DISPATCHPRICE
SCHEMA_DISPATCH_PRICE = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
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

# Schema for TRADINGPRICE
SCHEMA_TRADING_PRICE = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
    StructField("SETTLEMENTDATE", StringType()),
    StructField("RUNNO", StringType()),
    StructField("REGIONID", StringType()),
    StructField("PERIODID", StringType()),
    StructField("RRP", DoubleType()),
    StructField("EEP", DoubleType()),
    StructField("INVALIDFLAG", StringType()),
])

# Schema for DISPATCH_UNIT_SCADA
SCHEMA_DISPATCH_UNIT_SCADA = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
    StructField("SETTLEMENTDATE", StringType()),
    StructField("DUID", StringType()),
    StructField("SCADAVALUE", DoubleType()),
])

# Schema for P5MIN_REGIONSOLUTION
SCHEMA_P5MIN_REGIONSOLUTION = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
    StructField("RUN_DATETIME", StringType()),
    StructField("INTERVAL_DATETIME", StringType()),
    StructField("REGIONID", StringType()),
    StructField("RRP", DoubleType()),
    StructField("ROP", DoubleType()),
    StructField("EXCESSGENERATION", DoubleType()),
    StructField("TOTALDEMAND", DoubleType()),
])

# Schema for P5MIN_INTERCONNECTORSOLN
SCHEMA_P5MIN_INTERCONNECTORSOLN = StructType([
    StructField("ROW_TYPE", StringType()),
    StructField("RECORD_TYPE1", StringType()),
    StructField("RECORD_TYPE2", StringType()),
    StructField("VERSION", StringType()),
    StructField("RUN_DATETIME", StringType()),
    StructField("INTERVAL_DATETIME", StringType()),
    StructField("INTERCONNECTORID", StringType()),
    StructField("MWFLOW", DoubleType()),
    StructField("MWLOSSES", DoubleType()),
    StructField("EXPORTLIMIT", DoubleType()),
    StructField("IMPORTLIMIT", DoubleType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Streaming Tables with Auto Loader

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_regionsum",
    comment="Streaming 5-minute regional demand, generation, and interchange data from NEMWEB"
)
def bronze_dispatch_regionsum():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
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
            col("NETINTERCHANGE").alias("net_interchange_mw"),
            col("DISPATCHINTERVAL").alias("dispatch_interval"),
            "_ingested_at"
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_price",
    comment="Streaming 5-minute regional spot prices from NEMWEB"
)
def bronze_dispatch_price():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
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
            "_ingested_at"
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_trading_price",
    comment="Streaming 30-minute trading period prices from NEMWEB"
)
def bronze_trading_price():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/tradingprice")
        .schema(SCHEMA_TRADING_PRICE)
        .load(f"{CSV_PATH}/tradingprice/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("settlement_date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "settlement_date",
            col("REGIONID").alias("region_id"),
            col("PERIODID").alias("period_id"),
            col("RRP").alias("rrp"),
            "_ingested_at"
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_unit_scada",
    comment="Streaming 5-minute real-time generation per power station from NEMWEB"
)
def bronze_dispatch_unit_scada():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
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
            "_ingested_at"
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_p5min_regionsolution",
    comment="Streaming 5-minute pre-dispatch regional forecasts from NEMWEB"
)
def bronze_p5min_regionsolution():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/p5min_regionsolution")
        .schema(SCHEMA_P5MIN_REGIONSOLUTION)
        .load(f"{CSV_PATH}/p5min_regionsolution/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("run_datetime", to_timestamp(col("RUN_DATETIME"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("interval_datetime", to_timestamp(col("INTERVAL_DATETIME"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "run_datetime",
            "interval_datetime",
            col("REGIONID").alias("region_id"),
            col("RRP").alias("rrp"),
            col("TOTALDEMAND").alias("total_demand_mw"),
            "_ingested_at"
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_p5min_interconnectorsoln",
    comment="Streaming 5-minute pre-dispatch interconnector forecasts from NEMWEB"
)
def bronze_p5min_interconnectorsoln():
    """Auto Loader streaming from extracted CSV files."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("cloudFiles.schemaLocation", f"{CSV_PATH}/_schemas/p5min_interconnectorsoln")
        .schema(SCHEMA_P5MIN_INTERCONNECTORSOLN)
        .load(f"{CSV_PATH}/p5min_interconnectorsoln/")
        .filter(col("ROW_TYPE") == "D")
        .withColumn("run_datetime", to_timestamp(col("RUN_DATETIME"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("interval_datetime", to_timestamp(col("INTERVAL_DATETIME"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("_ingested_at", current_timestamp())
        .select(
            "run_datetime",
            "interval_datetime",
            col("INTERCONNECTORID").alias("interconnector_id"),
            col("MWFLOW").alias("mw_flow"),
            col("EXPORTLIMIT").alias("export_limit_mw"),
            col("IMPORTLIMIT").alias("import_limit_mw"),
            "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Streaming Tables with Joins

# COMMAND ----------

@dp.table(
    name="silver_dispatch_with_price",
    comment="Joined dispatch and price data from streaming bronze tables"
)
def silver_dispatch_with_price():
    """Join regional dispatch summary with prices."""
    dispatch = spark.readStream.table("bronze_dispatch_regionsum")
    price = spark.readStream.table("bronze_dispatch_price")

    return (
        dispatch.alias("d")
        .join(
            price.alias("p"),
            (col("d.settlement_date") == col("p.settlement_date")) &
            (col("d.region_id") == col("p.region_id")),
            "inner"
        )
        .select(
            col("d.settlement_date"),
            col("d.region_id"),
            col("d.total_demand_mw"),
            col("d.available_generation_mw"),
            col("d.net_interchange_mw"),
            col("p.rrp"),
            col("p.rop"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

@dp.table(
    name="silver_unit_generation",
    comment="Unit generation streaming table with positive values only"
)
@dp.expect_or_drop("positive_generation", "scada_value_mw >= 0")
def silver_unit_generation():
    """Cleansed unit generation data from streaming bronze."""
    return (
        spark.readStream.table("bronze_dispatch_unit_scada")
        .select(
            "settlement_date",
            "duid",
            "scada_value_mw",
            "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Materialized Views for Aggregations

# COMMAND ----------

@dp.materialized_view(
    name="gold_hourly_generation_by_unit",
    comment="Hourly generation totals per unit"
)
def gold_hourly_generation_by_unit():
    """Aggregate unit generation by hour."""
    from pyspark.sql.functions import sum as _sum, avg, max as _max, date_trunc

    return (
        spark.read.table("silver_unit_generation")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("duid", "hour")
        .agg(
            avg("scada_value_mw").alias("avg_generation_mw"),
            _max("scada_value_mw").alias("max_generation_mw"),
            _sum("scada_value_mw").alias("total_mwh")
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_regional_price_summary",
    comment="Regional price statistics by hour"
)
def gold_regional_price_summary():
    """Aggregate regional prices by hour."""
    from pyspark.sql.functions import min as _min, max as _max, avg, date_trunc

    return (
        spark.read.table("silver_dispatch_with_price")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("region_id", "hour")
        .agg(
            avg("rrp").alias("avg_rrp"),
            _min("rrp").alias("min_rrp"),
            _max("rrp").alias("max_rrp"),
            avg("total_demand_mw").alias("avg_demand_mw")
        )
    )
