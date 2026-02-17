# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB MMS Full Ingest Pipeline
# MAGIC
# MAGIC This pipeline ingests all supported NEMWEB MMS tables as bronze tables using
# MAGIC the custom PySpark datasource. Each table is loaded directly from NEMWEB's
# MAGIC public API.
# MAGIC
# MAGIC ## Bronze Tables Created
# MAGIC - `bronze_dispatch_regionsum`: Regional demand/generation (5-min)
# MAGIC - `bronze_dispatch_price`: Regional spot prices (5-min)
# MAGIC - `bronze_trading_price`: Trading period prices (30-min)
# MAGIC - `bronze_dispatch_unit_scada`: Real-time unit generation (5-min)
# MAGIC - `bronze_p5min_regionsolution`: Pre-dispatch forecasts (5-min)
# MAGIC - `bronze_p5min_interconnectorsoln`: Interconnector forecasts (5-min)
# MAGIC - `bronze_bidperoffer`: Generator bid availability (daily)
# MAGIC - `bronze_biddayoffer`: Generator price bands (daily)
# MAGIC
# MAGIC ## Silver/Gold Layers
# MAGIC - `silver_dispatch_with_price`: Joined dispatch + price
# MAGIC - `silver_unit_generation`: Cleansed unit generation
# MAGIC - `gold_hourly_generation_by_unit`: Hourly generation aggregations
# MAGIC
# MAGIC Reference: https://nemweb.com.au/Reports/Current/MMSDataModelReport/

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit

# Register the custom datasource
from nemweb_datasource_arrow import NemwebArrowDataSource
spark.dataSource.register(NemwebArrowDataSource)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get catalog and schema from pipeline configuration
# These are set by the bundle's pipeline definition in databricks.yml
CATALOG = spark.conf.get("pipeline.catalog", "workspace")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

# Volume path for downloaded files (dynamically constructed from catalog/schema)
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
print(f"Using volume path: {VOLUME_PATH}")

def get_datasource_options(table: str, include_current: bool = True) -> dict:
    """Get standard options for NEMWEB datasource."""
    return {
        "table": table,
        "include_current": str(include_current).lower(),
        "volume_path": VOLUME_PATH,
        "auto_download": "true",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Dispatch IS Reports (5-minute data)

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_regionsum",
    comment="Raw 5-minute regional demand, generation, and interchange data from NEMWEB"
)
def bronze_dispatch_regionsum():
    """
    DISPATCHREGIONSUM - Regional summary for each 5-minute dispatch interval.

    Key fields:
      - SETTLEMENTDATE: Dispatch interval timestamp
      - REGIONID: NEM region (NSW1, QLD1, SA1, VIC1, TAS1)
      - TOTALDEMAND: Regional demand (MW)
      - AVAILABLEGENERATION: Available generation capacity (MW)
      - NETINTERCHANGE: Net flow into region (MW, negative = exporting)
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("DISPATCHREGIONSUM"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("DISPATCHREGIONSUM"))
    )

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_price",
    comment="Raw 5-minute regional spot prices from NEMWEB"
)
def bronze_dispatch_price():
    """
    DISPATCHPRICE - Regional Reference Price (RRP) for each 5-minute interval.

    Key fields:
      - SETTLEMENTDATE: Dispatch interval timestamp
      - REGIONID: NEM region
      - RRP: Regional Reference Price ($/MWh)
      - ROP: Regional Override Price
      - APCFLAG: Administered Price Cap flag
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("DISPATCHPRICE"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("DISPATCHPRICE"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Trading IS Reports (30-minute data)

# COMMAND ----------

@dp.table(
    name="bronze_trading_price",
    comment="Raw 30-minute trading period prices from NEMWEB"
)
def bronze_trading_price():
    """
    TRADINGPRICE - Trading period prices (settlement prices).

    Key fields:
      - SETTLEMENTDATE: Trading period timestamp
      - REGIONID: NEM region
      - PERIODID: Trading period (1-48 per day)
      - RRP: Regional Reference Price for settlement
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("TRADINGPRICE"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("TRADINGPRICE"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Dispatch SCADA (Real-time unit data)

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_unit_scada",
    comment="Raw 5-minute real-time generation per power station from NEMWEB"
)
def bronze_dispatch_unit_scada():
    """
    DISPATCH_UNIT_SCADA - Real-time SCADA output for each generating unit.

    Key fields:
      - SETTLEMENTDATE: Dispatch interval timestamp
      - DUID: Dispatchable Unit ID (power station identifier)
      - SCADAVALUE: Real-time MW output from SCADA
      - LASTCHANGED: When SCADA value was last updated
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("DISPATCH_UNIT_SCADA"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("DISPATCH_UNIT_SCADA"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - P5MIN Reports (5-minute pre-dispatch forecasts)

# COMMAND ----------

@dp.table(
    name="bronze_p5min_regionsolution",
    comment="Raw 5-minute pre-dispatch regional forecasts from NEMWEB"
)
def bronze_p5min_regionsolution():
    """
    P5MIN_REGIONSOLUTION - 5-minute pre-dispatch regional solution.

    Contains forecasts for the next hour at 5-minute granularity.

    Key fields:
      - RUN_DATETIME: When the forecast was generated
      - INTERVAL_DATETIME: Forecast target time
      - REGIONID: NEM region
      - RRP: Forecast Regional Reference Price ($/MWh)
      - TOTALDEMAND: Forecast demand (MW)
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("P5MIN_REGIONSOLUTION"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("P5MIN_REGIONSOLUTION"))
    )

# COMMAND ----------

@dp.table(
    name="bronze_p5min_interconnectorsoln",
    comment="Raw 5-minute pre-dispatch interconnector forecasts from NEMWEB"
)
def bronze_p5min_interconnectorsoln():
    """
    P5MIN_INTERCONNECTORSOLN - 5-minute pre-dispatch interconnector solution.

    Key fields:
      - RUN_DATETIME: When the forecast was generated
      - INTERVAL_DATETIME: Forecast target time
      - INTERCONNECTORID: Interconnector identifier (e.g., NSW1-QLD1)
      - MWFLOW: Forecast MW flow
      - EXPORTLIMIT: Export limit (MW)
      - IMPORTLIMIT: Import limit (MW)
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("P5MIN_INTERCONNECTORSOLN"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("P5MIN_INTERCONNECTORSOLN"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Bidmove Complete (Generator bids)

# COMMAND ----------

@dp.table(
    name="bronze_bidperoffer",
    comment="Raw generator bid availability per trading period from NEMWEB"
)
def bronze_bidperoffer():
    """
    BIDPEROFFER_D - Generator bid availability per trading period.

    Contains the MW availability in each of 10 price bands for each
    trading period (48 per day).

    Key fields:
      - DUID: Dispatchable Unit ID
      - BIDTYPE: Bid type (ENERGY, RAISE6SEC, etc.)
      - PERIODID: Trading period (1-48)
      - MAXAVAIL: Maximum availability (MW)
      - BANDAVAIL1-10: Availability in each price band (MW)
      - ROCUP/ROCDOWN: Ramp rates (MW/min)
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("BIDPEROFFER_D"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("BIDPEROFFER_D"))
    )

# COMMAND ----------

@dp.table(
    name="bronze_biddayoffer",
    comment="Raw generator daily price bands from NEMWEB"
)
def bronze_biddayoffer():
    """
    BIDDAYOFFER_D - Generator daily offer with price bands.

    Contains the $/MWh price for each of 10 bands, valid for the entire
    trading day.

    Key fields:
      - DUID: Dispatchable Unit ID
      - BIDTYPE: Bid type (ENERGY, RAISE6SEC, etc.)
      - PARTICIPANTID: Registered participant
      - PRICEBAND1-10: Price for each band ($/MWh)
      - REBIDEXPLANATION: Reason for rebid (if applicable)
    """
    return (
        spark.read.format("nemweb_arrow")
        .options(**get_datasource_options("BIDDAYOFFER_D"))
        .load()
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit("BIDDAYOFFER_D"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Joined and Cleansed Data

# COMMAND ----------

@dp.table(
    name="silver_dispatch_with_price",
    comment="Joined dispatch and price data"
)
def silver_dispatch_with_price():
    """
    Join regional dispatch summary with prices for analysis.
    """
    dispatch = spark.read.table("bronze_dispatch_regionsum")
    price = spark.read.table("bronze_dispatch_price")

    return (
        dispatch.alias("d")
        .join(
            price.alias("p"),
            (col("d.SETTLEMENTDATE") == col("p.SETTLEMENTDATE")) &
            (col("d.REGIONID") == col("p.REGIONID")),
            "inner"
        )
        .select(
            col("d.SETTLEMENTDATE").alias("settlement_date"),
            col("d.REGIONID").alias("region_id"),
            col("d.TOTALDEMAND").alias("total_demand_mw"),
            col("d.AVAILABLEGENERATION").alias("available_generation_mw"),
            col("d.NETINTERCHANGE").alias("net_interchange_mw"),
            col("p.RRP").alias("rrp"),
            col("p.ROP").alias("rop"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

@dp.table(
    name="silver_unit_generation",
    comment="Unit generation with positive values only"
)
@dp.expect_or_drop("positive_generation", "scada_value_mw >= 0")
def silver_unit_generation():
    """
    Cleansed unit generation data.
    """
    return (
        spark.read.table("bronze_dispatch_unit_scada")
        .select(
            col("SETTLEMENTDATE").alias("settlement_date"),
            col("DUID").alias("duid"),
            col("SCADAVALUE").alias("scada_value_mw"),
            col("_ingested_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Aggregations

# COMMAND ----------

@dp.materialized_view(
    name="gold_hourly_generation_by_unit",
    comment="Hourly generation totals per unit"
)
def gold_hourly_generation_by_unit():
    """
    Aggregate unit generation by hour.
    """
    from pyspark.sql.functions import sum as _sum, avg, max as _max, date_trunc

    return (
        spark.read.table("silver_unit_generation")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("duid", "hour")
        .agg(
            avg("scada_value_mw").alias("avg_generation_mw"),
            _max("scada_value_mw").alias("max_generation_mw"),
            _sum("scada_value_mw").alias("total_mwh")  # Approx MWh (5-min intervals)
        )
    )
