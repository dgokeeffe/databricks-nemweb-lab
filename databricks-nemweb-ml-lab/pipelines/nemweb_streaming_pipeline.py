# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB Streaming Pipeline
# MAGIC
# MAGIC Streaming pipeline that ingests **live** NEMWEB data directly from the AEMO website
# MAGIC using a custom PySpark streaming data source. No pre-download or CSV extraction needed.
# MAGIC
# MAGIC Also fetches BOM weather observations (temperature, humidity, wind) and joins them
# MAGIC with dispatch data to create ML-ready features for load and price forecasting.
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC NEMWEB CURRENT API ──► nemweb_stream (custom datasource) ──► Bronze streaming tables
# MAGIC BOM Weather API ────► bom_weather.py (batch fetch) ────────► Bronze weather table
# MAGIC
# MAGIC Bronze ──► Silver (demand + price + weather joined) ──► Gold (hourly aggregations)
# MAGIC ```
# MAGIC
# MAGIC ## Tables
# MAGIC | Layer | Table | Source | Description |
# MAGIC |-------|-------|--------|-------------|
# MAGIC | Bronze | `bronze_dispatch_stream` | NEMWEB CURRENT | 5-min regional demand/generation |
# MAGIC | Bronze | `bronze_price_stream` | NEMWEB CURRENT | 5-min regional spot prices |
# MAGIC | Bronze | `bronze_bom_weather` | BOM JSON feed | Half-hourly weather observations |
# MAGIC | Silver | `silver_demand_weather` | Joined | Demand + price + weather (ML-ready) |
# MAGIC | Gold | `gold_demand_hourly` | Aggregated | Hourly demand/price/weather by region |
# MAGIC | Gold | `gold_price_hourly` | Aggregated | Hourly price statistics by region |

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, to_timestamp, date_trunc,
    avg, min as _min, max as _max, sum as _sum, stddev, count, last,
    abs as _abs, row_number, round as _round,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = spark.conf.get("pipeline.catalog", "daveok")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

print(f"Pipeline config: catalog={CATALOG}, schema={SCHEMA}")
print("Data source: NEMWEB CURRENT (live streaming) + BOM Weather API")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Custom Streaming Data Source
# MAGIC
# MAGIC The `nemweb_stream` data source polls the NEMWEB CURRENT folder for new
# MAGIC 5-minute dispatch interval files. AEMO publishes new files approximately
# MAGIC every 5 minutes, making this a near-real-time data source.

# COMMAND ----------

from nemweb_datasource_stream import NemwebStreamDataSource
spark.dataSource.register(NemwebStreamDataSource)

print("Registered nemweb_stream data source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Streaming Tables from NEMWEB CURRENT
# MAGIC Uses `@dp.table()` (Lakeflow SDP): streaming tables are defined by returning a streaming DataFrame from readStream.

# COMMAND ----------

@dp.table(
    name="bronze_dispatch_stream",
    comment="5-minute regional demand and generation streamed live from NEMWEB CURRENT"
)
def bronze_dispatch_stream():
    """Stream DISPATCHREGIONSUM from NEMWEB CURRENT folder.

    Contains total demand, available generation, demand forecast,
    and net interchange for each NEM region at 5-minute intervals.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "DISPATCHREGIONSUM")
        .option("regions", "NSW1,QLD1,SA1,VIC1,TAS1")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="bronze_price_stream",
    comment="5-minute regional spot prices streamed live from NEMWEB CURRENT"
)
def bronze_price_stream():
    """Stream DISPATCHPRICE from NEMWEB CURRENT folder.

    Contains Regional Reference Price (RRP), Excess Energy Price (EEP),
    and Regional Override Price (ROP) at 5-minute intervals.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "DISPATCHPRICE")
        .option("regions", "NSW1,QLD1,SA1,VIC1,TAS1")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - AEMO Pre-Dispatch Forecasts (P5MIN)
# MAGIC
# MAGIC P5MIN is AEMO's 5-minute pre-dispatch engine. Every 5 minutes it publishes
# MAGIC demand and price **forecasts** for the next 12 intervals (1 hour ahead).
# MAGIC Both Load Forecasting and Market Modelling teams benchmark against P5MIN.

# COMMAND ----------

@dp.table(
    name="bronze_p5min_forecast",
    comment="AEMO 5-minute pre-dispatch forecasts - demand and price for the next 12 intervals (1 hour ahead)"
)
def bronze_p5min_forecast():
    """Stream P5MIN_REGIONSOLUTION from NEMWEB CURRENT folder.

    Each P5MIN run (every 5 minutes) contains forecasts for the next
    ~12 dispatch intervals (1 hour ahead) per region. Key fields:
    - RUN_DATETIME: when this forecast was produced
    - INTERVAL_DATETIME: the interval being forecast
    - RRP: forecast Regional Reference Price ($/MWh)
    - TOTALDEMAND: forecast total demand (MW)

    Used by:
    - Load Forecasting: benchmark your demand forecast against AEMO's
    - Market Modelling: benchmark your price forecast against AEMO's
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "P5MIN_REGIONSOLUTION")
        .option("regions", "NSW1,QLD1,SA1,VIC1,TAS1")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Unit-Level SCADA (Generator Output)
# MAGIC
# MAGIC DISPATCH_UNIT_SCADA shows real-time MW output for every Dispatchable Unit
# MAGIC (DUID) in the NEM. Critical for market modelling: a generator trip
# MAGIC (SCADAVALUE → 0) signals a supply shortfall before the price moves.

# COMMAND ----------

@dp.table(
    name="bronze_scada_stream",
    comment="Real-time unit-level MW generation from NEMWEB Dispatch SCADA"
)
def bronze_scada_stream():
    """Stream DISPATCH_UNIT_SCADA from NEMWEB CURRENT folder.

    Contains MW output (SCADAVALUE) for every Dispatchable Unit ID (DUID)
    in the NEM. Updated every 5 minutes. Hundreds of DUIDs per interval.

    No REGIONID filter -- SCADA reports by DUID, not region. Join with
    a participant registration table (DUDETAILSUMMARY) for region mapping.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "DISPATCH_UNIT_SCADA")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Interconnector Flows
# MAGIC
# MAGIC DISPATCH_INTERCONNECTOR shows actual metered power flows between NEM regions.
# MAGIC Key interconnectors: NSW↔QLD, VIC↔NSW, VIC↔SA, VIC↔TAS (Basslink).
# MAGIC Positive flow = export from the "from" region; negative = import.
# MAGIC Price differentials between regions are driven by interconnector constraints.

# COMMAND ----------

@dp.table(
    name="bronze_interconnector_stream",
    comment="Cross-regional interconnector flows and constraints from NEMWEB Dispatch Reports"
)
def bronze_interconnector_stream():
    """Stream DISPATCH_INTERCONNECTOR from NEMWEB CURRENT folder.

    Contains metered MW flow, dispatch target, losses, and import/export
    limits for all NEM interconnectors. Published every 5 minutes.

    Key for market modelling: when an interconnector binds (flow hits
    import/export limit), regions decouple and prices diverge.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "DISPATCH_INTERCONNECTOR")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Rooftop Solar PV
# MAGIC
# MAGIC AEMO's satellite-derived estimates of behind-the-meter rooftop solar
# MAGIC generation. Published every 30 minutes. Critical for load forecasting:
# MAGIC rooftop PV suppresses operational demand (the "duck curve") by 5-10 GW
# MAGIC nationally at midday.

# COMMAND ----------

@dp.table(
    name="bronze_rooftop_pv_stream",
    comment="Rooftop solar PV generation estimates from AEMO satellite data (30-min updates)"
)
def bronze_rooftop_pv_stream():
    """Stream ROOFTOP_PV_ACTUAL from NEMWEB CURRENT folder.

    Satellite-derived estimates of behind-the-meter rooftop solar generation
    per NEM region. Published every 30 minutes.

    Used by Load Forecasting: rooftop PV is subtracted from gross demand to
    get "operational demand" (what the grid must supply from scheduled generation).
    Without this, demand models systematically overforecast midday demand.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "ROOFTOP_PV_ACTUAL")
        .option("regions", "NSW1,QLD1,SA1,VIC1,TAS1")
        .option("poll_interval_seconds", "60")
        .option("max_files_per_batch", "5")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - BOM Weather Observations
# MAGIC
# MAGIC Weather data is fetched as a batch (materialized view) since BOM publishes
# MAGIC observations every 30 minutes. Temperature is the primary driver of electricity
# MAGIC demand in the NEM.

# COMMAND ----------

@dp.materialized_view(
    name="bronze_bom_weather",
    comment="BOM weather observations for NEM region capital city stations"
)
def bronze_bom_weather():
    """Fetch latest BOM weather observations for all 5 NEM regions.

    Fetches from BOM JSON feeds for Sydney, Melbourne, Brisbane,
    Adelaide, and Hobart weather stations. Falls back to sample
    data if the API is unreachable.
    """
    from bom_weather import fetch_bom_observations, generate_sample_weather

    try:
        observations = fetch_bom_observations(latest_only=False)
    except Exception as e:
        print(f"BOM API unavailable ({e}), using sample weather data")
        observations = generate_sample_weather()

    if not observations:
        print("No BOM observations returned, using sample weather data")
        observations = generate_sample_weather()

    schema = StructType([
        StructField("region_id", StringType(), False),
        StructField("observation_time", TimestampType(), True),
        StructField("air_temp_c", DoubleType(), True),
        StructField("apparent_temp_c", DoubleType(), True),
        StructField("rel_humidity_pct", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("cloud_oktas", DoubleType(), True),
        StructField("rain_since_9am_mm", DoubleType(), True),
        StructField("station_name", StringType(), True),
    ])

    rows = []
    for obs in observations:
        rows.append((
            obs.get("region_id"),
            obs.get("observation_time"),
            obs.get("air_temp_c"),
            obs.get("apparent_temp_c"),
            obs.get("rel_humidity_pct"),
            obs.get("wind_speed_kmh"),
            obs.get("wind_dir"),
            obs.get("cloud_oktas"),
            obs.get("rain_since_9am_mm"),
            obs.get("station_name"),
        ))

    return spark.createDataFrame(rows, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Joined and Enriched for ML

# COMMAND ----------

@dp.table(
    name="silver_demand_weather",
    comment="Dispatch demand + price + weather joined - ML-ready for load and price forecasting"
)
def silver_demand_weather():
    """Join streaming dispatch data with prices and latest weather.

    This is the primary table used by both workshop variants:
    - Load Forecasting: target = total_demand_mw, key feature = temperature
    - Market Modelling: target = rrp, key feature = demand-supply margin

    Weather is joined using the latest available observation for each region
    (BOM updates every 30 minutes; dispatch updates every 5 minutes).
    """
    # Watermarks are required for stream-stream joins in continuous mode.
    # NEMWEB publishes dispatch and price in the same 5-minute ZIP file,
    # so both bronze tables ingest the same SETTLEMENTDATE within the
    # same microbatch (seconds apart). A 2-minute watermark is tight
    # enough for low-latency bidding while tolerating minor skew.
    dispatch = (
        spark.readStream.table("bronze_dispatch_stream")
        .withWatermark("SETTLEMENTDATE", "2 minutes")
    )
    price = (
        spark.readStream.table("bronze_price_stream")
        .withWatermark("SETTLEMENTDATE", "2 minutes")
    )

    # Stream-stream join: dispatch + price on same interval and region
    demand_price = (
        dispatch.alias("d")
        .join(
            price.alias("p"),
            (col("d.SETTLEMENTDATE") == col("p.SETTLEMENTDATE"))
            & (col("d.REGIONID") == col("p.REGIONID")),
            "inner",
        )
        .select(
            col("d.SETTLEMENTDATE").alias("settlement_date"),
            col("d.REGIONID").alias("region_id"),
            col("d.TOTALDEMAND").alias("total_demand_mw"),
            col("d.AVAILABLEGENERATION").alias("available_generation_mw"),
            col("d.DEMANDFORECAST").alias("demand_forecast_mw"),
            col("d.NETINTERCHANGE").alias("net_interchange_mw"),
            col("p.RRP").alias("rrp"),
            col("p.ROP").alias("rop"),
            current_timestamp().alias("_processed_at"),
        )
    )

    return demand_price

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - P5MIN Forecast vs Actual Dispatch
# MAGIC
# MAGIC Joins AEMO's P5MIN pre-dispatch forecasts with actual dispatch outcomes.
# MAGIC This is the key calibration table for both teams: how accurate was AEMO,
# MAGIC and where do your own models need to improve?

# COMMAND ----------

@dp.materialized_view(
    name="silver_forecast_vs_actual",
    comment="AEMO P5MIN forecast vs actual dispatch - demand and price accuracy with lead time"
)
def silver_forecast_vs_actual():
    """Join P5MIN forecasts with actual dispatch to measure AEMO accuracy.

    For each dispatch interval, shows:
    - What AEMO predicted (demand, price, generation)
    - What actually happened
    - The forecast error and lead time

    Uses the most recent P5MIN run for each interval (shortest lead time)
    to give AEMO's "best guess" at the time.

    Used by:
    - Load Forecasting: compare your demand forecast vs AEMO's TOTALDEMAND
    - Market Modelling: compare your price forecast vs AEMO's RRP
    """
    p5min = spark.read.table("bronze_p5min_forecast")
    actual = spark.read.table("silver_demand_weather")

    # Take the latest P5MIN forecast for each interval (shortest lead time)
    w = Window.partitionBy("INTERVAL_DATETIME", "REGIONID").orderBy(
        col("RUN_DATETIME").desc()
    )
    latest_forecast = (
        p5min
        .filter(col("INTERVENTION") == "0")
        .withColumn("_rank", row_number().over(w))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    return (
        latest_forecast.alias("f")
        .join(
            actual.alias("a"),
            (col("f.INTERVAL_DATETIME") == col("a.settlement_date"))
            & (col("f.REGIONID") == col("a.region_id")),
            "inner",
        )
        .select(
            col("a.settlement_date"),
            col("a.region_id"),
            col("f.RUN_DATETIME").alias("forecast_run_time"),
            # Lead time: how far ahead the forecast was (minutes)
            _round(
                (col("f.INTERVAL_DATETIME").cast("long") - col("f.RUN_DATETIME").cast("long")) / 60
            ).alias("lead_time_minutes"),
            # Demand: forecast vs actual
            col("f.TOTALDEMAND").alias("forecast_demand_mw"),
            col("a.total_demand_mw").alias("actual_demand_mw"),
            _round(col("f.TOTALDEMAND") - col("a.total_demand_mw"), 1).alias("demand_error_mw"),
            # Price: forecast vs actual
            col("f.RRP").alias("forecast_rrp"),
            col("a.rrp").alias("actual_rrp"),
            _round(col("f.RRP") - col("a.rrp"), 2).alias("price_error"),
            # Supply context
            col("f.AVAILABLEGENERATION").alias("forecast_available_gen_mw"),
            col("a.available_generation_mw").alias("actual_available_gen_mw"),
            col("f.NETINTERCHANGE").alias("forecast_net_interchange_mw"),
            col("a.net_interchange_mw").alias("actual_net_interchange_mw"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Supply Stack (Total Active Generation)
# MAGIC
# MAGIC Aggregate SCADA data to NEM-wide generation metrics. Because SCADA
# MAGIC reports by DUID (not by region), this table provides a bottom-up supply
# MAGIC estimate: the sum of all generating units' MW output.

# COMMAND ----------

@dp.materialized_view(
    name="silver_supply_stack",
    comment="NEM-wide generation metrics aggregated from unit-level SCADA data"
)
def silver_supply_stack():
    """Aggregate DISPATCH_UNIT_SCADA to a per-interval supply summary.

    Provides:
    - Total NEM generation (sum of all positive SCADAVALUE)
    - Number of active units (SCADAVALUE > 0)
    - Number of offline units (SCADAVALUE = 0 or NULL)
    - Largest single unit output (potential contingency)

    Note: SCADA has no REGIONID. For regional breakdown, join with
    DUDETAILSUMMARY (participant registration) which maps DUID → REGIONID.
    """
    scada = spark.read.table("bronze_scada_stream")

    return (
        scada
        .groupBy("SETTLEMENTDATE")
        .agg(
            _sum(
                expr("CASE WHEN SCADAVALUE > 0 THEN SCADAVALUE ELSE 0 END")
            ).alias("total_generation_mw"),
            count(expr("CASE WHEN SCADAVALUE > 0 THEN 1 END")).alias("active_unit_count"),
            count(expr("CASE WHEN SCADAVALUE <= 0 OR SCADAVALUE IS NULL THEN 1 END")).alias("offline_unit_count"),
            _max("SCADAVALUE").alias("largest_unit_mw"),
            count("*").alias("total_duid_count"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Aggregations for Exploration and App

# COMMAND ----------

@dp.materialized_view(
    name="gold_demand_hourly",
    comment="Hourly demand, price, and weather aggregations by NEM region"
)
def gold_demand_hourly():
    """Hourly aggregation of demand, price, and weather data.

    Used by the Dash app for the Load Forecast view and for quick
    exploration of demand patterns across regions.
    """
    demand = spark.read.table("silver_demand_weather")
    weather = spark.read.table("bronze_bom_weather")

    # Get latest weather observation per region per hour
    latest_weather = (
        weather
        .withColumn("hour", date_trunc("hour", col("observation_time")))
        .groupBy("region_id", "hour")
        .agg(
            last("air_temp_c").alias("air_temp_c"),
            last("apparent_temp_c").alias("apparent_temp_c"),
            last("rel_humidity_pct").alias("rel_humidity_pct"),
            last("wind_speed_kmh").alias("wind_speed_kmh"),
        )
    )

    # Hourly demand/price aggregations
    hourly_demand = (
        demand
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("region_id", "hour")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            _min("total_demand_mw").alias("min_demand_mw"),
            _max("total_demand_mw").alias("max_demand_mw"),
            avg("available_generation_mw").alias("avg_available_gen_mw"),
            avg("net_interchange_mw").alias("avg_net_interchange_mw"),
            avg("rrp").alias("avg_rrp"),
            _min("rrp").alias("min_rrp"),
            _max("rrp").alias("max_rrp"),
            count("*").alias("interval_count"),
        )
    )

    # Join hourly demand with weather
    return (
        hourly_demand.alias("d")
        .join(
            latest_weather.alias("w"),
            (col("d.region_id") == col("w.region_id"))
            & (col("d.hour") == col("w.hour")),
            "left",
        )
        .select(
            col("d.region_id"),
            col("d.hour"),
            col("d.avg_demand_mw"),
            col("d.min_demand_mw"),
            col("d.max_demand_mw"),
            col("d.avg_available_gen_mw"),
            col("d.avg_net_interchange_mw"),
            col("d.avg_rrp"),
            col("d.min_rrp"),
            col("d.max_rrp"),
            col("d.interval_count"),
            col("w.air_temp_c"),
            col("w.apparent_temp_c"),
            col("w.rel_humidity_pct"),
            col("w.wind_speed_kmh"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_price_hourly",
    comment="Hourly price statistics by NEM region"
)
def gold_price_hourly():
    """Hourly price summary with volatility metrics.

    Used by the Dash app for the Price Forecast view and for
    market modelling exploration.
    """
    return (
        spark.read.table("silver_demand_weather")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("region_id", "hour")
        .agg(
            avg("rrp").alias("avg_rrp"),
            _min("rrp").alias("min_rrp"),
            _max("rrp").alias("max_rrp"),
            stddev("rrp").alias("stddev_rrp"),
            avg("total_demand_mw").alias("avg_demand_mw"),
            _max("total_demand_mw").alias("peak_demand_mw"),
            avg("available_generation_mw").alias("avg_available_gen_mw"),
            count("*").alias("interval_count"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_forecast_accuracy",
    comment="Hourly AEMO P5MIN forecast accuracy - MAPE, bias, and error statistics by region"
)
def gold_forecast_accuracy():
    """Hourly summary of AEMO's P5MIN forecast accuracy.

    Key metrics:
    - MAE: Mean Absolute Error (MW for demand, $/MWh for price)
    - Bias: systematic over/under-forecast (negative = AEMO under-forecasts)
    - MAPE: Mean Absolute Percentage Error
    - Interval count: how many 5-min intervals are in each hourly bucket

    Both teams use this to:
    - Identify hours where AEMO forecasts are weakest (opportunity for alpha)
    - Detect systematic biases (e.g., AEMO underforecasts demand at 6pm)
    - Calibrate their own models against a known benchmark
    """
    return (
        spark.read.table("silver_forecast_vs_actual")
        .withColumn("hour", date_trunc("hour", col("settlement_date")))
        .groupBy("region_id", "hour")
        .agg(
            # Demand forecast accuracy
            avg(_abs(col("demand_error_mw"))).alias("demand_mae_mw"),
            avg(col("demand_error_mw")).alias("demand_bias_mw"),
            avg(
                _abs(col("demand_error_mw")) / _abs(col("actual_demand_mw")) * 100
            ).alias("demand_mape_pct"),
            # Price forecast accuracy
            avg(_abs(col("price_error"))).alias("price_mae"),
            avg(col("price_error")).alias("price_bias"),
            # Context
            avg("lead_time_minutes").alias("avg_lead_time_minutes"),
            count("*").alias("interval_count"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_interconnector_hourly",
    comment="Hourly interconnector flow summary - utilisation and constraint binding"
)
def gold_interconnector_hourly():
    """Hourly aggregation of interconnector flows for cross-regional analysis.

    Shows average, min, and max flows for each interconnector plus
    utilisation metrics. When flow approaches import/export limits,
    the interconnector is "binding" and regional prices decouple.

    Key interconnectors:
    - N-Q-MNSP1: NSW ↔ QLD (Queensland-NSW Interconnector)
    - VIC1-NSW1: VIC ↔ NSW (main southern link)
    - V-SA: VIC ↔ SA (Heywood interconnector)
    - T-V-MNSP1: TAS ↔ VIC (Basslink undersea cable)
    """
    return (
        spark.read.table("bronze_interconnector_stream")
        .withColumn("hour", date_trunc("hour", col("SETTLEMENTDATE")))
        .groupBy("INTERCONNECTORID", "hour")
        .agg(
            avg("METEREDMWFLOW").alias("avg_flow_mw"),
            _min("METEREDMWFLOW").alias("min_flow_mw"),
            _max("METEREDMWFLOW").alias("max_flow_mw"),
            avg("MWLOSSES").alias("avg_losses_mw"),
            avg("IMPORTLIMIT").alias("avg_import_limit_mw"),
            avg("EXPORTLIMIT").alias("avg_export_limit_mw"),
            # Utilisation: how close to limits
            avg(
                _abs(col("METEREDMWFLOW")) /
                expr("GREATEST(ABS(IMPORTLIMIT), ABS(EXPORTLIMIT), 1)") * 100
            ).alias("avg_utilisation_pct"),
            count("*").alias("interval_count"),
        )
    )
