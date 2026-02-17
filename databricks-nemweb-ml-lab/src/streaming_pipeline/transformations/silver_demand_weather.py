from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window


@dp.table(
    name="silver_demand_weather",
    comment="Dispatch demand + price + weather joined - ML-ready for load and price forecasting"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_settlement_date", "settlement_date IS NOT NULL")
@dp.expect_or_drop("reasonable_demand", "total_demand_mw >= 0 AND total_demand_mw < 25000")
@dp.expect_or_drop(
    "reasonable_available_generation",
    "available_generation_mw >= 0 AND available_generation_mw < 80000",
)
@dp.expect_or_drop("reasonable_rrp", "rrp BETWEEN -1500 AND 25000")
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
    weather = spark.read.table("bronze_bom_weather")

    # Keep one latest weather row per region so stream-static join stays stable.
    latest_weather = (
        weather
        .withColumn(
            "_weather_rank",
            row_number().over(
                Window.partitionBy("region_id").orderBy(col("observation_time").desc())
            ),
        )
        .filter(col("_weather_rank") == 1)
        .drop("_weather_rank")
    )

    # Stream-stream join: dispatch + price on same interval and region
    demand_price_weather = (
        dispatch.alias("d")
        .join(
            price.alias("p"),
            (col("d.SETTLEMENTDATE") == col("p.SETTLEMENTDATE"))
            & (col("d.REGIONID") == col("p.REGIONID")),
            "inner",
        )
        .join(
            latest_weather.alias("w"),
            col("d.REGIONID") == col("w.region_id"),
            "left",
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
            col("w.observation_time").alias("weather_observation_time"),
            col("w.air_temp_c").alias("air_temp_c"),
            col("w.apparent_temp_c").alias("apparent_temp_c"),
            col("w.rel_humidity_pct").alias("rel_humidity_pct"),
            col("w.wind_speed_kmh").alias("wind_speed_kmh"),
            col("w.wind_dir").alias("wind_dir"),
            col("w.cloud_oktas").alias("cloud_oktas"),
            col("w.rain_since_9am_mm").alias("rain_since_9am_mm"),
            col("w.station_name").alias("station_name"),
            current_timestamp().alias("_processed_at"),
        )
    )

    return demand_price_weather
