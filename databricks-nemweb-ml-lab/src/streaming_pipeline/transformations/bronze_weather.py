import json

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_trunc,
    explode,
    from_json,
    minute,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    DoubleType,
    StringType,
    TimestampType,
)


@dp.table(
    name="bronze_bom_weather",
    comment="BOM weather observations for NEM regions (streaming ingest every 30 minutes)"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_observation_time", "observation_time IS NOT NULL")
@dp.expect_or_drop("air_temp_in_range", "air_temp_c IS NULL OR (air_temp_c BETWEEN -20 AND 60)")
@dp.expect_or_drop(
    "humidity_in_range",
    "rel_humidity_pct IS NULL OR (rel_humidity_pct BETWEEN 0 AND 100)",
)
def bronze_bom_weather():
    """Ingest BOM weather snapshots on a deterministic 30-minute cadence.

    The cadence is driven by DISPATCHREGIONSUM stream timestamps:
    only dispatch intervals at minute 00 and 30 trigger a BOM pull.
    This guarantees weather refreshes while the streaming pipeline runs,
    instead of relying on isolated MV re-evaluation behavior.
    """
    from bom_weather import fetch_bom_observations, generate_sample_weather

    observation_schema = StructType([
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

    @udf(returnType=StringType())
    def fetch_weather_json(_slot_ts):
        try:
            observations = fetch_bom_observations(latest_only=True)
        except Exception as e:
            print(f"BOM API unavailable ({e}), using sample weather data")
            observations = generate_sample_weather()

        if not observations:
            print("No BOM observations returned, using sample weather data")
            observations = generate_sample_weather()

        return json.dumps(observations, default=str)

    # One trigger row per half-hour, deduplicated across regions.
    weather_ticks = (
        spark.readStream.table("bronze_dispatch_stream")
        .select(col("SETTLEMENTDATE").cast("timestamp").alias("settlement_ts"))
        .withWatermark("settlement_ts", "2 hours")
        .where((minute(col("settlement_ts")) % 30) == 0)
        .select(date_trunc("minute", col("settlement_ts")).alias("refresh_slot"))
        .dropDuplicates(["refresh_slot"])
    )

    return (
        weather_ticks
        .withColumn(
            "observations",
            from_json(
                fetch_weather_json(col("refresh_slot")),
                ArrayType(observation_schema),
            ),
        )
        .select(
            col("refresh_slot"),
            explode(col("observations")).alias("obs"),
            current_timestamp().alias("_ingested_at"),
        )
        .select(
            col("obs.region_id").alias("region_id"),
            col("obs.observation_time").alias("observation_time"),
            col("obs.air_temp_c").alias("air_temp_c"),
            col("obs.apparent_temp_c").alias("apparent_temp_c"),
            col("obs.rel_humidity_pct").alias("rel_humidity_pct"),
            col("obs.wind_speed_kmh").alias("wind_speed_kmh"),
            col("obs.wind_dir").alias("wind_dir"),
            col("obs.cloud_oktas").alias("cloud_oktas"),
            col("obs.rain_since_9am_mm").alias("rain_since_9am_mm"),
            col("obs.station_name").alias("station_name"),
            col("refresh_slot"),
            col("_ingested_at"),
        )
    )
