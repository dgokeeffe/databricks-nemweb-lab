from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, date_trunc, avg, min as _min, max as _max, count, last,
)


@dp.materialized_view(
    name="gold_demand_hourly",
    comment="Hourly demand, price, and weather aggregations by NEM region"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_hour", "hour IS NOT NULL")
@dp.expect_or_drop("positive_interval_count", "interval_count > 0 AND interval_count <= 12")
@dp.expect_or_drop("reasonable_avg_demand", "avg_demand_mw >= 0 AND avg_demand_mw < 25000")
def gold_demand_hourly():
    """Hourly aggregation of demand, price, and weather data.

    Used by the Dash app for the Load Forecast view and for quick
    exploration of demand patterns across regions.
    """
    demand = spark.read.table("silver_demand_weather")
    weather = spark.read.table("bronze_bom_weather")

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
