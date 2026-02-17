from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, date_trunc, avg, min as _min, max as _max, stddev, count,
)


@dp.materialized_view(
    name="gold_price_hourly",
    comment="Hourly price statistics by NEM region"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_hour", "hour IS NOT NULL")
@dp.expect_or_drop("positive_interval_count", "interval_count > 0 AND interval_count <= 12")
@dp.expect_or_drop("reasonable_avg_rrp", "avg_rrp BETWEEN -1500 AND 25000")
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
