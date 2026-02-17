from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import col, row_number, round as _round
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver_forecast_vs_actual",
    comment="AEMO P5MIN forecast vs actual dispatch - demand and price accuracy with lead time"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_settlement_date", "settlement_date IS NOT NULL")
@dp.expect_or_drop("reasonable_lead_time", "lead_time_minutes BETWEEN 0 AND 180")
@dp.expect_or_drop("reasonable_actual_demand", "actual_demand_mw >= 0 AND actual_demand_mw < 25000")
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
            _round(
                (col("f.INTERVAL_DATETIME").cast("long") - col("f.RUN_DATETIME").cast("long")) / 60
            ).alias("lead_time_minutes"),
            col("f.TOTALDEMAND").alias("forecast_demand_mw"),
            col("a.total_demand_mw").alias("actual_demand_mw"),
            _round(col("f.TOTALDEMAND") - col("a.total_demand_mw"), 1).alias("demand_error_mw"),
            col("f.RRP").alias("forecast_rrp"),
            col("a.rrp").alias("actual_rrp"),
            _round(col("f.RRP") - col("a.rrp"), 2).alias("price_error"),
            col("f.AVAILABLEGENERATION").alias("forecast_available_gen_mw"),
            col("a.available_generation_mw").alias("actual_available_gen_mw"),
            col("f.NETINTERCHANGE").alias("forecast_net_interchange_mw"),
            col("a.net_interchange_mw").alias("actual_net_interchange_mw"),
        )
    )
