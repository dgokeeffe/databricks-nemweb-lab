from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, date_trunc, avg, abs as _abs, count,
)


@dp.materialized_view(
    name="gold_forecast_accuracy",
    comment="Hourly AEMO P5MIN forecast accuracy - MAPE, bias, and error statistics by region"
)
@dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_hour", "hour IS NOT NULL")
@dp.expect_or_drop("positive_interval_count", "interval_count > 0 AND interval_count <= 12")
@dp.expect_or_drop("reasonable_demand_mape", "demand_mape_pct IS NULL OR demand_mape_pct BETWEEN 0 AND 500")
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
            avg(_abs(col("demand_error_mw"))).alias("demand_mae_mw"),
            avg(col("demand_error_mw")).alias("demand_bias_mw"),
            avg(
                _abs(col("demand_error_mw")) / _abs(col("actual_demand_mw")) * 100
            ).alias("demand_mape_pct"),
            avg(_abs(col("price_error"))).alias("price_mae"),
            avg(col("price_error")).alias("price_bias"),
            avg("lead_time_minutes").alias("avg_lead_time_minutes"),
            count("*").alias("interval_count"),
        )
    )
