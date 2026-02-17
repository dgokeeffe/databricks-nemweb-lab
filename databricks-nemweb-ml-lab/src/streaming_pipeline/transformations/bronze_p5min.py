from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_p5min_forecast",
    comment="AEMO 5-minute pre-dispatch forecasts - demand and price for the next 12 intervals (1 hour ahead)"
)
@dp.expect_or_drop("valid_region", "REGIONID IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_run_and_interval", "RUN_DATETIME IS NOT NULL AND INTERVAL_DATETIME IS NOT NULL")
@dp.expect_or_drop("interval_after_run", "INTERVAL_DATETIME >= RUN_DATETIME")
@dp.expect_or_drop("valid_intervention", "INTERVENTION IN ('0', '1')")
@dp.expect_or_drop("reasonable_forecast_demand", "TOTALDEMAND >= 0 AND TOTALDEMAND < 25000")
@dp.expect_or_drop("reasonable_forecast_rrp", "RRP BETWEEN -1500 AND 25000")
@dp.expect_or_drop(
    "recent_ingest",
    "_ingested_at >= current_timestamp() - INTERVAL 1 DAY",
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
