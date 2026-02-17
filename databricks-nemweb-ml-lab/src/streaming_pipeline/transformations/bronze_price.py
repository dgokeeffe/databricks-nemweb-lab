from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_price_stream",
    comment="5-minute regional spot prices streamed live from NEMWEB CURRENT"
)
@dp.expect_or_drop("valid_region", "REGIONID IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dp.expect_or_drop("valid_intervention", "INTERVENTION IN ('0', '1')")
@dp.expect_or_drop("reasonable_rrp", "RRP BETWEEN -1500 AND 25000")
@dp.expect_or_drop(
    "recent_ingest",
    "_ingested_at >= current_timestamp() - INTERVAL 1 DAY",
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
