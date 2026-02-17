from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_rooftop_pv_stream",
    comment="Rooftop solar PV generation estimates from AEMO satellite data (30-min updates)"
)
@dp.expect_or_drop("valid_region", "REGIONID IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
@dp.expect_or_drop("valid_interval", "INTERVAL_DATETIME IS NOT NULL")
@dp.expect_or_drop("non_negative_power", "POWER IS NOT NULL AND POWER >= 0 AND POWER < 15000")
@dp.expect_or_drop(
    "recent_ingest",
    "_ingested_at >= current_timestamp() - INTERVAL 1 DAY",
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
