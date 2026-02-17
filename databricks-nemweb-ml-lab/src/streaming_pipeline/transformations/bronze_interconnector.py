from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_interconnector_stream",
    comment="Cross-regional interconnector flows and constraints from NEMWEB Dispatch Reports"
)
@dp.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dp.expect_or_drop("valid_interconnector", "INTERCONNECTORID IS NOT NULL AND INTERCONNECTORID <> ''")
@dp.expect_or_drop("reasonable_metered_flow", "METEREDMWFLOW IS NOT NULL AND ABS(METEREDMWFLOW) <= 15000")
@dp.expect_or_drop("valid_intervention", "INTERVENTION IN ('0', '1')")
@dp.expect_or_drop(
    "recent_ingest",
    "_ingested_at >= current_timestamp() - INTERVAL 1 DAY",
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
