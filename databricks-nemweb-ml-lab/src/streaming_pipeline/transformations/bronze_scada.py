from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_scada_stream",
    comment="Real-time unit-level MW generation from NEMWEB Dispatch SCADA"
)
@dp.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dp.expect_or_drop("valid_duid", "DUID IS NOT NULL AND DUID <> ''")
@dp.expect_or_drop("reasonable_scada_value", "SCADAVALUE IS NOT NULL AND ABS(SCADAVALUE) <= 5000")
@dp.expect_or_drop(
    "recent_ingest",
    "_ingested_at >= current_timestamp() - INTERVAL 1 DAY",
)
def bronze_scada_stream():
    """Stream DISPATCH_UNIT_SCADA from NEMWEB CURRENT folder.

    Contains MW output (SCADAVALUE) for every Dispatchable Unit ID (DUID)
    in the NEM. Updated every 5 minutes. Hundreds of DUIDs per interval.

    No REGIONID filter -- SCADA reports by DUID, not region. Join with
    a participant registration table (DUDETAILSUMMARY) for region mapping.
    """
    return (
        spark.readStream
        .format("nemweb_stream")
        .option("table", "DISPATCH_UNIT_SCADA")
        .option("poll_interval_seconds", "15")
        .option("max_files_per_batch", "10")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )
