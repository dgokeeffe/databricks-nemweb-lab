from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, date_trunc, avg, min as _min, max as _max,
    abs as _abs, count,
)


@dp.materialized_view(
    name="gold_interconnector_hourly",
    comment="Hourly interconnector flow summary - utilisation and constraint binding"
)
@dp.expect_or_drop("valid_interconnector", "INTERCONNECTORID IS NOT NULL AND INTERCONNECTORID <> ''")
@dp.expect_or_drop("valid_hour", "hour IS NOT NULL")
@dp.expect_or_drop("positive_interval_count", "interval_count > 0 AND interval_count <= 12")
@dp.expect_or_drop("non_negative_utilisation", "avg_utilisation_pct >= 0")
def gold_interconnector_hourly():
    """Hourly aggregation of interconnector flows for cross-regional analysis.

    Shows average, min, and max flows for each interconnector plus
    utilisation metrics. When flow approaches import/export limits,
    the interconnector is "binding" and regional prices decouple.

    Key interconnectors:
    - N-Q-MNSP1: NSW <-> QLD (Queensland-NSW Interconnector)
    - VIC1-NSW1: VIC <-> NSW (main southern link)
    - V-SA: VIC <-> SA (Heywood interconnector)
    - T-V-MNSP1: TAS <-> VIC (Basslink undersea cable)
    """
    return (
        spark.read.table("bronze_interconnector_stream")
        .withColumn("hour", date_trunc("hour", col("SETTLEMENTDATE")))
        .groupBy("INTERCONNECTORID", "hour")
        .agg(
            avg("METEREDMWFLOW").alias("avg_flow_mw"),
            _min("METEREDMWFLOW").alias("min_flow_mw"),
            _max("METEREDMWFLOW").alias("max_flow_mw"),
            avg("MWLOSSES").alias("avg_losses_mw"),
            avg("IMPORTLIMIT").alias("avg_import_limit_mw"),
            avg("EXPORTLIMIT").alias("avg_export_limit_mw"),
            avg(
                _abs(col("METEREDMWFLOW")) /
                expr("GREATEST(ABS(IMPORTLIMIT), ABS(EXPORTLIMIT), 1)") * 100
            ).alias("avg_utilisation_pct"),
            count("*").alias("interval_count"),
        )
    )
