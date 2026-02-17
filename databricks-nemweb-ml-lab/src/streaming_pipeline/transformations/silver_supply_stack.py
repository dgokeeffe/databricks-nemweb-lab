from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.functions import expr, max as _max, sum as _sum, count


@dp.materialized_view(
    name="silver_supply_stack",
    comment="NEM-wide generation metrics aggregated from unit-level SCADA data"
)
@dp.expect_or_drop("valid_settlement_date", "SETTLEMENTDATE IS NOT NULL")
@dp.expect_or_drop("non_negative_total_generation", "total_generation_mw >= 0")
@dp.expect_or_drop("positive_total_duid_count", "total_duid_count > 0")
def silver_supply_stack():
    """Aggregate DISPATCH_UNIT_SCADA to a per-interval supply summary.

    Provides:
    - Total NEM generation (sum of all positive SCADAVALUE)
    - Number of active units (SCADAVALUE > 0)
    - Number of offline units (SCADAVALUE = 0 or NULL)
    - Largest single unit output (potential contingency)

    Note: SCADA has no REGIONID. For regional breakdown, join with
    DUDETAILSUMMARY (participant registration) which maps DUID to REGIONID.
    """
    scada = spark.read.table("bronze_scada_stream")

    return (
        scada
        .groupBy("SETTLEMENTDATE")
        .agg(
            _sum(
                expr("CASE WHEN SCADAVALUE > 0 THEN SCADAVALUE ELSE 0 END")
            ).alias("total_generation_mw"),
            count(expr("CASE WHEN SCADAVALUE > 0 THEN 1 END")).alias("active_unit_count"),
            count(expr("CASE WHEN SCADAVALUE <= 0 OR SCADAVALUE IS NULL THEN 1 END")).alias("offline_unit_count"),
            _max("SCADAVALUE").alias("largest_unit_mw"),
            count("*").alias("total_duid_count"),
        )
    )
