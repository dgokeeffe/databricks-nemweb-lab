# =============================================================================
# Custom Data Sources
# =============================================================================

@dp.table(
    name="nemweb_dispatch_regionsum_lf",
    comment="NEMWEB DISPATCHREGIONSUM (demand/supply) loaded via nemweb_arrow"
)
def nemweb_dispatch_regionsum():
    return (
        spark.read.format("nemweb_arrow")
        .option("volume_path", VOLUME_PATH)
        .option("table", "DISPATCHREGIONSUM")
        .option("start_date", START_DATE)
        .option("end_date", END_DATE)
        .option("auto_download", "true")
        .option("max_workers", "8")
        .option("skip_existing", "true")
        .option("include_current", INCLUDE_CURRENT)
        .load()
    )

@dp.table(
    name="nemweb_dispatch_prices_lf",
    comment="NEMWEB DISPATCHPRICE (price) loaded via nemweb_arrow"
)
def nemweb_dispatch_prices():
    return (
        spark.read.format("nemweb_arrow")
        .option("volume_path", VOLUME_PATH)
        .option("table", "DISPATCHPRICE")
        .option("start_date", START_DATE)
        .option("end_date", END_DATE)
        .option("auto_download", "true")
        .option("max_workers", "8")
        .option("skip_existing", "true")
        .option("include_current", INCLUDE_CURRENT)
        .load()
    )
from nem_registry import NemRegistryDataSource
spark.dataSource.register(NemRegistryDataSource)
@dp.table(
    name="nem_generators_lf",
    comment="NEM generator metadata loaded via nem_registry"
)
def nem_generators():
    return (
        spark.read.format("nemweb_arrow").load()
    )
