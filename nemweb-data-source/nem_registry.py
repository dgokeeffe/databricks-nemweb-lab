"""
NEM Generator Registry Data Source

Custom PySpark data source providing generator metadata for joining with dispatch data.
Data source: https://github.com/opennem/opennem/tree/main/opennem/data

Usage:
    # Register and read
    spark.dataSource.register(NemRegistryDataSource)
    registry_df = spark.read.format("nem_registry").load()

    # Join with dispatch data
    dispatch_df = spark.read.format("nemweb").option("table", "DISPATCH_UNIT_SCADA").load()
    enriched = dispatch_df.join(registry_df, "DUID", "left")

    # Filter options
    spark.read.format("nem_registry").option("state", "VIC").load()
    spark.read.format("nem_registry").option("fuel_category", "wind").load()

Schema:
    DUID: string - Dispatchable Unit Identifier
    station_code: string - Station identifier
    station_name: string - Human-readable station name
    state: string - Australian state (NSW, VIC, QLD, SA, TAS)
    fueltech: string - Fuel technology (coal_black, wind, solar_utility, etc.)
    fuel_category: string - Broad category (coal, gas, hydro, wind, solar, battery)
    capacity_mw: double - Registered capacity in megawatts
    lat: double - Latitude
    lng: double - Longitude
    status: string - Operating status
"""

import json
import logging
from pathlib import Path
from typing import Iterator, Tuple, Optional

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logger = logging.getLogger(__name__)

# Fuel tech categories for analysis
FUEL_CATEGORIES = {
    "coal": ["coal_black", "coal_brown"],
    "gas": ["gas_ccgt", "gas_ocgt", "gas_steam", "gas_recip", "gas_wcmg"],
    "hydro": ["hydro"],
    "wind": ["wind"],
    "solar": ["solar_utility", "solar_rooftop"],
    "battery": ["battery_charging", "battery_discharging"],
    "other": ["bioenergy_biogas", "bioenergy_biomass", "distillate", "pumps"],
}


def get_fuel_category(fueltech: str) -> str:
    """Map a fueltech to a broader category."""
    if not fueltech:
        return "unknown"
    for category, fueltechs in FUEL_CATEGORIES.items():
        if fueltech in fueltechs:
            return category
    return "other"


# OpenNEM GitHub URL for latest station data
OPENNEM_STATIONS_URL = "https://raw.githubusercontent.com/opennem/opennem/master/opennem/data/stations.json"


def _fetch_from_opennem() -> list[dict]:
    """Fetch latest station data from OpenNEM GitHub."""
    import urllib.request
    from urllib.error import HTTPError, URLError

    try:
        request = urllib.request.Request(
            OPENNEM_STATIONS_URL,
            headers={"User-Agent": "DatabricksNemwebLab/2.0"}
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode('utf-8')
            return json.loads(data)
    except (HTTPError, URLError) as e:
        logger.warning(f"Failed to fetch from OpenNEM: {e}")
        return []


def _get_local_data_path() -> Optional[Path]:
    """Get path to local data file (fallback)."""
    # Try same directory as this module (for wheel installation)
    module_dir = Path(__file__).parent
    wheel_path = module_dir / "nem_stations.json"
    if wheel_path.exists():
        return wheel_path

    # Try relative to this file (for local development)
    local_path = module_dir.parent / "data" / "nem_stations.json"
    if local_path.exists():
        return local_path

    return None


def load_registry_data() -> list[dict]:
    """
    Load registry data from OpenNEM GitHub (primary) or local file (fallback).

    Fetches the latest station data from OpenNEM's GitHub repository.
    Falls back to bundled JSON file if network is unavailable.
    """
    # Try fetching latest from OpenNEM
    stations = _fetch_from_opennem()

    if not stations:
        # Fallback to local file
        local_path = _get_local_data_path()
        if local_path and local_path.exists():
            logger.info(f"Using local fallback: {local_path}")
            stations = json.loads(local_path.read_text())
        else:
            logger.warning("No station data available (network failed, no local fallback)")
            return []

    # Build flat list of DUIDs with metadata
    rows = []
    for station in stations:
        location = station.get('location', {}) or {}

        for fac in station.get('facilities', []):
            duid = fac.get('code')
            if duid:
                fueltech = fac.get('fueltech_id')
                rows.append({
                    'DUID': duid,
                    'station_code': station.get('code'),
                    'station_name': station.get('name'),
                    'state': location.get('state'),
                    'fueltech': fueltech,
                    'fuel_category': get_fuel_category(fueltech),
                    'capacity_mw': fac.get('capacity_registered'),
                    'lat': location.get('lat'),
                    'lng': location.get('lng'),
                    'status': fac.get('status_id'),
                })

    logger.info(f"Loaded {len(rows)} generator units from registry")
    return rows


class NemRegistryPartition(InputPartition):
    """Single partition for registry data (small dataset, single partition)."""

    def __init__(self, state_filter: Optional[str] = None, fuel_filter: Optional[str] = None):
        self.state_filter = state_filter
        self.fuel_filter = fuel_filter


class NemRegistryReader(DataSourceReader):
    """Reader for NEM registry data."""

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.state_filter = options.get("state")
        self.fuel_filter = options.get("fuel_category")

    def partitions(self) -> list[InputPartition]:
        """Single partition for this small dataset."""
        return [NemRegistryPartition(self.state_filter, self.fuel_filter)]

    def read(self, partition: NemRegistryPartition) -> Iterator[Tuple]:
        """Read registry data and yield tuples."""
        data = load_registry_data()

        for row in data:
            # Apply filters
            if partition.state_filter:
                if row.get('state') != partition.state_filter:
                    continue

            if partition.fuel_filter:
                if row.get('fuel_category') != partition.fuel_filter:
                    continue

            # Yield tuple in schema order
            yield (
                row.get('DUID'),
                row.get('station_code'),
                row.get('station_name'),
                row.get('state'),
                row.get('fueltech'),
                row.get('fuel_category'),
                row.get('capacity_mw'),
                row.get('lat'),
                row.get('lng'),
                row.get('status'),
            )


class NemRegistryDataSource(DataSource):
    """
    Custom PySpark Data Source for NEM generator registry.

    Provides metadata for all registered generators in the NEM including
    station names, fuel types, capacities, and locations.

    Options:
        state (str): Filter by state (NSW, VIC, QLD, SA, TAS)
        fuel_category (str): Filter by fuel category (coal, gas, wind, solar, etc.)

    Example:
        spark.dataSource.register(NemRegistryDataSource)

        # Load all generators
        registry = spark.read.format("nem_registry").load()

        # Filter to Victorian wind farms
        vic_wind = (spark.read.format("nem_registry")
                    .option("state", "VIC")
                    .option("fuel_category", "wind")
                    .load())

        # Join with dispatch data
        dispatch = spark.read.format("nemweb").load()
        enriched = dispatch.join(registry, "DUID", "left")
    """

    @classmethod
    def name(cls) -> str:
        return "nem_registry"

    def schema(self) -> StructType:
        return StructType([
            StructField("DUID", StringType(), False),
            StructField("station_code", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("fueltech", StringType(), True),
            StructField("fuel_category", StringType(), True),
            StructField("capacity_mw", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
            StructField("status", StringType(), True),
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        return NemRegistryReader(schema, self.options)


# Convenience functions for non-Spark usage
def get_duid_info(duid: str) -> Optional[dict]:
    """Get metadata for a single DUID."""
    for row in load_registry_data():
        if row.get('DUID') == duid:
            return row
    return None


def get_capacity_by_state() -> dict:
    """Get total registered capacity by state."""
    data = load_registry_data()
    by_state = {}
    for row in data:
        state = row.get("state") or "Unknown"
        capacity = row.get("capacity_mw") or 0
        by_state[state] = by_state.get(state, 0) + capacity
    return dict(sorted(by_state.items(), key=lambda x: -x[1]))


def get_capacity_by_fueltech() -> dict:
    """Get total registered capacity by fuel technology."""
    data = load_registry_data()
    by_fuel = {}
    for row in data:
        fuel = row.get("fueltech") or "unknown"
        capacity = row.get("capacity_mw") or 0
        by_fuel[fuel] = by_fuel.get(fuel, 0) + capacity
    return dict(sorted(by_fuel.items(), key=lambda x: -x[1]))


if __name__ == "__main__":
    print("NEM Generator Registry Data Source")
    print("=" * 60)

    data = load_registry_data()
    print(f"Total units: {len(data)}")

    print("\nCapacity by State:")
    for state, cap in get_capacity_by_state().items():
        print(f"  {state}: {cap:,.0f} MW")

    print("\nCapacity by Fuel Category:")
    by_cat = {}
    for row in data:
        cat = row.get("fuel_category", "unknown")
        cap = row.get("capacity_mw") or 0
        by_cat[cat] = by_cat.get(cat, 0) + cap
    for cat, cap in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {cap:,.0f} MW")
