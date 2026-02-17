"""
BOM Weather Observation Fetcher

Fetches current weather observations from the Australian Bureau of Meteorology
for weather stations near each NEM region. Temperature is the primary driver
of electricity demand, making this a key feature for load forecasting.

BOM JSON Feed:
    http://www.bom.gov.au/fwo/{product_id}/{product_id}.{station_id}.json

Stations are mapped to NEM regions (closest capital city station):
    NSW1 -> Sydney Observatory Hill
    VIC1 -> Melbourne Olympic Park
    QLD1 -> Brisbane
    SA1  -> Adelaide
    TAS1 -> Hobart

Usage:
    from bom_weather import fetch_bom_observations

    # Get latest observations for all NEM regions
    obs = fetch_bom_observations()
    # Returns: [{"region_id": "NSW1", "air_temp_c": 28.5, ...}, ...]

    # Get as pandas DataFrame
    df = fetch_bom_observations(as_pandas=True)
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional, Union
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

__version__ = "1.0.0"

# BOM observation feed base URL
BOM_FWO_URL = "http://www.bom.gov.au/fwo"

# Request configuration (matching nemweb_utils patterns)
REQUEST_TIMEOUT = 30
USER_AGENT = "DatabricksNemwebLab/1.0"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0

# BOM weather stations mapped to NEM regions
# Each entry: (product_id, station_id, station_name)
NEM_REGION_STATIONS = {
    "NSW1": ("IDN60901", "94768", "Sydney Observatory Hill"),
    "VIC1": ("IDV60901", "95936", "Melbourne Olympic Park"),
    "QLD1": ("IDQ60901", "94576", "Brisbane"),
    "SA1":  ("IDS60901", "94648", "Adelaide"),
    "TAS1": ("IDT60901", "94970", "Hobart"),
}


def _fetch_with_retry(url: str) -> Optional[bytes]:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()
        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"BOM feed not found: {url}")
                return None
            logger.warning(
                f"HTTP error {e.code} fetching BOM data, "
                f"attempt {attempt + 1}/{MAX_RETRIES}"
            )
        except (URLError, TimeoutError) as e:
            logger.warning(
                f"Error fetching BOM data: {e}, "
                f"attempt {attempt + 1}/{MAX_RETRIES}"
            )

        if attempt < MAX_RETRIES - 1:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            time.sleep(delay)

    return None


def _parse_bom_json(raw_data: bytes, region_id: str) -> list[dict]:
    """
    Parse BOM JSON observation feed.

    BOM JSON structure:
        {"observations": {"data": [{"air_temp": 28.5, ...}, ...]}}

    Each observation is a half-hourly weather reading.
    """
    try:
        data = json.loads(raw_data)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to parse BOM JSON for {region_id}: {e}")
        return []

    observations = data.get("observations", {}).get("data", [])
    if not observations:
        logger.warning(f"No observations in BOM feed for {region_id}")
        return []

    results = []
    for obs in observations:
        # Parse observation timestamp
        obs_time_str = obs.get("local_date_time_full")
        obs_time = None
        if obs_time_str:
            try:
                obs_time = datetime.strptime(obs_time_str, "%Y%m%d%H%M%S")
            except ValueError:
                pass

        results.append({
            "region_id": region_id,
            "observation_time": obs_time,
            "air_temp_c": _safe_float(obs.get("air_temp")),
            "apparent_temp_c": _safe_float(obs.get("apparent_t")),
            "rel_humidity_pct": _safe_float(obs.get("rel_hum")),
            "wind_speed_kmh": _safe_float(obs.get("wind_spd_kmh")),
            "wind_dir": obs.get("wind_dir", ""),
            "cloud_oktas": _safe_float(obs.get("cloud_oktas")),
            "rain_since_9am_mm": _safe_float(obs.get("rain_trace")),
            "station_name": obs.get("name", ""),
        })

    return results


def _safe_float(val) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def fetch_bom_observations(
    regions: Optional[list[str]] = None,
    latest_only: bool = False,
    as_pandas: bool = False,
) -> Union[list[dict], "pd.DataFrame"]:
    """
    Fetch BOM weather observations for NEM regions.

    Args:
        regions: List of NEM region IDs (default: all 5 regions)
        latest_only: If True, return only the most recent observation per region
        as_pandas: If True, return a pandas DataFrame

    Returns:
        List of dicts or pandas DataFrame with weather observations.
        Each row contains: region_id, observation_time, air_temp_c,
        apparent_temp_c, rel_humidity_pct, wind_speed_kmh, wind_dir,
        cloud_oktas, rain_since_9am_mm, station_name
    """
    if regions is None:
        regions = list(NEM_REGION_STATIONS.keys())

    all_observations = []

    for region_id in regions:
        if region_id not in NEM_REGION_STATIONS:
            logger.warning(f"Unknown NEM region: {region_id}, skipping")
            continue

        product_id, station_id, station_name = NEM_REGION_STATIONS[region_id]
        url = f"{BOM_FWO_URL}/{product_id}/{product_id}.{station_id}.json"

        logger.info(f"Fetching BOM observations for {region_id} ({station_name})")
        raw_data = _fetch_with_retry(url)

        if raw_data is None:
            logger.warning(f"Failed to fetch BOM data for {region_id}")
            continue

        observations = _parse_bom_json(raw_data, region_id)

        if latest_only and observations:
            all_observations.append(observations[0])
        else:
            all_observations.extend(observations)

    logger.info(f"Fetched {len(all_observations)} BOM observations")

    if as_pandas:
        import pandas as pd
        df = pd.DataFrame(all_observations)
        if not df.empty and "observation_time" in df.columns:
            df["observation_time"] = pd.to_datetime(df["observation_time"])
        return df

    return all_observations


def get_latest_weather_by_region(
    regions: Optional[list[str]] = None,
) -> dict[str, dict]:
    """
    Get the latest weather observation for each NEM region.

    Returns:
        Dict mapping region_id to latest observation dict.
        e.g. {"NSW1": {"air_temp_c": 28.5, ...}, "VIC1": {...}}
    """
    observations = fetch_bom_observations(regions=regions, latest_only=True)
    return {obs["region_id"]: obs for obs in observations}


def generate_sample_weather() -> list[dict]:
    """
    Generate sample weather data for demo/fallback when BOM API is unreachable.

    Returns representative weather for each NEM region.
    """
    now = datetime.now()
    hour = now.hour

    # Realistic temperature patterns by region and time of day
    base_temps = {
        "NSW1": 22 + 8 * _day_cycle(hour),
        "VIC1": 18 + 10 * _day_cycle(hour),
        "QLD1": 26 + 6 * _day_cycle(hour),
        "SA1":  20 + 12 * _day_cycle(hour),
        "TAS1": 14 + 6 * _day_cycle(hour),
    }

    sample = []
    for region_id, temp in base_temps.items():
        station_name = NEM_REGION_STATIONS[region_id][2]
        sample.append({
            "region_id": region_id,
            "observation_time": now,
            "air_temp_c": round(temp, 1),
            "apparent_temp_c": round(temp - 1.5, 1),
            "rel_humidity_pct": round(55 + 20 * (1 - _day_cycle(hour)), 1),
            "wind_speed_kmh": round(15 + 10 * _day_cycle(hour), 1),
            "wind_dir": "SW",
            "cloud_oktas": round(3 + 2 * (1 - _day_cycle(hour))),
            "rain_since_9am_mm": 0.0,
            "station_name": f"{station_name} (sample)",
        })

    return sample


def _day_cycle(hour: int) -> float:
    """
    Return a 0-1 value representing position in the daily temperature cycle.
    Peak at ~14:00, trough at ~05:00.
    """
    import math
    return 0.5 + 0.5 * math.sin((hour - 5) * math.pi / 12)
