"""
NEMWEB Utility Functions

Helper functions for fetching and parsing AEMO NEMWEB data.
Used by the custom PySpark data source implementation.

NEMWEB Data Structure:
    - Current reports: https://www.nemweb.com.au/REPORTS/CURRENT/
    - Archive reports: https://www.nemweb.com.au/REPORTS/ARCHIVE/
    - File format: CSV within ZIP archives
    - Naming: PUBLIC_{TABLE}_{YYYYMMDDHHMM}.CSV
"""

import csv
import io
import logging
import zipfile
from datetime import datetime, timedelta
from typing import Iterator, Tuple, Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

logger = logging.getLogger(__name__)

# NEMWEB base URLs
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
NEMWEB_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE"

# Mapping of MMS table names to NEMWEB folder paths and file prefixes
# The table name (e.g., DISPATCHREGIONSUM) is the data inside the CSV
# The file prefix (e.g., DISPATCHSCADA) is what the ZIP file is named
TABLE_CONFIG = {
    "DISPATCHREGIONSUM": {"folder": "Dispatch_SCADA", "file_prefix": "DISPATCHSCADA"},
    "DISPATCHPRICE": {"folder": "DispatchIS_Reports", "file_prefix": "DISPATCHIS"},
    "TRADINGPRICE": {"folder": "TradingIS_Reports", "file_prefix": "TRADINGIS"},
    "DISPATCH_UNIT_SCADA": {"folder": "Dispatch_SCADA", "file_prefix": "DISPATCHSCADA"},
    "ROOFTOP_PV_ACTUAL": {"folder": "ROOFTOP_PV/ACTUAL", "file_prefix": "ROOFTOP_PV_ACTUAL"},
}

# Legacy mapping for backwards compatibility
TABLE_TO_FOLDER = {k: v["folder"] for k, v in TABLE_CONFIG.items()}

# Request timeout in seconds
REQUEST_TIMEOUT = 30

# User agent for HTTP requests
USER_AGENT = "DatabricksNemwebLab/1.0"

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds


def fetch_with_retry(
    url: str,
    max_retries: int = MAX_RETRIES,
    base_delay: float = RETRY_BASE_DELAY
) -> bytes:
    """
    Fetch URL with exponential backoff retry.

    Args:
        url: URL to fetch
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff

    Returns:
        Response bytes

    Raises:
        HTTPError: After all retries exhausted
    """
    import time

    last_error = None

    for attempt in range(max_retries):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()

        except (HTTPError, URLError) as e:
            last_error = e

            # Don't retry on 404 (data doesn't exist)
            if isinstance(e, HTTPError) and e.code == 404:
                raise

            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {url}")

    raise last_error


def fetch_nemweb_data(
    table: str,
    region: Optional[str] = None,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-07",
    use_sample: bool = False
) -> list[dict]:
    """
    Fetch data from NEMWEB for the specified table and date range.

    Args:
        table: MMS table name (e.g., DISPATCHREGIONSUM)
        region: Optional region filter (e.g., NSW1)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        use_sample: If True, return sample data instead of fetching

    Returns:
        List of dictionaries representing rows

    Raises:
        ValueError: If table is not supported
        HTTPError: If HTTP request fails
    """
    # TODO (Exercise 1.3): Implement HTTP fetching
    # Hint: Use _build_nemweb_url() and _fetch_and_extract_zip()
    # Consider: How to handle missing files for specific dates?

    if use_sample:
        return _get_sample_data(table, region)

    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}. Supported: {list(TABLE_CONFIG.keys())}")

    config = TABLE_CONFIG[table]
    rows = []

    # Parse date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    while current <= end:
        try:
            url = _build_nemweb_url(config["folder"], config["file_prefix"], current)
            data = _fetch_and_extract_zip(url)

            # Filter by region if specified
            if region:
                data = [row for row in data if row.get("REGIONID") == region]

            rows.extend(data)

        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"No data for {current.date()}: {url}")
            else:
                raise

        current += timedelta(days=1)

    return rows


def _build_nemweb_url(folder: str, file_prefix: str, date: datetime) -> str:
    """
    Build NEMWEB URL for the specified folder, file prefix, and date.

    Recent data (< 7 days) is in CURRENT, older data is in ARCHIVE.
    Archive files are daily aggregates named PUBLIC_{prefix}_{YYYYMMDD}.zip
    """
    days_ago = (datetime.now() - date).days

    # NEMWEB file naming convention
    date_str = date.strftime("%Y%m%d")
    filename = f"PUBLIC_{file_prefix}_{date_str}.zip"

    if days_ago < 7:
        return f"{NEMWEB_CURRENT_URL}/{folder}/{filename}"
    else:
        return f"{NEMWEB_ARCHIVE_URL}/{folder}/{filename}"


def _fetch_and_extract_zip(url: str, use_retry: bool = True) -> list[dict]:
    """
    Fetch a ZIP file from URL and extract CSV data.

    Args:
        url: URL to the ZIP file
        use_retry: Whether to use retry logic (default: True)

    Returns:
        List of dictionaries from the CSV
    """
    logger.info(f"Fetching: {url}")

    if use_retry:
        raw_data = fetch_with_retry(url)
    else:
        request = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            raw_data = response.read()

    zip_data = io.BytesIO(raw_data)

    rows = []
    with zipfile.ZipFile(zip_data) as zf:
        for name in zf.namelist():
            if name.endswith(".CSV") or name.endswith(".csv"):
                with zf.open(name) as csv_file:
                    # NEMWEB CSVs have a header row
                    text_wrapper = io.TextIOWrapper(csv_file, encoding="utf-8")
                    reader = csv.DictReader(text_wrapper)
                    rows.extend(list(reader))

    return rows


def _get_sample_data(table: str, region: Optional[str] = None) -> list[dict]:
    """
    Return sample data for offline development and testing.
    """
    sample = [
        {
            "SETTLEMENTDATE": "2024-01-01 00:05:00",
            "RUNNO": "1",
            "REGIONID": "NSW1",
            "DISPATCHINTERVAL": "1",
            "INTERVENTION": "0",
            "TOTALDEMAND": "7500.5",
            "AVAILABLEGENERATION": "8000.0",
            "NETINTERCHANGE": "-200.5",
        },
        {
            "SETTLEMENTDATE": "2024-01-01 00:05:00",
            "RUNNO": "1",
            "REGIONID": "VIC1",
            "DISPATCHINTERVAL": "1",
            "INTERVENTION": "0",
            "TOTALDEMAND": "5200.3",
            "AVAILABLEGENERATION": "5500.0",
            "NETINTERCHANGE": "150.2",
        },
        {
            "SETTLEMENTDATE": "2024-01-01 00:05:00",
            "RUNNO": "1",
            "REGIONID": "QLD1",
            "DISPATCHINTERVAL": "1",
            "INTERVENTION": "0",
            "TOTALDEMAND": "6100.8",
            "AVAILABLEGENERATION": "6800.0",
            "NETINTERCHANGE": "-50.5",
        },
        {
            "SETTLEMENTDATE": "2024-01-01 00:05:00",
            "RUNNO": "1",
            "REGIONID": "SA1",
            "DISPATCHINTERVAL": "1",
            "INTERVENTION": "0",
            "TOTALDEMAND": "1800.2",
            "AVAILABLEGENERATION": "2100.0",
            "NETINTERCHANGE": "100.3",
        },
        {
            "SETTLEMENTDATE": "2024-01-01 00:05:00",
            "RUNNO": "1",
            "REGIONID": "TAS1",
            "DISPATCHINTERVAL": "1",
            "INTERVENTION": "0",
            "TOTALDEMAND": "1100.5",
            "AVAILABLEGENERATION": "1400.0",
            "NETINTERCHANGE": "-50.0",
        },
    ]

    if region:
        sample = [row for row in sample if row["REGIONID"] == region]

    return sample


def parse_nemweb_csv(data: list[dict], schema: StructType) -> Iterator[Tuple]:
    """
    Parse NEMWEB data and yield tuples matching the Spark schema.

    Args:
        data: List of dictionaries from CSV
        schema: Spark StructType to match

    Yields:
        Tuples with values in schema order
    """
    field_names = [field.name for field in schema.fields]
    field_types = {field.name: field.dataType for field in schema.fields}

    for row in data:
        values = []
        for name in field_names:
            raw_value = row.get(name)

            if raw_value is None or raw_value == "":
                values.append(None)
            else:
                values.append(_convert_value(raw_value, field_types[name]))

        yield tuple(values)


def _convert_value(value: str, spark_type) -> any:
    """Convert string value to appropriate Python type based on Spark type."""
    if value is None or value == "":
        return None

    if isinstance(spark_type, StringType):
        return str(value)
    elif isinstance(spark_type, DoubleType):
        return float(value)
    elif isinstance(spark_type, IntegerType):
        return int(float(value))
    elif isinstance(spark_type, TimestampType):
        # NEMWEB format: "2024/01/01 00:05:00" or "2024-01-01 00:05:00"
        for fmt in ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return value  # Return as string if parsing fails
    else:
        return str(value)


def get_nemweb_schema(table: str) -> StructType:
    """
    Get the Spark schema for a NEMWEB table.

    Schemas are based on the MMS Electricity Data Model Report:
    https://nemweb.com.au/Reports/Current/MMSDataModelReport/

    Args:
        table: MMS table name

    Returns:
        StructType schema for the table
    """
    schemas = {
        "DISPATCHREGIONSUM": StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("RUNNO", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
            StructField("INTERVENTION", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("AVAILABLEGENERATION", DoubleType(), True),
            StructField("AVAILABLELOAD", DoubleType(), True),
            StructField("DEMANDFORECAST", DoubleType(), True),
            StructField("DISPATCHABLEGENERATION", DoubleType(), True),
            StructField("DISPATCHABLELOAD", DoubleType(), True),
            StructField("NETINTERCHANGE", DoubleType(), True),
        ]),

        "DISPATCHPRICE": StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("RUNNO", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
            StructField("INTERVENTION", StringType(), True),
            StructField("RRP", DoubleType(), True),  # Regional Reference Price
            StructField("EEP", DoubleType(), True),  # Excess Energy Price
            StructField("ROP", DoubleType(), True),  # Regional Override Price
            StructField("APCFLAG", StringType(), True),
            StructField("MARKETSUSPENDEDFLAG", StringType(), True),
        ]),

        "TRADINGPRICE": StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("RUNNO", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("PERIODID", StringType(), True),
            StructField("RRP", DoubleType(), True),
            StructField("EEP", DoubleType(), True),
            StructField("INVALIDFLAG", StringType(), True),
        ]),
    }

    if table not in schemas:
        # Return a generic schema for unknown tables
        logger.warning(f"No schema defined for {table}, using generic schema")
        return StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("DATA", StringType(), True),
        ])

    return schemas[table]


def list_available_tables() -> list[str]:
    """Return list of supported NEMWEB tables."""
    return list(TABLE_TO_FOLDER.keys())


def get_nem_regions() -> list[str]:
    """Return list of NEM region IDs."""
    return ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]
