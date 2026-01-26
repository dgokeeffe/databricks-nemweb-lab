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
from typing import Iterator, Tuple, Optional, TYPE_CHECKING
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# Lazy import pyspark types - only needed for schema functions
if TYPE_CHECKING:
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure logging for NEMWEB utilities.

    Call this in your Databricks notebook to see debug output:

        from nemweb_utils import configure_logging
        configure_logging(logging.DEBUG)

    Args:
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
    """
    # Configure the nemweb_utils logger
    logger.setLevel(level)

    # Add handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.info(f"NEMWEB logging configured at level: {logging.getLevelName(level)}")


# NEMWEB base URLs
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
NEMWEB_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE"

# Mapping of MMS table names to NEMWEB folder paths, file prefixes, and record types
# The NEMWEB CSV format contains multiple record types per file:
#   C = Comment/metadata
#   I = Header row for a record type
#   D = Data row
# The record_type is used to filter rows from the multi-record CSV
TABLE_CONFIG = {
    "DISPATCHREGIONSUM": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
        "record_type": "DISPATCH,REGIONSUM"  # Matches I/D rows like "D,DISPATCH,REGIONSUM,..."
    },
    "DISPATCHPRICE": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
        "record_type": "DISPATCH,PRICE"
    },
    "TRADINGPRICE": {
        "folder": "TradingIS_Reports",
        "file_prefix": "TRADINGIS",
        "record_type": "TRADING,PRICE"
    },
    "DISPATCH_UNIT_SCADA": {
        "folder": "Dispatch_SCADA",
        "file_prefix": "DISPATCHSCADA",
        "record_type": "DISPATCH,UNIT_SCADA"
    },
    "ROOFTOP_PV_ACTUAL": {
        "folder": "ROOFTOP_PV/ACTUAL",
        "file_prefix": "ROOFTOP_PV_ACTUAL",
        "record_type": None  # Uses standard CSV format
    },
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
    logger.info(f"fetch_nemweb_data: table={table}, region={region}, "
                f"start={start_date}, end={end_date}, use_sample={use_sample}")

    if use_sample:
        sample_data = _get_sample_data(table, region)
        logger.info(f"Returning {len(sample_data)} sample rows")
        return sample_data

    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}. Supported: {list(TABLE_CONFIG.keys())}")

    config = TABLE_CONFIG[table]
    record_type = config.get("record_type")
    logger.debug(f"Table config: folder={config['folder']}, prefix={config['file_prefix']}, "
                 f"record_type={record_type}")
    rows = []

    # Parse date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    num_days = (end - start).days + 1
    logger.info(f"Fetching {num_days} day(s) of data: {start_date} to {end_date}")
    current = start

    day_count = 0
    while current <= end:
        day_count += 1
        try:
            url = _build_nemweb_url(config["folder"], config["file_prefix"], current)
            logger.debug(f"[Day {day_count}/{num_days}] Fetching: {url}")
            data = _fetch_and_extract_zip(url, record_type=record_type)
            logger.debug(f"[Day {day_count}/{num_days}] Got {len(data)} rows for {current.date()}")

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

    logger.info(f"fetch_nemweb_data complete: {len(rows)} total rows fetched")
    if rows:
        # Log sample of first row keys for debugging schema issues
        sample_keys = list(rows[0].keys())[:5]
        logger.debug(f"Sample row keys: {sample_keys}...")
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


def _fetch_and_extract_zip(url: str, record_type: str = None, use_retry: bool = True) -> list[dict]:
    """
    Fetch a ZIP file from URL and extract CSV data.

    Args:
        url: URL to the ZIP file
        record_type: NEMWEB record type filter (e.g., "DISPATCH,REGIONSUM")
                     If None, uses standard CSV parsing
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
            # Handle nested ZIPs (archive files contain ZIPs inside)
            if name.endswith(".zip") or name.endswith(".ZIP"):
                with zf.open(name) as nested_zip_file:
                    nested_zip_data = io.BytesIO(nested_zip_file.read())
                    with zipfile.ZipFile(nested_zip_data) as nested_zf:
                        for nested_name in nested_zf.namelist():
                            if nested_name.endswith(".CSV") or nested_name.endswith(".csv"):
                                with nested_zf.open(nested_name) as csv_file:
                                    csv_rows = _parse_nemweb_csv_file(csv_file, record_type)
                                    rows.extend(csv_rows)

            # Also handle CSVs directly in the ZIP (for CURRENT files)
            elif name.endswith(".CSV") or name.endswith(".csv"):
                with zf.open(name) as csv_file:
                    csv_rows = _parse_nemweb_csv_file(csv_file, record_type)
                    rows.extend(csv_rows)

    return rows


def _parse_nemweb_csv_file(csv_file, record_type: str = None) -> list[dict]:
    """
    Parse a NEMWEB CSV file, handling the multi-record format.

    NEMWEB CSV format (MMS format with multiple tables per file):
        C,... = Comment/metadata row
        I,CATEGORY,RECORD_TYPE,VERSION,COL1,COL2,... = Header row
        D,CATEGORY,RECORD_TYPE,VERSION,VAL1,VAL2,... = Data row

    Uses csv.reader for proper handling of quoted fields (e.g., timestamps).
    Based on OpenNEM's parsing approach: https://github.com/opennem/opennem

    Args:
        csv_file: File-like object for the CSV
        record_type: Record type to filter (e.g., "DISPATCH,REGIONSUM")
                     If None, uses standard CSV parsing

    Returns:
        List of dictionaries
    """
    text = csv_file.read().decode("utf-8")

    if not record_type:
        # Standard CSV format - use DictReader
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    # NEMWEB multi-record format - use csv.reader for proper quote handling
    rows = []
    headers = None

    # csv.reader properly handles quoted fields like "2025/12/27 00:05:00"
    reader = csv.reader(io.StringIO(text))

    for parts in reader:
        if not parts:
            continue

        row_type = parts[0].strip().upper()

        if row_type == "I":
            # Header row: I,CATEGORY,RECORD_TYPE,VERSION,COL1,COL2,...
            if len(parts) > 2:
                row_record = f"{parts[1]},{parts[2]}"
                if row_record == record_type:
                    # Columns start at index 4 (after I,CATEGORY,RECORD,VERSION)
                    headers = parts[4:]

        elif row_type == "D" and headers:
            # Data row: D,CATEGORY,RECORD_TYPE,VERSION,VAL1,VAL2,...
            if len(parts) > 2:
                row_record = f"{parts[1]},{parts[2]}"
                if row_record == record_type:
                    # Values start at index 4
                    values = parts[4:]
                    # Create dict - csv.reader already handles quote stripping
                    row_dict = dict(zip(headers, values))
                    # Fill missing values with None
                    for header in headers[len(values):]:
                        row_dict[header] = None
                    rows.append(row_dict)

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


def parse_nemweb_csv(data: list[dict], schema: "StructType") -> Iterator[Tuple]:
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

    logger.debug(f"parse_nemweb_csv: parsing {len(data)} rows with {len(field_names)} fields")
    logger.debug(f"Schema fields: {field_names}")

    # Log first row's raw values for debugging
    if data and logger.isEnabledFor(logging.DEBUG):
        first_row = data[0]
        logger.debug(f"First row sample - raw values:")
        for name in field_names[:5]:  # First 5 fields
            raw_val = first_row.get(name)
            logger.debug(f"  {name}: {repr(raw_val)} (type: {type(raw_val).__name__})")

    row_count = 0
    for row in data:
        row_count += 1
        values = []
        for name in field_names:
            raw_value = row.get(name)

            if raw_value is None or raw_value == "":
                values.append(None)
            else:
                converted = _convert_value(raw_value, field_types[name])
                # Log conversion issues at DEBUG level
                if converted is None and raw_value:
                    logger.debug(f"Row {row_count}: {name} conversion returned None "
                                f"for value {repr(raw_value)}")
                values.append(converted)

        yield tuple(values)

    logger.debug(f"parse_nemweb_csv: yielded {row_count} tuples")


def _convert_value(value, spark_type):
    """
    Convert value to appropriate Python type based on Spark type.

    IMPORTANT: For TimestampType, this MUST return either a datetime.datetime
    object or None. Spark's Arrow serializer will fail with AssertionError
    if any other type is returned.
    """
    from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType

    # Handle None/empty values
    if value is None or value == "":
        return None

    # Ensure we have a string to work with
    if not isinstance(value, str):
        value = str(value)

    if isinstance(spark_type, StringType):
        return str(value)

    elif isinstance(spark_type, DoubleType):
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to double: {value}")
            return None

    elif isinstance(spark_type, IntegerType):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to integer: {value}")
            return None

    elif isinstance(spark_type, TimestampType):
        # CRITICAL: Must return datetime.datetime or None - Spark requires this
        # NEMWEB timestamp formats (various formats observed in AEMO data)
        timestamp_formats = [
            "%Y/%m/%d %H:%M:%S",      # 2024/01/01 00:05:00
            "%Y-%m-%d %H:%M:%S",      # 2024-01-01 00:05:00
            "%Y/%m/%d %H:%M",         # 2024/01/01 00:05 (no seconds)
            "%Y-%m-%d %H:%M",         # 2024-01-01 00:05 (no seconds)
            "%d/%m/%Y %H:%M:%S",      # 01/01/2024 00:05:00 (AU format)
            "%d/%m/%Y %H:%M",         # 01/01/2024 00:05 (AU format, no seconds)
        ]

        # Try each format
        for fmt in timestamp_formats:
            try:
                result = datetime.strptime(value, fmt)
                # Verify it's actually a datetime (defensive)
                if isinstance(result, datetime):
                    return result
            except (ValueError, TypeError):
                continue

        # If we get here, no format worked - MUST return None
        logger.warning(f"Could not parse timestamp value: {repr(value)}")
        return None

    else:
        return str(value)


def get_nemweb_schema(table: str) -> "StructType":
    """
    Get the Spark schema for a NEMWEB table.

    Schemas are based on the MMS Electricity Data Model Report:
    https://nemweb.com.au/Reports/Current/MMSDataModelReport/

    Args:
        table: MMS table name

    Returns:
        StructType schema for the table
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType
    )

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
