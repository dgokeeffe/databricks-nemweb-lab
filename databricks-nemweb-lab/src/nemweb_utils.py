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

# Version for debugging - increment when making changes
__version__ = "2.10.5"

# Debug file path - check this in DBFS after errors
DEBUG_LOG_PATH = "/tmp/nemweb_debug.log"


def get_version() -> str:
    """Return the module version for debugging cache issues."""
    return __version__


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

    Always uses ARCHIVE for daily consolidated files (PUBLIC_{prefix}_{YYYYMMDD}.zip).
    CURRENT folder only contains individual 5-minute interval files, not daily aggregates.
    """
    # NEMWEB file naming convention for daily archives
    date_str = date.strftime("%Y%m%d")
    filename = f"PUBLIC_{file_prefix}_{date_str}.zip"

    # Always use ARCHIVE - CURRENT doesn't have daily consolidated files
    return f"{NEMWEB_ARCHIVE_URL}/{folder}/{filename}"


def fetch_nemweb_current(
    table: str,
    region: Optional[str] = None,
    max_files: int = 6,
    use_sample: bool = False,
    debug: bool = False
) -> list[dict]:
    """
    Fetch recent data from NEMWEB CURRENT folder (5-minute interval files).

    This is faster than fetching daily archives - useful for demos and testing.
    CURRENT contains ~7 days of 5-minute interval files.

    Args:
        table: MMS table name (e.g., DISPATCHREGIONSUM)
        region: Optional region filter (e.g., NSW1)
        max_files: Maximum number of recent files to fetch (default: 6 = 30 mins)
        use_sample: If True, return sample data instead of fetching
        debug: If True, print debug info to stdout (useful for Databricks)

    Returns:
        List of dictionaries representing rows
    """
    import re

    def log(msg):
        logger.info(msg)
        if debug:
            print(f"[NEMWEB] {msg}")

    log(f"fetch_nemweb_current: table={table}, region={region}, max_files={max_files}")

    if use_sample:
        sample_data = _get_sample_data(table, region)
        log(f"Returning {len(sample_data)} sample rows")
        return sample_data

    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}. Supported: {list(TABLE_CONFIG.keys())}")

    config = TABLE_CONFIG[table]
    folder = config["folder"]
    file_prefix = config["file_prefix"]
    record_type = config.get("record_type")

    # Fetch directory listing from CURRENT
    current_url = f"{NEMWEB_CURRENT_URL}/{folder}/"
    log(f"Listing CURRENT directory: {current_url}")

    try:
        request = Request(current_url, headers={"User-Agent": USER_AGENT})
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            html = response.read().decode('utf-8')
        log(f"Got directory listing ({len(html)} bytes)")
    except (HTTPError, URLError) as e:
        log(f"ERROR: Failed to list CURRENT directory: {e}")
        raise

    # Parse HTML for ZIP files matching our prefix
    # Pattern: PUBLIC_DISPATCHIS_202501270005_0000000500374526.zip
    # Format: PREFIX_YYYYMMDDHHMM_SEQUENCEID.zip
    pattern = rf'(PUBLIC_{file_prefix}_\d{{12}}_\d+\.zip)'
    matches = re.findall(pattern, html, re.IGNORECASE)

    if not matches:
        log(f"WARNING: No files found matching prefix {file_prefix} in CURRENT")
        if debug:
            # Print a sample of the HTML to help debug
            print(f"[NEMWEB] HTML sample (first 500 chars): {html[:500]}")
        return []

    # Deduplicate (files appear twice in HTML) and sort by filename descending
    unique_matches = sorted(set(matches), reverse=True)[:max_files]
    log(f"Found {len(unique_matches)} recent files: {unique_matches[:3]}...")

    rows = []
    for filename in unique_matches:
        url = f"{NEMWEB_CURRENT_URL}/{folder}/{filename}"
        try:
            data = _fetch_and_extract_zip(url, record_type=record_type)
            log(f"Fetched {len(data)} rows from {filename}")
            if region:
                data = [row for row in data if row.get("REGIONID") == region]
                log(f"After region filter ({region}): {len(data)} rows")
            rows.extend(data)
        except HTTPError as e:
            log(f"WARNING: Failed to fetch {filename}: {e}")
        except Exception as e:
            log(f"ERROR: Exception fetching {filename}: {e}")

    log(f"fetch_nemweb_current complete: {len(rows)} total rows")
    return rows


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


def _debug_log(msg: str) -> None:
    """Write debug message to file (visible on workers)."""
    try:
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(f"{msg}\n")
    except Exception:
        pass


def parse_nemweb_csv(data: list[dict], schema: "StructType") -> Iterator[Tuple]:
    """
    Parse NEMWEB data and yield tuples matching the Spark schema.

    All values are coerced to pure Python types for Serverless Arrow compatibility.
    Timestamps are returned as datetime.datetime objects (not pandas/numpy types).

    Args:
        data: List of dictionaries from CSV
        schema: Spark StructType to match

    Yields:
        Tuples with values in schema order (all pure Python types)
    """
    _debug_log(f"=== parse_nemweb_csv v{__version__} started ===")
    _debug_log(f"Processing {len(data)} rows")

    field_names = [field.name for field in schema.fields]
    field_types = {field.name: field.dataType for field in schema.fields}

    row_num = 0
    for row in data:
        row_num += 1
        try:
            values = []
            skip_row = False
            
            for name in field_names:
                raw_value = row.get(name)

                if raw_value is None or raw_value == "":
                    values.append(None)
                else:
                    # Convert to appropriate type
                    converted = _convert_value(raw_value, field_types[name])
                    # Coerce to pure Python type (handles pandas/numpy edge cases)
                    coerced = _to_python_scalar(converted, field_types[name])
                    
                    # CRITICAL: Validate timestamp types - Spark requires datetime.datetime or None
                    from pyspark.sql.types import TimestampType
                    if isinstance(field_types[name], TimestampType):
                        if coerced is not None and not isinstance(coerced, datetime):
                            # Skip this row if timestamp is invalid (especially for required fields like SETTLEMENTDATE)
                            _debug_log(f"Row {row_num}: Invalid timestamp for {name}: {type(coerced).__name__} = {coerced}")
                            skip_row = True
                            break
                    
                    values.append(coerced)
            
            # Skip rows with invalid timestamps
            if skip_row:
                continue

            result = tuple(values)

            # Debug first few rows
            if row_num <= 3:
                _debug_log(f"Row {row_num} tuple[0]: type={type(result[0]).__name__}, val={result[0]}")

            yield result

        except Exception as e:
            _debug_log(f"ROW {row_num} ERROR: {e}")
            continue

    _debug_log(f"=== parse_nemweb_csv complete: {row_num} rows ===")


def _to_python_scalar(v: any, spark_type: any = None) -> any:
    """
    Coerce any value to a pure Python native type.

    This ensures compatibility with Serverless Arrow fast path which requires
    exact Python types (datetime.datetime, not pandas.Timestamp or numpy.datetime64).

    Args:
        v: Value to coerce
        spark_type: Optional Spark type hint for better conversion

    Returns:
        Pure Python scalar (datetime.datetime, float, int, str, bool, None)
    """
    # None passes through
    if v is None:
        return None

    # Timestamp-like -> datetime.datetime
    if isinstance(v, datetime):
        # Ensure tz-naive
        if v.tzinfo is not None:
            import datetime as dt
            return v.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return v

    # Handle pandas Timestamp
    try:
        import pandas as pd
        if isinstance(v, pd.Timestamp):
            if pd.isna(v):
                return None
            # Convert to tz-naive if needed
            if v.tz is not None:
                v = v.tz_convert(None)
            return v.to_pydatetime()
    except (ImportError, AttributeError):
        pass

    # Handle numpy datetime64
    try:
        import numpy as np
        if isinstance(v, (np.datetime64,)):
            # Explicitly check for NaT first
            if np.isnat(v):
                return None
            # Convert via pandas for reliable handling
            try:
                import pandas as pd
                ts = pd.to_datetime(v, utc=False)
                if pd.isna(ts):
                    return None
                return ts.to_pydatetime()
            except Exception:
                # Fallback: convert to string and parse
                try:
                    ts_str = np.datetime_as_string(v, unit='us', timezone='naive')
                    return datetime.fromisoformat(ts_str.replace('Z', ''))
                except Exception:
                    return None
    except (ImportError, AttributeError):
        pass

    # Handle numpy numeric types -> Python types
    try:
        import numpy as np
        if isinstance(v, (np.float32, np.float64)):
            return float(v)
        if isinstance(v, (np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(v)
        if isinstance(v, np.bool_):
            return bool(v)
        # Handle generic numpy integer/floating (broader check)
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)
    except (ImportError, AttributeError):
        pass

    # Handle pandas numeric types
    try:
        import pandas as pd
        if isinstance(v, (pd.Float64Dtype, pd.Int64Dtype)):
            if pd.isna(v):
                return None
            return float(v) if isinstance(v, pd.Float64Dtype) else int(v)
    except (ImportError, AttributeError):
        pass

    # Handle bytes-like objects
    if hasattr(v, 'tobytes'):
        try:
            return v.tobytes()
        except (AttributeError, TypeError):
            pass

    # CRITICAL: Check TimestampType BEFORE general string handling
    # This ensures timestamp strings are parsed, not returned as strings
    if spark_type:
        from pyspark.sql.types import TimestampType
        if isinstance(spark_type, TimestampType):
            # If we already have a datetime, return it
            if isinstance(v, datetime):
                return v
            # If None, return None
            if v is None:
                return None
            # If string, try to parse it
            if isinstance(v, str):
                parsed = _parse_timestamp_value(v)
                if parsed is not None:
                    return parsed
                # If parsing failed, return None (Spark can handle None but not invalid types)
                return None
            # For any other type with TimestampType, return None to avoid assertion errors
            # Spark requires datetime.datetime or None, nothing else
            return None

    # String handling - ensure pure str (only for non-timestamp types)
    if isinstance(v, str):
        return v

    # Default: return as-is (assume already Python-native)
    return v


def _convert_value(value, spark_type):
    """
    Convert value to appropriate Python type based on Spark type.

    For TimestampType, returns tz-naive datetime.datetime objects.
    All values are coerced to pure Python types for Serverless compatibility.
    """
    from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType

    # Handle None/empty values
    if value is None or value == "":
        return None

    # Convert to string and strip whitespace
    try:
        str_value = str(value).strip()
    except Exception:
        return None

    if str_value == "":
        return None

    if isinstance(spark_type, TimestampType):
        # Parse to datetime.datetime (tz-naive) for Spark compatibility
        parsed_ts = _parse_timestamp_value(str_value)
        # Coerce to ensure pure Python datetime
        result = _to_python_scalar(parsed_ts, spark_type)
        # CRITICAL: Ensure result is either None or datetime.datetime
        # Spark's timestamp converter requires datetime.datetime objects
        if result is not None and not isinstance(result, datetime):
            # If somehow we got a non-datetime value, try parsing again as fallback
            if isinstance(result, str):
                result = _parse_timestamp_value(result)
            else:
                # Last resort: return None to skip this row
                return None
        return result

    elif isinstance(spark_type, StringType):
        # For string columns, normalize timestamp format if present
        if "/" in str_value:
            str_value = str_value.replace("/", "-")
        return str_value

    elif isinstance(spark_type, DoubleType):
        try:
            float_val = float(str_value)
            # Coerce to ensure pure Python float (not numpy)
            return _to_python_scalar(float_val, spark_type)
        except (ValueError, TypeError):
            return None

    elif isinstance(spark_type, IntegerType):
        try:
            int_val = int(float(str_value))
            # Coerce to ensure pure Python int (not numpy)
            return _to_python_scalar(int_val, spark_type)
        except (ValueError, TypeError):
            return None

    else:
        return str_value


def _parse_timestamp_value(ts_str: str) -> datetime:
    """
    Parse timestamp string to tz-naive datetime.datetime.

    Returns None only if parsing completely fails.
    Spark requires datetime.datetime objects for TimestampType columns.
    
    Handles both slash and dash formats, and strips quotes/whitespace.
    """
    if not ts_str:
        return None

    # Strip whitespace and quotes (csv.reader should handle quotes, but be defensive)
    ts_str = str(ts_str).strip().strip('"').strip("'").strip()
    if not ts_str:
        return None

    formats = [
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    return None


def get_nemweb_schema(table: str) -> "StructType":
    """
    Get the Spark schema for a NEMWEB table.

    Schemas are based on the MMS Electricity Data Model Report:
    https://nemweb.com.au/Reports/Current/MMSDataModelReport/

    NOTE: SETTLEMENTDATE is StringType (not TimestampType) because Spark Connect
    (Serverless) cannot serialize Python datetime objects through Arrow. Users
    should cast to timestamp in Spark SQL after loading:
        df.withColumn("SETTLEMENTDATE", to_timestamp("SETTLEMENTDATE"))

    Args:
        table: MMS table name

    Returns:
        StructType schema for the table
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType
    )

    # NOTE: SETTLEMENTDATE is StringType for Serverless compatibility
    # Cast to timestamp in Spark: to_timestamp(col("SETTLEMENTDATE"))
    schemas = {
        "DISPATCHREGIONSUM": StructType([
            StructField("SETTLEMENTDATE", StringType(), True),  # Cast later with to_timestamp()
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
            StructField("SETTLEMENTDATE", StringType(), True),  # Cast later with to_timestamp()
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
            StructField("SETTLEMENTDATE", StringType(), True),  # Cast later with to_timestamp()
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
            StructField("SETTLEMENTDATE", StringType(), True),  # Cast later with to_timestamp()
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
