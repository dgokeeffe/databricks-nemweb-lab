#!/usr/bin/env python3
"""
NEMWEB Standalone Data Loader

A simple, dependency-free utility for fetching AEMO NEMWEB market data.
Works with Python 3.9+ using only the standard library.

Usage:
    # Python API
    from nemweb import fetch, list_tables, NEM_REGIONS
    data = fetch("DISPATCHPRICE", days=1)

    # CLI
    python nemweb.py DISPATCHPRICE --days 1
    python nemweb.py --list-tables

NEMWEB Data Structure:
    - Current reports: https://www.nemweb.com.au/REPORTS/CURRENT/
    - Archive reports: https://www.nemweb.com.au/REPORTS/ARCHIVE/
    - File format: CSV within ZIP archives
"""

__version__ = "1.0.0"

import argparse
import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Configure logging
logger = logging.getLogger(__name__)

# NEM region identifiers
NEM_REGIONS = ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]

# NEMWEB base URLs
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
NEMWEB_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE"

# Request configuration
REQUEST_TIMEOUT = 30
USER_AGENT = "NEMWEBStandalone/1.0"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0

# Table configuration - maps table names to NEMWEB folder paths and record types
TABLE_CONFIG = {
    "DISPATCHREGIONSUM": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
        "record_type": "DISPATCH,REGIONSUM",
        "description": "Regional demand, generation, interchange (5-min)",
    },
    "DISPATCHPRICE": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
        "record_type": "DISPATCH,PRICE",
        "description": "Regional spot prices - RRP (5-min)",
    },
    "TRADINGPRICE": {
        "folder": "TradingIS_Reports",
        "file_prefix": "TRADINGIS",
        "record_type": "TRADING,PRICE",
        "description": "Trading period prices (30-min)",
    },
    "DISPATCH_UNIT_SCADA": {
        "folder": "Dispatch_SCADA",
        "file_prefix": "DISPATCHSCADA",
        "record_type": "DISPATCH,UNIT_SCADA",
        "description": "Real-time unit generation per DUID (5-min)",
    },
    "ROOFTOP_PV_ACTUAL": {
        "folder": "ROOFTOP_PV/ACTUAL",
        "file_prefix": "ROOFTOP_PV_ACTUAL",
        "record_type": None,
        "description": "Rooftop solar generation estimates",
    },
    "DISPATCH_REGION": {
        "folder": "Dispatch_Reports",
        "file_prefix": "DISPATCH",
        "file_suffix": "_LEGACY",
        "record_type": "DREGION,",
        "description": "Comprehensive dispatch with FCAS prices (5-min)",
    },
    "DISPATCH_INTERCONNECTOR": {
        "folder": "Dispatch_Reports",
        "file_prefix": "DISPATCH",
        "file_suffix": "_LEGACY",
        "record_type": "DINT,",
        "description": "Interconnector dispatch details (5-min)",
    },
    "DISPATCH_INTERCONNECTOR_TRADING": {
        "folder": "Dispatch_Reports",
        "file_prefix": "DISPATCH",
        "file_suffix": "_LEGACY",
        "record_type": "TINT,",
        "description": "Metered interconnector flows (5-min)",
    },
}


def list_tables() -> list[dict]:
    """
    List available NEMWEB tables.

    Returns:
        List of dicts with 'name' and 'description' keys
    """
    return [
        {"name": name, "description": config.get("description", "")}
        for name, config in TABLE_CONFIG.items()
    ]


def fetch(
    table: str,
    *,
    days: Optional[int] = None,
    hours: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    regions: Optional[list[str]] = None,
    max_files: int = 288,  # 24 hours of 5-min files
    sample: bool = False,
    as_pandas: bool = False,
    as_polars: bool = False,
) -> list[dict]:
    """
    Fetch NEMWEB data for the specified table.

    Args:
        table: Table name (e.g., "DISPATCHPRICE")
        days: Number of days of data (uses ARCHIVE)
        hours: Number of hours of recent data (uses CURRENT, default 1 if nothing specified)
        start_date: Start date YYYY-MM-DD (uses ARCHIVE)
        end_date: End date YYYY-MM-DD (uses ARCHIVE)
        regions: Filter by regions (e.g., ["NSW1", "VIC1"])
        max_files: Max files when using CURRENT (default: 288 = 24 hours)
        sample: Return sample data (no network)
        as_pandas: Return pandas DataFrame (requires pandas)
        as_polars: Return polars DataFrame (requires polars)

    Returns:
        List of dicts (or DataFrame if as_pandas/as_polars)

    Examples:
        # Last hour of prices
        data = fetch("DISPATCHPRICE", hours=1)

        # Last 7 days from archive
        data = fetch("DISPATCHREGIONSUM", days=7)

        # Specific date range
        data = fetch("DISPATCHPRICE", start_date="2024-01-01", end_date="2024-01-07")

        # Sample data (offline)
        data = fetch("DISPATCHPRICE", sample=True)
    """
    if table not in TABLE_CONFIG:
        raise ValueError(f"Unknown table: {table}. Use list_tables() to see available tables.")

    # Determine fetch mode
    if sample:
        data = _get_sample_data(table, regions)
    elif start_date or end_date or days:
        # Use ARCHIVE for date ranges
        if days:
            end = datetime.now()
            start = end - timedelta(days=days)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
        elif not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        elif not start_date:
            start_date = end_date

        data = _fetch_archive(table, start_date, end_date, regions)
    else:
        # Use CURRENT for recent data (default: 1 hour = 12 files)
        if hours:
            max_files = hours * 12  # 12 five-minute intervals per hour
        elif max_files == 288:
            max_files = 12  # Default to 1 hour

        data = _fetch_current(table, max_files, regions)

    # Convert to DataFrame if requested
    if as_pandas:
        try:
            import pandas as pd
            return pd.DataFrame(data)
        except ImportError:
            raise ImportError("pandas is required for as_pandas=True")

    if as_polars:
        try:
            import polars as pl
            return pl.DataFrame(data)
        except ImportError:
            raise ImportError("polars is required for as_polars=True")

    return data


def _fetch_with_retry(url: str) -> bytes:
    """Fetch URL with exponential backoff retry."""
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()

        except (HTTPError, URLError) as e:
            last_error = e

            # Don't retry on 404
            if isinstance(e, HTTPError) and e.code == 404:
                raise

            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retrying in {delay:.1f}s...")
                time.sleep(delay)

    raise last_error


def _fetch_archive(
    table: str,
    start_date: str,
    end_date: str,
    regions: Optional[list[str]] = None,
) -> list[dict]:
    """Fetch from ARCHIVE (daily consolidated files)."""
    config = TABLE_CONFIG[table]
    record_type = config.get("record_type")
    rows = []

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    while current <= end:
        date_str = current.strftime("%Y%m%d")
        filename = f"PUBLIC_{config['file_prefix']}_{date_str}.zip"
        url = f"{NEMWEB_ARCHIVE_URL}/{config['folder']}/{filename}"

        try:
            data = _fetch_and_extract_zip(url, record_type)
            if regions:
                data = [r for r in data if r.get("REGIONID") in regions]
            rows.extend(data)
            logger.info(f"Fetched {len(data)} rows for {current.date()}")
        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"No data for {current.date()}")
            else:
                raise

        current += timedelta(days=1)

    return rows


def _fetch_current(
    table: str,
    max_files: int,
    regions: Optional[list[str]] = None,
) -> list[dict]:
    """Fetch from CURRENT (recent 5-minute interval files)."""
    config = TABLE_CONFIG[table]
    folder = config["folder"]
    file_prefix = config["file_prefix"]
    file_suffix = config.get("file_suffix", "")
    record_type = config.get("record_type")

    # Get directory listing
    current_url = f"{NEMWEB_CURRENT_URL}/{folder}/"
    request = Request(current_url, headers={"User-Agent": USER_AGENT})

    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        html = response.read().decode("utf-8")

    # Find ZIP files matching our pattern
    if file_suffix:
        pattern = rf'(PUBLIC_{file_prefix}_\d{{12}}_\d{{14}}{file_suffix}\.zip)'
    else:
        pattern = rf'(PUBLIC_{file_prefix}_\d{{12}}_\d+\.zip)'

    matches = re.findall(pattern, html, re.IGNORECASE)

    if not matches:
        logger.warning(f"No files found for {table} in CURRENT")
        return []

    # Get most recent files
    unique_files = sorted(set(matches), reverse=True)[:max_files]
    logger.info(f"Found {len(unique_files)} files to fetch")

    rows = []
    for filename in unique_files:
        url = f"{NEMWEB_CURRENT_URL}/{folder}/{filename}"
        try:
            data = _fetch_and_extract_zip(url, record_type)
            if regions:
                data = [r for r in data if r.get("REGIONID") in regions]
            rows.extend(data)
        except HTTPError as e:
            logger.warning(f"Failed to fetch {filename}: {e}")

    return rows


def _fetch_and_extract_zip(url: str, record_type: Optional[str] = None) -> list[dict]:
    """Fetch ZIP file and extract CSV data."""
    raw_data = _fetch_with_retry(url)
    zip_data = io.BytesIO(raw_data)

    rows = []
    with zipfile.ZipFile(zip_data) as zf:
        for name in zf.namelist():
            # Handle nested ZIPs
            if name.lower().endswith(".zip"):
                with zf.open(name) as nested_file:
                    nested_data = io.BytesIO(nested_file.read())
                    with zipfile.ZipFile(nested_data) as nested_zf:
                        for nested_name in nested_zf.namelist():
                            if nested_name.lower().endswith(".csv"):
                                with nested_zf.open(nested_name) as csv_file:
                                    rows.extend(_parse_csv(csv_file, record_type))

            elif name.lower().endswith(".csv"):
                with zf.open(name) as csv_file:
                    rows.extend(_parse_csv(csv_file, record_type))

    return rows


def _parse_csv(csv_file, record_type: Optional[str] = None) -> list[dict]:
    """
    Parse NEMWEB CSV file.

    NEMWEB uses a multi-record format:
        C,... = Comment row
        I,CATEGORY,RECORD_TYPE,VERSION,COL1,COL2,... = Header
        D,CATEGORY,RECORD_TYPE,VERSION,VAL1,VAL2,... = Data
    """
    text = csv_file.read().decode("utf-8")

    if not record_type:
        # Standard CSV format
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    # NEMWEB multi-record format
    rows = []
    headers = None
    reader = csv.reader(io.StringIO(text))

    for parts in reader:
        if not parts:
            continue

        row_type = parts[0].strip().upper()

        if row_type == "I" and len(parts) > 2:
            row_record = f"{parts[1]},{parts[2]}"
            if row_record == record_type:
                headers = parts[4:]

        elif row_type == "D" and headers and len(parts) > 2:
            row_record = f"{parts[1]},{parts[2]}"
            if row_record == record_type:
                values = parts[4:]
                row_dict = dict(zip(headers, values))
                for header in headers[len(values):]:
                    row_dict[header] = None
                rows.append(row_dict)

    return rows


def _get_sample_data(table: str, regions: Optional[list[str]] = None) -> list[dict]:
    """Return sample data for offline testing."""
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
            "RRP": "85.50",
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
            "RRP": "72.30",
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
            "RRP": "95.20",
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
            "RRP": "110.80",
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
            "RRP": "68.40",
        },
    ]

    if regions:
        sample = [r for r in sample if r["REGIONID"] in regions]

    return sample


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch AEMO NEMWEB market data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s DISPATCHPRICE --hours 1
  %(prog)s DISPATCHPRICE --days 7 --regions NSW1,VIC1
  %(prog)s DISPATCHREGIONSUM --start 2024-01-01 --end 2024-01-07
  %(prog)s --list-tables
  %(prog)s DISPATCHPRICE --sample
        """,
    )

    parser.add_argument("table", nargs="?", help="Table name (e.g., DISPATCHPRICE)")
    parser.add_argument("--list-tables", action="store_true", help="List available tables")
    parser.add_argument("--hours", type=int, help="Hours of recent data (uses CURRENT)")
    parser.add_argument("--days", type=int, help="Days of data (uses ARCHIVE)")
    parser.add_argument("--start", dest="start_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", dest="end_date", help="End date YYYY-MM-DD")
    parser.add_argument("--regions", help="Comma-separated regions (e.g., NSW1,VIC1)")
    parser.add_argument("--sample", action="store_true", help="Use sample data (offline)")
    parser.add_argument("--output", "-o", help="Output file (CSV or JSON based on extension)")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="Output format (default: json)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING)

    # Handle --list-tables
    if args.list_tables:
        tables = list_tables()
        print("\nAvailable NEMWEB tables:\n")
        for t in tables:
            print(f"  {t['name']:<30} {t['description']}")
        print(f"\nRegions: {', '.join(NEM_REGIONS)}")
        return

    # Require table name
    if not args.table:
        parser.error("table name is required (or use --list-tables)")

    # Parse regions
    regions = args.regions.split(",") if args.regions else None

    # Fetch data
    try:
        data = fetch(
            args.table,
            hours=args.hours,
            days=args.days,
            start_date=args.start_date,
            end_date=args.end_date,
            regions=regions,
            sample=args.sample,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except (HTTPError, URLError) as e:
        print(f"Network error: {e}", file=sys.stderr)
        sys.exit(1)

    if not data:
        print("No data found", file=sys.stderr)
        sys.exit(0)

    # Determine output format
    output_format = args.format
    if args.output:
        if args.output.endswith(".csv"):
            output_format = "csv"
        elif args.output.endswith(".json"):
            output_format = "json"

    # Generate output
    if output_format == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        content = output.getvalue()
    else:
        content = json.dumps(data, indent=2)

    # Write output
    if args.output:
        with open(args.output, "w") as f:
            f.write(content)
        print(f"Wrote {len(data)} rows to {args.output}", file=sys.stderr)
    else:
        print(content)


if __name__ == "__main__":
    main()
