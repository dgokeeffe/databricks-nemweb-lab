"""
NEMWEB File Ingestion

Downloads NEMWEB ZIP files to a Unity Catalog Volume and extracts CSVs for
Auto Loader streaming. Supports parallel downloads for faster ingestion.

Usage:
    from nemweb_ingest import download_nemweb_files, extract_to_csv

    # Download 7 days of data to UC Volume
    files = download_nemweb_files(
        volume_path="/Volumes/main/ml_workshops/raw_files",
        table="DISPATCHREGIONSUM",
        start_date="2024-01-01",
        end_date="2024-01-07",
        max_workers=8
    )

    # Extract CSVs for Auto Loader
    extract_to_csv(
        zip_path="/Volumes/main/ml_workshops/raw_files",
        csv_path="/Volumes/main/ml_workshops/extracted_csv",
        table="DISPATCHREGIONSUM"
    )
"""

import os
import io
import zipfile
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

from nemweb_utils import (
    TABLE_CONFIG,
    NEMWEB_CURRENT_URL,
    NEMWEB_ARCHIVE_URL,
    USER_AGENT,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)

logger = logging.getLogger(__name__)


def get_supported_tables() -> list[str]:
    """Return list of supported NEMWEB table names."""
    return list(TABLE_CONFIG.keys())


def _build_url(folder: str, file_prefix: str, date: datetime) -> str:
    """Build NEMWEB URL for the given date."""
    days_ago = (datetime.now() - date).days
    date_str = date.strftime("%Y%m%d")
    filename = f"PUBLIC_{file_prefix}_{date_str}.zip"

    if days_ago < 7:
        return f"{NEMWEB_CURRENT_URL}/{folder}/{filename}"
    else:
        return f"{NEMWEB_ARCHIVE_URL}/{folder}/{filename}"


def _download_file(url: str, dest_path: str, max_retries: int = MAX_RETRIES) -> dict:
    """
    Download a single file with retry logic.

    Returns:
        dict with 'success', 'url', 'path', 'size', 'error'
    """
    import time

    last_error = None

    for attempt in range(max_retries):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                data = response.read()

            # Write to destination
            with open(dest_path, 'wb') as f:
                f.write(data)

            return {
                "success": True,
                "url": url,
                "path": dest_path,
                "size": len(data),
                "error": None
            }

        except HTTPError as e:
            if e.code == 404:
                # File doesn't exist - not an error, just no data for this date
                return {
                    "success": False,
                    "url": url,
                    "path": dest_path,
                    "size": 0,
                    "error": "not_found"
                }
            last_error = str(e)

        except (URLError, TimeoutError) as e:
            last_error = str(e)

        if attempt < max_retries - 1:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            time.sleep(delay)

    return {
        "success": False,
        "url": url,
        "path": dest_path,
        "size": 0,
        "error": last_error
    }


def download_nemweb_files(
    volume_path: str,
    table: str = "DISPATCHREGIONSUM",
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-07",
    max_workers: int = 8,
    skip_existing: bool = True
) -> list[dict]:
    """
    Download NEMWEB files to a UC Volume in parallel.

    Args:
        volume_path: Path to UC Volume (e.g., /Volumes/catalog/schema/volume_name)
        table: NEMWEB table name (DISPATCHREGIONSUM, DISPATCHPRICE, etc.)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        max_workers: Number of parallel download threads
        skip_existing: Skip files that already exist in the volume

    Returns:
        List of result dicts with download status for each file
    """
    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}. Supported: {list(TABLE_CONFIG.keys())}")

    config = TABLE_CONFIG[table]

    # Create volume subdirectory using file_prefix (groups related tables together)
    prefix_path = os.path.join(volume_path, config["file_prefix"].lower())
    os.makedirs(prefix_path, exist_ok=True)

    # Generate list of dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Build download tasks
    tasks = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        filename = f"PUBLIC_{config['file_prefix']}_{date_str}.zip"
        dest_path = os.path.join(prefix_path, filename)

        # Skip if file exists and skip_existing is True
        if skip_existing and os.path.exists(dest_path):
            tasks.append({
                "date": current,
                "url": None,
                "dest_path": dest_path,
                "skip": True
            })
        else:
            url = _build_url(config["folder"], config["file_prefix"], current)
            tasks.append({
                "date": current,
                "url": url,
                "dest_path": dest_path,
                "skip": False
            })

        current += timedelta(days=1)

    # Count skipped vs to-download
    to_download = [t for t in tasks if not t["skip"]]
    skipped = [t for t in tasks if t["skip"]]

    print(f"NEMWEB Download [{table}]: {len(tasks)} days total")
    print(f"  - To download: {len(to_download)}")
    print(f"  - Already exists: {len(skipped)}")
    print(f"  - Destination: {prefix_path}")

    results = []

    # Add skipped files to results
    for task in skipped:
        results.append({
            "success": True,
            "url": "skipped",
            "path": task["dest_path"],
            "size": os.path.getsize(task["dest_path"]),
            "error": None,
            "skipped": True
        })

    # Download files in parallel
    if to_download:
        print(f"  Downloading {len(to_download)} files...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(_download_file, task["url"], task["dest_path"]): task
                for task in to_download
            }

            completed = 0
            for future in as_completed(future_to_task):
                completed += 1
                result = future.result()
                result["skipped"] = False
                results.append(result)

                # Progress indicator
                if completed % 10 == 0 or completed == len(to_download):
                    print(f"    Progress: {completed}/{len(to_download)}")

    # Summary
    successful = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"] and r.get("error") != "not_found")
    not_found = sum(1 for r in results if r.get("error") == "not_found")
    total_size = sum(r["size"] for r in results)

    print(f"  Complete: {successful} ok, {not_found} not found, {failed} failed ({total_size / 1024 / 1024:.1f} MB)")

    return results


def download_all_tables(
    volume_path: str,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-07",
    max_workers: int = 8,
    skip_existing: bool = True,
    tables: Optional[list[str]] = None
) -> dict[str, list[dict]]:
    """
    Download all NEMWEB tables to a UC Volume.

    Args:
        volume_path: Path to UC Volume
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        max_workers: Number of parallel download threads
        skip_existing: Skip files that already exist
        tables: List of tables to download (default: all)

    Returns:
        Dict mapping table name to list of download results
    """
    if tables is None:
        tables = list(TABLE_CONFIG.keys())

    results = {}
    for table in tables:
        results[table] = download_nemweb_files(
            volume_path=volume_path,
            table=table,
            start_date=start_date,
            end_date=end_date,
            max_workers=max_workers,
            skip_existing=skip_existing
        )

    return results


def _extract_records(csv_file, record_type: str) -> list[str]:
    """Extract rows matching the specified record type from a NEMWEB CSV."""
    import codecs

    rows = []
    reader = codecs.getreader('utf-8')(csv_file)
    header = None

    for line in reader:
        line = line.strip()
        if not line:
            continue

        parts = line.split(',')
        if len(parts) < 3:
            continue

        row_type = parts[0]
        record_id = f"{parts[1]},{parts[2]}" if len(parts) > 2 else ""

        # Capture header for this record type
        if row_type == "I" and record_id == record_type:
            header = line
        # Capture data rows
        elif row_type == "D" and record_id == record_type:
            if header and not rows:
                # Include header as first row
                rows.append(header)
            rows.append(line)

    return rows


def extract_to_csv(
    zip_path: str,
    csv_path: str,
    table: str = "DISPATCHREGIONSUM",
    skip_existing: bool = True
) -> dict:
    """
    Extract and filter CSV records from downloaded ZIP files.

    Reads ZIP files from zip_path, extracts records matching the table's
    record_type, and writes them to csv_path for Auto Loader streaming.

    Args:
        zip_path: Path to downloaded ZIP files (e.g., /Volumes/.../raw_files)
        csv_path: Path to write extracted CSVs (e.g., /Volumes/.../extracted_csv)
        table: NEMWEB table name
        skip_existing: Skip CSV files that already exist

    Returns:
        Dict with extraction summary
    """
    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}. Supported: {list(TABLE_CONFIG.keys())}")

    config = TABLE_CONFIG[table]
    record_type = config["record_type"]

    # Source: ZIP files are stored by file_prefix
    source_path = os.path.join(zip_path, config["file_prefix"].lower())
    # Destination: CSVs are stored by table name
    dest_path = os.path.join(csv_path, table.lower())
    os.makedirs(dest_path, exist_ok=True)

    if not os.path.exists(source_path):
        print(f"Source path does not exist: {source_path}")
        return {"table": table, "extracted": 0, "skipped": 0, "total_rows": 0}

    # Find ZIP files
    zip_files = [f for f in os.listdir(source_path) if f.endswith('.zip')]

    extracted = 0
    skipped = 0
    total_rows = 0

    for zip_filename in sorted(zip_files):
        # Extract date from filename (PUBLIC_DISPATCHIS_YYYYMMDD.zip)
        parts = zip_filename.replace('.zip', '').split('_')
        if len(parts) >= 3:
            date_str = parts[-1]
        else:
            continue

        csv_filename = f"{table.lower()}_{date_str}.csv"
        csv_filepath = os.path.join(dest_path, csv_filename)

        # Skip if already extracted
        if skip_existing and os.path.exists(csv_filepath):
            skipped += 1
            continue

        # Read and extract from ZIP
        zip_filepath = os.path.join(source_path, zip_filename)
        try:
            with open(zip_filepath, 'rb') as f:
                zip_data = io.BytesIO(f.read())

            rows = []
            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    # Handle nested ZIPs
                    if name.lower().endswith('.zip'):
                        with zf.open(name) as nested_zip_file:
                            nested_data = io.BytesIO(nested_zip_file.read())
                            with zipfile.ZipFile(nested_data) as nested_zf:
                                for nested_name in nested_zf.namelist():
                                    if nested_name.upper().endswith('.CSV'):
                                        with nested_zf.open(nested_name) as csv_file:
                                            rows.extend(_extract_records(csv_file, record_type))
                    # Direct CSV files
                    elif name.upper().endswith('.CSV'):
                        with zf.open(name) as csv_file:
                            rows.extend(_extract_records(csv_file, record_type))

            # Write extracted CSV
            if rows:
                with open(csv_filepath, 'w') as out_f:
                    for row in rows:
                        out_f.write(row + '\n')
                extracted += 1
                total_rows += len(rows)

        except Exception as e:
            logger.warning(f"Error extracting {zip_filename}: {e}")

    print(f"Extract [{table}]: {extracted} files, {skipped} skipped, {total_rows} rows")
    return {"table": table, "extracted": extracted, "skipped": skipped, "total_rows": total_rows}


def extract_all_tables(
    zip_path: str,
    csv_path: str,
    tables: Optional[list[str]] = None,
    skip_existing: bool = True
) -> dict[str, dict]:
    """
    Extract CSVs for all NEMWEB tables.

    Args:
        zip_path: Path to downloaded ZIP files
        csv_path: Path to write extracted CSVs
        tables: List of tables to extract (default: all)
        skip_existing: Skip CSV files that already exist

    Returns:
        Dict mapping table name to extraction summary
    """
    if tables is None:
        tables = list(TABLE_CONFIG.keys())

    results = {}
    for table in tables:
        results[table] = extract_to_csv(
            zip_path=zip_path,
            csv_path=csv_path,
            table=table,
            skip_existing=skip_existing
        )

    return results


def list_downloaded_files(volume_path: str, table: str = "DISPATCHREGIONSUM") -> list[str]:
    """List all downloaded ZIP files for a table."""
    if table not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table}")

    config = TABLE_CONFIG[table]
    prefix_path = os.path.join(volume_path, config["file_prefix"].lower())

    if not os.path.exists(prefix_path):
        return []

    files = [
        os.path.join(prefix_path, f)
        for f in os.listdir(prefix_path)
        if f.endswith('.zip')
    ]
    return sorted(files)


def get_date_range_from_files(volume_path: str, table: str = "DISPATCHREGIONSUM") -> tuple[Optional[str], Optional[str]]:
    """Get the date range of downloaded files."""
    files = list_downloaded_files(volume_path, table)
    if not files:
        return None, None

    # Extract dates from filenames (PUBLIC_DISPATCHIS_YYYYMMDD.zip)
    dates = []
    for f in files:
        basename = os.path.basename(f)
        # Extract YYYYMMDD from filename
        parts = basename.replace('.zip', '').split('_')
        if len(parts) >= 3:
            try:
                date_str = parts[-1]
                date = datetime.strptime(date_str, "%Y%m%d")
                dates.append(date)
            except ValueError:
                continue

    if not dates:
        return None, None

    min_date = min(dates).strftime("%Y-%m-%d")
    max_date = max(dates).strftime("%Y-%m-%d")

    return min_date, max_date
