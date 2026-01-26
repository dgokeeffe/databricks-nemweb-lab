"""
NEMWEB File Ingestion

Downloads NEMWEB ZIP files to a Unity Catalog Volume for subsequent processing.
Supports parallel downloads for faster ingestion.

Usage:
    from nemweb_ingest import download_nemweb_files

    # Download 6 months of data to UC Volume
    files = download_nemweb_files(
        volume_path="/Volumes/main/nemweb_lab/raw_files",
        table="DISPATCHREGIONSUM",
        start_date="2024-07-01",
        end_date="2024-12-31",
        max_workers=8
    )
"""

import os
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

# NEMWEB URLs
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
NEMWEB_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE"

# Request settings
REQUEST_TIMEOUT = 60
USER_AGENT = "DatabricksNemwebLab/1.0"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0

# Table configurations
TABLE_CONFIG = {
    "DISPATCHREGIONSUM": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
    },
    "DISPATCHPRICE": {
        "folder": "DispatchIS_Reports",
        "file_prefix": "DISPATCHIS",
    },
    "TRADINGPRICE": {
        "folder": "TradingIS_Reports",
        "file_prefix": "TRADINGIS",
    },
}


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

    # Create volume subdirectory for this table
    table_path = os.path.join(volume_path, table.lower())
    os.makedirs(table_path, exist_ok=True)

    # Generate list of dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Build download tasks
    tasks = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        filename = f"PUBLIC_{config['file_prefix']}_{date_str}.zip"
        dest_path = os.path.join(table_path, filename)

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

    print(f"NEMWEB Download: {len(tasks)} days total")
    print(f"  - To download: {len(to_download)}")
    print(f"  - Already exists: {len(skipped)}")
    print(f"  - Destination: {table_path}")
    print(f"  - Parallel workers: {max_workers}")

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
        print(f"\nDownloading {len(to_download)} files...")

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
                    print(f"  Progress: {completed}/{len(to_download)}")

    # Summary
    successful = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"] and r.get("error") != "not_found")
    not_found = sum(1 for r in results if r.get("error") == "not_found")
    total_size = sum(r["size"] for r in results)

    print(f"\nDownload complete:")
    print(f"  - Successful: {successful}")
    print(f"  - Not found (no data): {not_found}")
    print(f"  - Failed: {failed}")
    print(f"  - Total size: {total_size / 1024 / 1024:.1f} MB")

    return results


def list_downloaded_files(volume_path: str, table: str = "DISPATCHREGIONSUM") -> list[str]:
    """List all downloaded ZIP files for a table."""
    table_path = os.path.join(volume_path, table.lower())
    if not os.path.exists(table_path):
        return []

    files = [
        os.path.join(table_path, f)
        for f in os.listdir(table_path)
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
