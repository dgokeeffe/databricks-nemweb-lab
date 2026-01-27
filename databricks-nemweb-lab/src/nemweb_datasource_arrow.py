"""
NEMWEB Custom PySpark Data Source - Arrow Version

This module implements a custom data source using PyArrow RecordBatch
for zero-copy transfer to Spark. This avoids Python datetime serialization
issues that occur with Spark Connect (Serverless).

Supports three modes:
1. Volume mode: Read from pre-downloaded files in UC Volume (fastest)
2. Auto-download mode: Download to volume first, then read (recommended)
3. HTTP mode: Fetch directly via HTTP (for development/small date ranges)

Usage:
    # Register the data source
    spark.dataSource.register(NemwebArrowDataSource)

    # Auto-download to Volume then read (recommended for production)
    df = (spark.read.format("nemweb_arrow")
          .option("volume_path", "/Volumes/main/nemweb/raw")
          .option("table", "DISPATCHREGIONSUM")
          .option("start_date", "2024-07-01")
          .option("end_date", "2024-12-31")
          .option("auto_download", "true")
          .load())

    # Read from pre-downloaded files (if already downloaded)
    df = (spark.read.format("nemweb_arrow")
          .option("volume_path", "/Volumes/main/nemweb/raw")
          .option("table", "DISPATCHREGIONSUM")
          .load())

    # Read via HTTP directly (for development/testing)
    df = (spark.read.format("nemweb_arrow")
          .option("table", "DISPATCHREGIONSUM")
          .option("start_date", "2024-01-01")
          .option("end_date", "2024-01-07")
          .option("regions", "NSW1,VIC1")
          .load())

Requirements:
    - PySpark 4.0+ / DBR 15.4+
    - PyArrow (included in Databricks runtimes)
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from typing import Iterator, Optional
from datetime import datetime, timedelta
import hashlib
import logging

logger = logging.getLogger(__name__)


# Schema definitions for each supported table
SCHEMAS = {
    "DISPATCHREGIONSUM": {
        "record_type": "DISPATCH,REGIONSUM",
        "fields": [
            ("SETTLEMENTDATE", TimestampType()),
            ("RUNNO", StringType()),
            ("REGIONID", StringType()),
            ("DISPATCHINTERVAL", StringType()),
            ("INTERVENTION", StringType()),
            ("TOTALDEMAND", DoubleType()),
            ("AVAILABLEGENERATION", DoubleType()),
            ("AVAILABLELOAD", DoubleType()),
            ("DEMANDFORECAST", DoubleType()),
            ("DISPATCHABLEGENERATION", DoubleType()),
            ("DISPATCHABLELOAD", DoubleType()),
            ("NETINTERCHANGE", DoubleType()),
        ],
    },
    "DISPATCHPRICE": {
        "record_type": "DISPATCH,PRICE",
        "fields": [
            ("SETTLEMENTDATE", TimestampType()),
            ("RUNNO", StringType()),
            ("REGIONID", StringType()),
            ("DISPATCHINTERVAL", StringType()),
            ("INTERVENTION", StringType()),
            ("RRP", DoubleType()),
            ("EEP", DoubleType()),
            ("ROP", DoubleType()),
            ("APCFLAG", StringType()),
            ("MARKETSUSPENDEDFLAG", StringType()),
        ],
    },
    "TRADINGPRICE": {
        "record_type": "TRADING,PRICE",
        "fields": [
            ("SETTLEMENTDATE", TimestampType()),
            ("RUNNO", StringType()),
            ("REGIONID", StringType()),
            ("PERIODID", StringType()),
            ("RRP", DoubleType()),
            ("EEP", DoubleType()),
            ("INVALIDFLAG", StringType()),
        ],
    },
}

# URL configuration
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
NEMWEB_ARCHIVE_URL = "https://www.nemweb.com.au/REPORTS/ARCHIVE"

# Download configuration
REQUEST_TIMEOUT = 60
USER_AGENT = "DatabricksNemwebLab/2.0"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0

TABLE_TO_FOLDER = {
    "DISPATCHREGIONSUM": ("DispatchIS_Reports", "DISPATCHIS"),
    "DISPATCHPRICE": ("DispatchIS_Reports", "DISPATCHIS"),
    "TRADINGPRICE": ("TradingIS_Reports", "TRADINGIS"),
}


class NemwebArrowPartition(InputPartition):
    """Partition for Arrow data source."""

    def __init__(
        self,
        table: str,
        region: Optional[str] = None,
        date: Optional[str] = None,
        file_path: Optional[str] = None,
    ):
        self.table = table
        self.region = region
        self.date = date
        self.file_path = file_path

        # Generate deterministic partition ID
        if file_path:
            id_string = file_path
        else:
            id_string = f"{table}:{region or 'all'}:{date}"
        self.partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]


class NemwebArrowReader(DataSourceReader):
    """
    Arrow-based reader that returns PyArrow RecordBatch objects.

    This bypasses Python datetime serialization issues by using Arrow's
    native timestamp handling for zero-copy transfer to Spark.

    Supports auto-download mode where files are downloaded to a UC Volume
    before reading. Downloads run on the driver using ThreadPoolExecutor
    for parallelism, then reading happens distributed across Spark workers.
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.volume_path = options.get("volume_path")
        self.regions = [r.strip() for r in options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")]
        self.start_date = options.get("start_date", "2024-01-01")
        self.end_date = options.get("end_date", "2024-01-07")

        # Download options
        self.auto_download = options.get("auto_download", "false").lower() == "true"
        self.max_workers = int(options.get("max_workers", "8"))
        self.skip_existing = options.get("skip_existing", "true").lower() == "true"
        # Include recent data from CURRENT (5-minute interval files)
        self.include_current = options.get("include_current", "false").lower() == "true"

    def partitions(self) -> list[InputPartition]:
        """Create partitions based on mode (volume, auto-download, or HTTP)."""
        if self.volume_path:
            # Auto-download files to volume if enabled
            if self.auto_download:
                self._download_to_volume()
            return self._volume_partitions()
        else:
            return self._http_partitions()

    def _volume_partitions(self) -> list[InputPartition]:
        """Create one partition per ZIP file in volume."""
        import os

        # Use file prefix (dispatchis, tradingis) as folder name, not table name
        # This allows DISPATCHREGIONSUM and DISPATCHPRICE to share the same files
        _, file_prefix = TABLE_TO_FOLDER.get(self.table, ("DispatchIS_Reports", "DISPATCHIS"))
        table_path = os.path.join(self.volume_path, file_prefix.lower())

        if not os.path.exists(table_path):
            logger.warning(f"Volume path does not exist: {table_path}")
            return []

        files = sorted([
            os.path.join(table_path, f)
            for f in os.listdir(table_path)
            if f.endswith('.zip')
        ])

        logger.info(f"Found {len(files)} ZIP files in {table_path}")

        return [
            NemwebArrowPartition(table=self.table, file_path=f)
            for f in files
        ]

    def _http_partitions(self) -> list[InputPartition]:
        """Create one partition per region+date for HTTP fetching."""
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")

        partitions = []
        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            for region in self.regions:
                partitions.append(NemwebArrowPartition(
                    table=self.table,
                    region=region,
                    date=date_str
                ))
            current += timedelta(days=1)

        logger.info(f"Created {len(partitions)} HTTP partitions")
        return partitions

    def _download_to_volume(self) -> None:
        """
        Download NEMWEB files to UC Volume in parallel.

        Runs on the driver using ThreadPoolExecutor. Downloads are skipped
        for files that already exist when skip_existing is True.
        """
        import os
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        folder, file_prefix = TABLE_TO_FOLDER.get(
            self.table, ("DispatchIS_Reports", "DISPATCHIS")
        )

        # Use file prefix (dispatchis, tradingis) as folder name, not table name
        # This allows DISPATCHREGIONSUM and DISPATCHPRICE to share the same downloaded files
        table_path = os.path.join(self.volume_path, file_prefix.lower())
        os.makedirs(table_path, exist_ok=True)

        # Generate download tasks for date range
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")

        print(f"Generating download tasks for {self.start_date} to {self.end_date}")
        print(f"Target folder: {table_path}")

        # Note: Daily consolidated files are only available in ARCHIVE after ~7 days
        # Recent dates will return 404 since CURRENT only has 5-minute interval files
        archive_cutoff = datetime.now() - timedelta(days=7)

        tasks = []
        skipped_recent = 0
        current = start
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            filename = f"PUBLIC_{file_prefix}_{date_str}.zip"
            dest_path = os.path.join(table_path, filename)

            # Skip dates that are too recent (not yet in ARCHIVE)
            if current > archive_cutoff:
                skipped_recent += 1
                current += timedelta(days=1)
                continue

            if self.skip_existing and os.path.exists(dest_path):
                tasks.append({
                    "date": current,
                    "url": None,
                    "dest_path": dest_path,
                    "skip": True
                })
            else:
                url = self._build_download_url(folder, file_prefix, current)
                tasks.append({
                    "date": current,
                    "url": url,
                    "dest_path": dest_path,
                    "skip": False
                })

            current += timedelta(days=1)

        if skipped_recent > 0:
            if self.include_current:
                print(f"Note: {skipped_recent} recent days will be fetched from CURRENT (5-min intervals)")
            else:
                print(f"Note: Skipping {skipped_recent} recent days (< 7 days old, not yet in ARCHIVE)")
                print("      Use .option('include_current', 'true') to download recent 5-minute interval files")

        to_download = [t for t in tasks if not t["skip"]]
        skipped = len([t for t in tasks if t["skip"]])

        # Print progress for user visibility (logger might not be visible in notebooks)
        print(f"NEMWEB Download: {len(tasks)} days total, {len(to_download)} to download, {skipped} existing/skipped")

        # Show sample URLs for verification
        if to_download:
            sample = to_download[0]
            print(f"Sample URL: {sample['url']}")

        logger.info(
            f"NEMWEB Download: {len(tasks)} days, "
            f"{len(to_download)} to download, {skipped} existing"
        )

        if not to_download:
            print("All files already exist, skipping download.")
            return

        # Download files in parallel
        results = {"success": 0, "failed": 0, "not_found": 0}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(
                    self._download_single_file,
                    task["url"],
                    task["dest_path"]
                ): task
                for task in to_download
            }

            for future in as_completed(future_to_task):
                result = future.result()
                if result["success"]:
                    results["success"] += 1
                elif result.get("error") == "not_found":
                    results["not_found"] += 1
                else:
                    results["failed"] += 1

        # Print results for user visibility
        print(
            f"Download complete: {results['success']} successful, "
            f"{results['not_found']} not found (404), {results['failed']} failed"
        )
        if results['not_found'] > 0:
            print(
                f"  Note: {results['not_found']} files returned 404 - "
                "these dates may not yet be in ARCHIVE (data takes ~7 days to consolidate)"
            )

        logger.info(
            f"Download complete: {results['success']} successful, "
            f"{results['not_found']} not found, {results['failed']} failed"
        )

        # Download recent files from CURRENT if enabled
        if self.include_current and skipped_recent > 0:
            self._download_from_current(folder, file_prefix, table_path, archive_cutoff, end)

    def _download_from_current(
        self,
        folder: str,
        file_prefix: str,
        table_path: str,
        start_date: datetime,
        end_date: datetime
    ) -> None:
        """
        Download recent 5-minute interval files from CURRENT.

        CURRENT contains individual dispatch interval files (every 5 minutes)
        that haven't been consolidated into daily archives yet.
        """
        import os
        import re
        from urllib.request import urlopen, Request
        from urllib.error import HTTPError, URLError
        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"\nDownloading recent data from CURRENT ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})...")

        # Fetch directory listing from CURRENT
        current_url = f"{NEMWEB_CURRENT_URL}/{folder}/"
        try:
            request = Request(current_url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                html = response.read().decode('utf-8')
        except (HTTPError, URLError) as e:
            print(f"  Failed to list CURRENT directory: {e}")
            return

        # Parse filenames from HTML directory listing
        # Pattern: PUBLIC_DISPATCHIS_YYYYMMDDHHMM_sequence.zip
        pattern = rf'PUBLIC_{file_prefix}_(\d{{12}})_\d+\.zip'
        matches = re.findall(pattern, html)

        # Filter to files within our date range
        target_dates = set()
        current = start_date
        while current <= end_date:
            target_dates.add(current.strftime("%Y%m%d"))
            current += timedelta(days=1)

        # Find files matching our date range
        files_to_download = []
        seen_files = set()
        for match in matches:
            date_part = match[:8]  # YYYYMMDD from YYYYMMDDHHMM
            if date_part in target_dates:
                # Reconstruct full filename
                full_match = re.search(rf'(PUBLIC_{file_prefix}_{match}_\d+\.zip)', html)
                if full_match:
                    filename = full_match.group(1)
                    if filename not in seen_files:
                        seen_files.add(filename)
                        dest_path = os.path.join(table_path, filename)
                        if not (self.skip_existing and os.path.exists(dest_path)):
                            files_to_download.append({
                                "url": f"{NEMWEB_CURRENT_URL}/{folder}/{filename}",
                                "dest_path": dest_path
                            })

        if not files_to_download:
            print("  No new CURRENT files to download")
            return

        print(f"  Found {len(files_to_download)} files to download from CURRENT")

        # Download in parallel
        results = {"success": 0, "failed": 0}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(
                    self._download_single_file,
                    task["url"],
                    task["dest_path"]
                ): task
                for task in files_to_download
            }

            for future in as_completed(future_to_task):
                result = future.result()
                if result["success"]:
                    results["success"] += 1
                else:
                    results["failed"] += 1

        print(f"  CURRENT download complete: {results['success']} successful, {results['failed']} failed")

    def _download_single_file(self, url: str, dest_path: str) -> dict:
        """Download a single file with retry logic."""
        import time
        from urllib.request import urlopen, Request
        from urllib.error import HTTPError, URLError

        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                request = Request(url, headers={"User-Agent": USER_AGENT})
                with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                    data = response.read()

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

            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)

        return {
            "success": False,
            "url": url,
            "path": dest_path,
            "size": 0,
            "error": last_error
        }

    def _build_download_url(self, folder: str, file_prefix: str, date: datetime) -> str:
        """
        Build NEMWEB URL for downloading a file.

        Note: Daily consolidated files (PUBLIC_DISPATCHIS_YYYYMMDD.zip) are only
        available in ARCHIVE. The CURRENT folder only has 5-minute interval files.
        So we always use ARCHIVE for daily downloads.
        """
        date_str = date.strftime("%Y%m%d")
        filename = f"PUBLIC_{file_prefix}_{date_str}.zip"

        # Daily consolidated files are only in ARCHIVE
        # (CURRENT has 5-minute interval files, not daily consolidated)
        return f"{NEMWEB_ARCHIVE_URL}/{folder}/{filename}"

    def read(self, partition: NemwebArrowPartition) -> Iterator:
        """
        Read data and yield PyArrow RecordBatch objects.

        Routes to appropriate reader based on partition type.
        """
        if partition.file_path:
            yield from self._read_volume_file(partition)
        else:
            yield from self._read_http(partition)

    def _read_volume_file(self, partition: NemwebArrowPartition) -> Iterator:
        """Read from local ZIP file in volume."""
        import zipfile
        import io

        table_config = SCHEMAS.get(partition.table, SCHEMAS["DISPATCHREGIONSUM"])
        record_type = table_config["record_type"]
        arrow_schema = self._build_arrow_schema(table_config["fields"])

        try:
            with open(partition.file_path, 'rb') as f:
                zip_data = io.BytesIO(f.read())

            rows = []
            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    # Handle nested ZIPs
                    if name.lower().endswith(".zip"):
                        with zf.open(name) as nested_zip_file:
                            nested_data = io.BytesIO(nested_zip_file.read())
                            with zipfile.ZipFile(nested_data) as nested_zf:
                                for nested_name in nested_zf.namelist():
                                    if nested_name.upper().endswith(".CSV"):
                                        with nested_zf.open(nested_name) as csv_file:
                                            rows.extend(self._parse_csv(csv_file, record_type))

                    # Direct CSV files
                    elif name.upper().endswith(".CSV"):
                        with zf.open(name) as csv_file:
                            rows.extend(self._parse_csv(csv_file, record_type))

            # Filter by regions if specified
            if self.regions and rows:
                rows = [r for r in rows if r.get("REGIONID") in self.regions]

            if rows:
                batch = self._rows_to_record_batch(rows, table_config["fields"], arrow_schema)
                yield batch

        except Exception as e:
            logger.error(f"Error reading {partition.file_path}: {e}")
            raise

    def _read_http(self, partition: NemwebArrowPartition) -> Iterator:
        """Fetch data via HTTP and return as RecordBatch."""
        import zipfile
        import io
        from urllib.request import urlopen, Request
        from urllib.error import HTTPError

        table_config = SCHEMAS.get(partition.table, SCHEMAS["DISPATCHREGIONSUM"])
        record_type = table_config["record_type"]
        arrow_schema = self._build_arrow_schema(table_config["fields"])

        try:
            url = self._build_url(partition.table, partition.date)
            logger.debug(f"Fetching: {url}")

            request = Request(url, headers={"User-Agent": "DatabricksLab/1.0"})
            with urlopen(request, timeout=30) as response:
                raw_data = response.read()

            zip_data = io.BytesIO(raw_data)

            rows = []
            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    # Handle nested ZIPs
                    if name.lower().endswith(".zip"):
                        with zf.open(name) as nested_zip_file:
                            nested_data = io.BytesIO(nested_zip_file.read())
                            with zipfile.ZipFile(nested_data) as nested_zf:
                                for nested_name in nested_zf.namelist():
                                    if nested_name.upper().endswith(".CSV"):
                                        with nested_zf.open(nested_name) as csv_file:
                                            rows.extend(self._parse_csv(csv_file, record_type))

                    # Direct CSV files
                    elif name.upper().endswith(".CSV"):
                        with zf.open(name) as csv_file:
                            rows.extend(self._parse_csv(csv_file, record_type))

            # Filter by region
            if partition.region:
                rows = [r for r in rows if r.get("REGIONID") == partition.region]

            if rows:
                batch = self._rows_to_record_batch(rows, table_config["fields"], arrow_schema)
                yield batch

        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"No data for {partition.date}")
            else:
                raise
        except Exception as e:
            logger.error(f"Error fetching {partition.table}/{partition.date}: {e}")
            raise

    def _build_url(self, table: str, date_str: str) -> str:
        """
        Build NEMWEB URL for the specified table and date.

        Note: Daily consolidated files are only in ARCHIVE.
        CURRENT has 5-minute interval files, not daily consolidated.
        """
        folder, prefix = TABLE_TO_FOLDER.get(table, ("DispatchIS_Reports", "DISPATCHIS"))
        date = datetime.strptime(date_str, "%Y-%m-%d")
        date_formatted = date.strftime("%Y%m%d")
        filename = f"PUBLIC_{prefix}_{date_formatted}.zip"

        # Daily consolidated files are only in ARCHIVE
        return f"{NEMWEB_ARCHIVE_URL}/{folder}/{filename}"

    def _build_arrow_schema(self, fields: list) -> "pa.Schema":
        """Build PyArrow schema from field definitions."""
        import pyarrow as pa

        type_map = {
            TimestampType: pa.timestamp('us'),
            StringType: pa.string(),
            DoubleType: pa.float64(),
        }

        arrow_fields = []
        for name, spark_type in fields:
            arrow_type = type_map.get(type(spark_type), pa.string())
            arrow_fields.append(pa.field(name, arrow_type))

        return pa.schema(arrow_fields)

    def _rows_to_record_batch(self, rows: list, fields: list, arrow_schema: "pa.Schema") -> "pa.RecordBatch":
        """Convert parsed rows to PyArrow RecordBatch."""
        import pyarrow as pa

        # Initialize column arrays
        columns = {name: [] for name, _ in fields}

        for row in rows:
            for name, spark_type in fields:
                raw_val = row.get(name)

                if isinstance(spark_type, TimestampType):
                    columns[name].append(self._parse_timestamp(raw_val))
                elif isinstance(spark_type, DoubleType):
                    columns[name].append(self._to_float(raw_val))
                else:
                    columns[name].append(raw_val if raw_val else None)

        # Create Arrow arrays
        arrays = []
        for name, spark_type in fields:
            if isinstance(spark_type, TimestampType):
                arrays.append(pa.array(columns[name], type=pa.timestamp('us')))
            elif isinstance(spark_type, DoubleType):
                arrays.append(pa.array(columns[name], type=pa.float64()))
            else:
                arrays.append(pa.array(columns[name], type=pa.string()))

        return pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)

    def _parse_csv(self, csv_file, record_type: str) -> list[dict]:
        """Parse NEMWEB multi-record CSV format."""
        import csv
        import io

        text = csv_file.read().decode("utf-8")

        if not record_type:
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)

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
                    rows.append(row_dict)

        return rows

    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """Parse NEMWEB timestamp to Python datetime."""
        if not ts_str:
            return None

        ts_str = str(ts_str).strip()
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

    def _to_float(self, val) -> Optional[float]:
        """Convert string to float."""
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None


class NemwebArrowDataSource(DataSource):
    """
    Arrow-based PySpark Data Source for NEMWEB electricity market data.

    Uses PyArrow RecordBatch for zero-copy transfer, avoiding
    Python datetime serialization issues on Spark Connect (Serverless).

    Supports multiple NEMWEB tables:
    - DISPATCHREGIONSUM: Regional demand and generation summary
    - DISPATCHPRICE: 5-minute dispatch prices
    - TRADINGPRICE: 30-minute trading prices

    Usage:
        spark.dataSource.register(NemwebArrowDataSource)

        # Auto-download to Volume then read (recommended)
        df = (spark.read.format("nemweb_arrow")
              .option("volume_path", "/Volumes/main/nemweb/raw")
              .option("table", "DISPATCHREGIONSUM")
              .option("start_date", "2024-07-01")
              .option("end_date", "2024-12-31")
              .option("auto_download", "true")
              .load())

        # Read from Volume (if files already downloaded)
        df = (spark.read.format("nemweb_arrow")
              .option("volume_path", "/Volumes/main/nemweb/raw")
              .option("table", "DISPATCHREGIONSUM")
              .load())

        # Read via HTTP directly (for development/testing)
        df = (spark.read.format("nemweb_arrow")
              .option("table", "DISPATCHPRICE")
              .option("start_date", "2024-01-01")
              .option("end_date", "2024-01-07")
              .option("regions", "NSW1,VIC1")
              .load())

        # Include recent data (last 7 days from CURRENT)
        df = (spark.read.format("nemweb_arrow")
              .option("volume_path", "/Volumes/main/nemweb/raw")
              .option("table", "DISPATCHREGIONSUM")
              .option("start_date", "2024-12-01")
              .option("end_date", "2024-12-31")
              .option("auto_download", "true")
              .option("include_current", "true")  # Also fetch recent 5-min interval files
              .load())

    Options:
        volume_path: Path to UC Volume for storing/reading files
        table: MMS table name (DISPATCHREGIONSUM, DISPATCHPRICE, TRADINGPRICE)
        regions: Comma-separated region IDs (default: all 5 NEM regions)
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        auto_download: If "true", download files to volume before reading (default: false)
        max_workers: Number of parallel download threads (default: 8)
        skip_existing: If "true", skip downloading files that exist (default: true)
        include_current: If "true", also download recent 5-minute interval files from
                        CURRENT for dates not yet in ARCHIVE (default: false)
    """

    @classmethod
    def name(cls) -> str:
        return "nemweb_arrow"

    def schema(self) -> StructType:
        """Return schema for the requested table."""
        table = self.options.get("table", "DISPATCHREGIONSUM")
        table_config = SCHEMAS.get(table, SCHEMAS["DISPATCHREGIONSUM"])

        return StructType([
            StructField(name, dtype, True)
            for name, dtype in table_config["fields"]
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        return NemwebArrowReader(schema, self.options)
