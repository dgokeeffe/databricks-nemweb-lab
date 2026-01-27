"""
NEMWEB Streaming PySpark Data Source

This module implements a streaming data source that polls NEMWEB CURRENT folder
for new 5-minute dispatch interval files. AEMO publishes new files approximately
every 5 minutes, making this ideal for near-real-time electricity market monitoring.

The streaming source:
- Polls NEMWEB CURRENT folder for new ZIP files
- Tracks processed files using filename-based offsets
- Supports checkpointing for fault tolerance
- Works with Spark Structured Streaming

Usage:
    # Register the streaming data source
    spark.dataSource.register(NemwebStreamDataSource)

    # Read as a stream
    df = (spark.readStream
          .format("nemweb_stream")
          .option("table", "DISPATCHREGIONSUM")
          .option("regions", "NSW1,VIC1")
          .option("poll_interval_seconds", "60")
          .load())

    # Write to Delta table
    query = (df.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", "/tmp/nemweb_checkpoint")
             .table("nemweb_live"))

Requirements:
    - PySpark 4.0+ / DBR 15.4+
    - Internet access to nemweb.com.au
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from typing import Iterator, Optional
from datetime import datetime, timedelta
import logging
import re

from nemweb_utils import (
    _parse_timestamp_value,
    _to_python_datetime,
    _to_python_float,
)

logger = logging.getLogger(__name__)

# Schema definitions (same as batch version)
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

# HTTP configuration
REQUEST_TIMEOUT = 30
USER_AGENT = "DatabricksNemwebStream/1.0"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0

TABLE_TO_FOLDER = {
    "DISPATCHREGIONSUM": ("DispatchIS_Reports", "DISPATCHIS"),
    "DISPATCHPRICE": ("DispatchIS_Reports", "DISPATCHIS"),
    "TRADINGPRICE": ("TradingIS_Reports", "TRADINGIS"),
}


class NemwebStreamPartition(InputPartition):
    """Partition representing a set of files to process."""

    def __init__(self, table: str, files: list, regions: list):
        self.table = table
        self.files = files  # List of (filename, url) tuples
        self.regions = regions


class NemwebStreamReader(DataSourceStreamReader):
    """
    Streaming reader that polls NEMWEB CURRENT folder for new files.

    NEMWEB publishes 5-minute dispatch files approximately every 5 minutes.
    File naming convention: PUBLIC_DISPATCHIS_YYYYMMDDHHMM_sequence.zip

    Offset format: {"last_file": "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"}

    The reader:
    1. Lists files in CURRENT folder
    2. Filters to files newer than the last processed offset
    3. Returns new files for processing in each microbatch
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.regions = [r.strip() for r in options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")]
        self.poll_interval = int(options.get("poll_interval_seconds", "60"))
        self.max_files_per_batch = int(options.get("max_files_per_batch", "50"))

        # Get folder and file prefix for this table
        self.folder, self.file_prefix = TABLE_TO_FOLDER.get(
            self.table, ("DispatchIS_Reports", "DISPATCHIS")
        )

        # Cache for file listing (to reduce HTTP calls)
        self._file_cache = None
        self._cache_time = None
        self._cache_ttl = self.poll_interval

    def initialOffset(self) -> dict:
        """
        Returns the initial offset for the stream.

        Starts from the beginning of today's files to catch up on recent data,
        or from a specific file if configured.
        """
        start_from = self.options.get("start_from")
        if start_from:
            return {"last_file": start_from}

        # Default: start from files in the last hour
        cutoff = datetime.now() - timedelta(hours=1)
        cutoff_str = cutoff.strftime("%Y%m%d%H%M")
        return {"last_file": f"PUBLIC_{self.file_prefix}_{cutoff_str}_0000000000.zip"}

    def latestOffset(self) -> dict:
        """
        Returns the latest available offset (most recent file).

        Lists the CURRENT folder and finds the newest file.
        """
        files = self._list_current_files()

        if not files:
            # No files found, return current offset unchanged
            return {"last_file": ""}

        # Sort by filename (which includes timestamp) and get the latest
        latest_file = sorted(files)[-1]
        return {"last_file": latest_file}

    def partitions(self, start: dict, end: dict) -> list:
        """
        Plan partitions for the current microbatch.

        Finds all files between start and end offsets and creates
        a single partition containing those files.
        """
        start_file = start.get("last_file", "")
        end_file = end.get("last_file", "")

        if not end_file:
            return []

        # Get all files in range
        files = self._list_current_files()
        files_in_range = [
            f for f in sorted(files)
            if f > start_file and f <= end_file
        ]

        # Limit files per batch to avoid overwhelming the stream
        files_in_range = files_in_range[:self.max_files_per_batch]

        if not files_in_range:
            return []

        # Build URLs for each file
        file_urls = [
            (f, f"{NEMWEB_CURRENT_URL}/{self.folder}/{f}")
            for f in files_in_range
        ]

        logger.info(f"Processing {len(file_urls)} files in microbatch")

        # Create a single partition with all files
        # (could split into multiple partitions for parallelism if needed)
        return [NemwebStreamPartition(
            table=self.table,
            files=file_urls,
            regions=self.regions
        )]

    def read(self, partition: NemwebStreamPartition) -> Iterator:
        """
        Read data from the files in this partition.

        Downloads each ZIP file, extracts CSV data, and yields PyArrow RecordBatch.
        """
        import zipfile
        import io
        import time

        table_config = SCHEMAS.get(partition.table, SCHEMAS["DISPATCHREGIONSUM"])
        record_type = table_config["record_type"]
        fields = table_config["fields"]

        for filename, url in partition.files:
            try:
                # Fetch the ZIP file with retry logic
                raw_data = self._fetch_with_retry(url)
                if raw_data is None:
                    continue

                zip_data = io.BytesIO(raw_data)

                # Parse the ZIP file
                rows = []
                with zipfile.ZipFile(zip_data) as zf:
                    for name in zf.namelist():
                        if name.upper().endswith(".CSV"):
                            with zf.open(name) as csv_file:
                                rows.extend(self._parse_csv(csv_file, record_type))

                # Filter by regions
                if partition.regions:
                    rows = [r for r in rows if r.get("REGIONID") in partition.regions]

                # Yield tuples with pure Python types for Serverless compatibility
                for row in rows:
                    result = self._row_to_tuple(row, fields)
                    if result is not None:
                        yield result

            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                # Continue to next file rather than failing the batch
                continue

    def commit(self, end: dict) -> None:
        """
        Called when a microbatch has been successfully processed.

        Can be used to clean up resources or update external state.
        """
        logger.info(f"Committed offset: {end}")

    def _list_current_files(self) -> list:
        """
        List ZIP files in the CURRENT folder for this table.

        Uses a cache to reduce HTTP calls.
        """
        import time
        from urllib.request import urlopen, Request
        from urllib.error import HTTPError, URLError

        # Check cache
        now = time.time()
        if self._file_cache is not None and self._cache_time is not None:
            if now - self._cache_time < self._cache_ttl:
                return self._file_cache

        # Fetch directory listing
        url = f"{NEMWEB_CURRENT_URL}/{self.folder}/"

        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                html = response.read().decode('utf-8')
        except (HTTPError, URLError) as e:
            logger.error(f"Failed to list CURRENT directory: {e}")
            return self._file_cache or []

        # Parse filenames from HTML directory listing
        # Pattern: PUBLIC_DISPATCHIS_YYYYMMDDHHMM_sequence.zip
        pattern = rf'(PUBLIC_{self.file_prefix}_\d{{12}}_\d+\.zip)'
        files = list(set(re.findall(pattern, html)))

        # Update cache
        self._file_cache = files
        self._cache_time = now

        return files

    def _fetch_with_retry(self, url: str) -> Optional[bytes]:
        """Fetch URL with retry logic."""
        import time
        from urllib.request import urlopen, Request
        from urllib.error import HTTPError, URLError

        for attempt in range(MAX_RETRIES):
            try:
                request = Request(url, headers={"User-Agent": USER_AGENT})
                with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                    return response.read()
            except HTTPError as e:
                if e.code == 404:
                    logger.warning(f"File not found: {url}")
                    return None
                logger.warning(f"HTTP error {e.code} fetching {url}, attempt {attempt + 1}")
            except (URLError, TimeoutError) as e:
                logger.warning(f"Error fetching {url}: {e}, attempt {attempt + 1}")

            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))

        return None

    def _parse_csv(self, csv_file, record_type: str) -> list:
        """Parse NEMWEB multi-record CSV format."""
        import csv
        import io

        text = csv_file.read().decode("utf-8")

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

    def _row_to_tuple(self, row: dict, fields: list) -> tuple:
        """
        Convert a row dict to a tuple with proper type conversion.

        Uses utilities from nemweb_utils for type coercion to ensure
        Serverless Arrow fast path compatibility.

        Returns None if required timestamp field cannot be parsed.
        """
        values = []
        for name, spark_type in fields:
            raw_val = row.get(name)

            if isinstance(spark_type, TimestampType):
                parsed_ts = _parse_timestamp_value(raw_val)
                if parsed_ts is None and name == "SETTLEMENTDATE":
                    return None
                values.append(_to_python_datetime(parsed_ts))
            elif isinstance(spark_type, DoubleType):
                values.append(_to_python_float(raw_val))
            else:
                values.append(str(raw_val) if raw_val else None)

        return tuple(values)


class NemwebStreamDataSource(DataSource):
    """
    Streaming PySpark Data Source for NEMWEB electricity market data.

    Polls the NEMWEB CURRENT folder for new 5-minute dispatch files
    and processes them as a stream.

    Usage:
        spark.dataSource.register(NemwebStreamDataSource)

        # Read as a stream
        df = (spark.readStream
              .format("nemweb_stream")
              .option("table", "DISPATCHREGIONSUM")
              .option("regions", "NSW1,VIC1")
              .load())

        # Process the stream
        query = (df.writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", "/tmp/nemweb_checkpoint")
                 .table("nemweb_live"))

    Options:
        table: MMS table name (DISPATCHREGIONSUM, DISPATCHPRICE, TRADINGPRICE)
        regions: Comma-separated region IDs (default: all 5 NEM regions)
        poll_interval_seconds: How often to check for new files (default: 60)
        max_files_per_batch: Maximum files to process per microbatch (default: 50)
        start_from: Specific filename to start from (optional)
    """

    @classmethod
    def name(cls) -> str:
        return "nemweb_stream"

    def schema(self) -> StructType:
        """Return schema for the requested table."""
        table = self.options.get("table", "DISPATCHREGIONSUM")
        table_config = SCHEMAS.get(table, SCHEMAS["DISPATCHREGIONSUM"])

        return StructType([
            StructField(name, dtype, True)
            for name, dtype in table_config["fields"]
        ])

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """Return a stream reader for this data source."""
        return NemwebStreamReader(schema, self.options)
