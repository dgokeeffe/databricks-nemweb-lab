"""
NEMWEB Unified PySpark Data Source (Batch + Streaming)

This module implements a unified data source supporting both batch reads
(spark.read) and streaming reads (spark.readStream) for all supported NEMWEB
tables. Schemas, constants, and parsing logic are imported from nemweb_utils
to avoid duplication with the Arrow data source.

Batch mode:
- Fetches daily ARCHIVE files for a date range
- One partition per date for parallel processing
- Supports optional region filtering

Streaming mode:
- Polls NEMWEB CURRENT folder for new 5-minute dispatch interval files
- Tracks processed files using filename-based offsets
- Supports checkpointing for fault tolerance

Usage:
    spark.dataSource.register(NemwebStreamDataSource)

    # Batch read (all 8 tables supported)
    df = (spark.read.format("nemweb_stream")
          .option("table", "DISPATCHREGIONSUM")
          .option("start_date", "2024-01-01")
          .option("end_date", "2024-01-07")
          .option("regions", "NSW1,VIC1")
          .load())

    # Streaming read
    df = (spark.readStream
          .format("nemweb_stream")
          .option("table", "DISPATCHREGIONSUM")
          .option("regions", "NSW1,VIC1")
          .option("poll_interval_seconds", "60")
          .load())

    # Write stream to Delta table
    query = (df.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", "/tmp/nemweb_checkpoint")
             .table("catalog.schema.dispatch_live"))

Requirements:
    - PySpark 4.0+ / DBR 15.4+
    - Internet access to nemweb.com.au
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType
from typing import Iterator, Optional
from datetime import datetime, timedelta
import io
import logging
import bisect

from nemweb_utils import (
    SCHEMAS,
    TABLE_CONFIG,
    NEMWEB_CURRENT_URL,
    NEMWEB_ARCHIVE_URL,
    USER_AGENT,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    fetch_with_retry,
    extract_rows_from_zip,
    list_current_files,
    _parse_timestamp_value,
    _to_python_datetime,
    _to_python_float,
)

logger = logging.getLogger(__name__)

# Field names that support region filtering
_REGION_FIELDS = {"REGIONID"}


def _has_region_field(table: str) -> bool:
    """Check if a table has a REGIONID field (supports region filtering)."""
    if table not in SCHEMAS:
        return False
    field_names = {name for name, _ in SCHEMAS[table]["fields"]}
    return bool(field_names & _REGION_FIELDS)


def _row_to_tuple(row: dict, fields: list) -> Optional[tuple]:
    """
    Convert a row dict to a tuple with proper type conversion.

    Uses utilities from nemweb_utils for type coercion to ensure
    Serverless Arrow fast path compatibility.

    Returns None if a required timestamp field (SETTLEMENTDATE or
    INTERVAL_DATETIME) cannot be parsed.
    """
    required_ts_fields = {"SETTLEMENTDATE", "INTERVAL_DATETIME"}
    values = []
    for name, spark_type in fields:
        raw_val = row.get(name)

        if isinstance(spark_type, TimestampType):
            parsed_ts = _parse_timestamp_value(raw_val)
            if parsed_ts is None and name in required_ts_fields:
                return None
            values.append(_to_python_datetime(parsed_ts))
        elif isinstance(spark_type, DoubleType):
            values.append(_to_python_float(raw_val))
        else:
            values.append(str(raw_val) if raw_val else None)

    return tuple(values)


class NemwebStreamPartition(InputPartition):
    """Partition representing a set of files to process (streaming)."""

    def __init__(self, table: str, files: list, regions: list):
        self.table = table
        self.files = files  # List of (filename, url) tuples
        self.regions = regions


class NemwebBatchPartition(InputPartition):
    """Partition representing a single date to fetch (batch)."""

    def __init__(self, table: str, date: str, regions: list):
        self.table = table
        self.date = date  # YYYY-MM-DD
        self.regions = regions


class NemwebBatchReader(DataSourceReader):
    """
    Batch reader that fetches daily ARCHIVE files for a date range.

    Creates one partition per date for parallel processing. Each partition
    downloads its daily ZIP file, extracts and parses CSV data, applies
    optional region filtering, and yields tuples.
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.start_date = options.get("start_date", "2024-01-01")
        self.end_date = options.get("end_date", "2024-01-07")

        regions_str = options.get("regions", "")
        if regions_str:
            self.regions = [r.strip() for r in regions_str.split(",")]
        else:
            self.regions = []

    def partitions(self) -> list[InputPartition]:
        """Create one partition per date in the range."""
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")

        partitions = []
        current = start
        while current <= end:
            partitions.append(NemwebBatchPartition(
                table=self.table,
                date=current.strftime("%Y-%m-%d"),
                regions=self.regions,
            ))
            current += timedelta(days=1)

        logger.info(f"Created {len(partitions)} batch partitions for {self.table}")
        return partitions

    def read(self, partition: NemwebBatchPartition) -> Iterator[tuple]:
        """Fetch a daily ARCHIVE ZIP and yield parsed tuples."""
        from urllib.error import HTTPError

        config = TABLE_CONFIG.get(partition.table)
        if not config:
            logger.error(f"No TABLE_CONFIG for {partition.table}")
            return

        schema_config = SCHEMAS.get(partition.table)
        if not schema_config:
            logger.error(f"No SCHEMAS entry for {partition.table}")
            return

        record_type = schema_config["record_type"]
        fields = schema_config["fields"]

        # Build ARCHIVE URL for this date
        date_obj = datetime.strptime(partition.date, "%Y-%m-%d")
        date_str = date_obj.strftime("%Y%m%d")
        file_suffix = config.get("file_suffix", "")
        if file_suffix:
            # Legacy files have a different archive naming pattern
            filename = f"PUBLIC_{config['file_prefix']}_{date_str}.zip"
        else:
            filename = f"PUBLIC_{config['file_prefix']}_{date_str}.zip"
        url = f"{NEMWEB_ARCHIVE_URL}/{config['folder']}/{filename}"

        try:
            raw_data = fetch_with_retry(url)
        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"No archive data for {partition.table} on {partition.date}")
                return
            raise
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return

        zip_data = io.BytesIO(raw_data)
        rows = extract_rows_from_zip(zip_data, record_type)

        # Apply region filter only for tables that have REGIONID
        if partition.regions and _has_region_field(partition.table):
            region_set = set(partition.regions)
            rows = [r for r in rows if r.get("REGIONID") in region_set]

        for row in rows:
            result = _row_to_tuple(row, fields)
            if result is not None:
                yield result


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
        self.startup_catchup_hours = int(options.get("startup_catchup_hours", "24"))

        # Get folder and file prefix for this table from TABLE_CONFIG
        config = TABLE_CONFIG.get(self.table)
        if config:
            self.folder = config["folder"]
            # Some tables have a different naming convention in CURRENT vs ARCHIVE
            # (e.g., ROOFTOP_PV_ACTUAL uses "ROOFTOP_PV_ACTUAL_SATELLITE" in CURRENT).
            # streaming_prefix overrides file_prefix for directory listing.
            self.file_prefix = config.get("streaming_prefix", config["file_prefix"])
            self.file_suffix = config.get("file_suffix", "")
        else:
            self.folder = "DispatchIS_Reports"
            self.file_prefix = "DISPATCHIS"
            self.file_suffix = ""

        # Cache for file listing (to reduce HTTP calls)
        self._file_cache = None
        self._cache_time = None
        self._cache_ttl = self.poll_interval
        self._last_committed_file = options.get("start_from", "")

    def initialOffset(self) -> dict:
        """
        Returns the initial offset for the stream.

        Starts from the beginning of today's files to catch up on recent data,
        or from a specific file if configured.

        NEMWEB filenames use AEST (Australian Eastern Standard Time, UTC+10)
        timestamps, so we must compute the cutoff in AEST regardless of the
        driver's system timezone (which is UTC on Databricks).
        """
        start_from = self.options.get("start_from")
        if start_from:
            self._last_committed_file = start_from
            return {"last_file": start_from}

        # Default: catch up from recent files (in AEST / NEM time).
        # This reduces startup gaps while keeping bounded replay behavior.
        from datetime import timezone
        aest = timezone(timedelta(hours=10))
        now_aest = datetime.now(aest)
        cutoff = now_aest - timedelta(hours=self.startup_catchup_hours)
        cutoff_str = cutoff.strftime("%Y%m%d%H%M")
        initial = {"last_file": f"PUBLIC_{self.file_prefix}_{cutoff_str}_0000000000.zip"}
        self._last_committed_file = initial["last_file"]
        return initial

    def latestOffset(self) -> dict:
        """
        Returns the latest available offset (most recent file).

        Lists the CURRENT folder and finds the newest file.
        """
        files = self._list_current_files()

        if not files:
            return {"last_file": ""}

        files_sorted = sorted(files)
        start_file = self._last_committed_file
        if not start_file:
            latest_file = files_sorted[-1]
            return {"last_file": latest_file}

        # Use bisect to safely handle when checkpointed file is no longer in CURRENT.
        start_idx = bisect.bisect_right(files_sorted, start_file)
        newer_files = files_sorted[start_idx:]
        if not newer_files:
            return {"last_file": start_file}

        bounded_newer = newer_files[:self.max_files_per_batch]
        return {"last_file": bounded_newer[-1]}

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

        # Safety guard: latestOffset should already bound this range.
        if len(files_in_range) > self.max_files_per_batch:
            files_in_range = files_in_range[:self.max_files_per_batch]
            end_file = files_in_range[-1]
            end["last_file"] = end_file

        if not files_in_range:
            return []

        # Build URLs for each file
        file_urls = [
            (f, f"{NEMWEB_CURRENT_URL}/{self.folder}/{f}")
            for f in files_in_range
        ]

        logger.info(f"Processing {len(file_urls)} files in microbatch")

        return [NemwebStreamPartition(
            table=self.table,
            files=file_urls,
            regions=self.regions
        )]

    def read(self, partition: NemwebStreamPartition) -> Iterator:
        """
        Read data from the files in this partition.

        Downloads each ZIP file, extracts CSV data, and yields tuples.
        """
        schema_config = SCHEMAS.get(partition.table, SCHEMAS["DISPATCHREGIONSUM"])
        record_type = schema_config["record_type"]
        fields = schema_config["fields"]
        can_filter_region = _has_region_field(partition.table)

        for filename, url in partition.files:
            try:
                raw_data = self._fetch_with_retry(url)
                if raw_data is None:
                    continue

                zip_data = io.BytesIO(raw_data)
                rows = extract_rows_from_zip(zip_data, record_type)

                # Filter by regions only for tables with REGIONID
                if partition.regions and can_filter_region:
                    region_set = set(partition.regions)
                    rows = [r for r in rows if r.get("REGIONID") in region_set]

                for row in rows:
                    result = _row_to_tuple(row, fields)
                    if result is not None:
                        yield result

            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                continue

    def commit(self, end: dict) -> None:
        """Called when a microbatch has been successfully processed."""
        self._last_committed_file = end.get("last_file", self._last_committed_file)
        logger.info(f"Committed offset: {end}")

    def _list_current_files(self) -> list:
        """
        List ZIP files in the CURRENT folder for this table.

        Uses a cache to reduce HTTP calls. Delegates to
        nemweb_utils.list_current_files() for the actual HTTP fetch.
        """
        import time

        now = time.time()
        if self._file_cache is not None and self._cache_time is not None:
            if now - self._cache_time < self._cache_ttl:
                return self._file_cache

        files = list_current_files(self.folder, self.file_prefix, self.file_suffix)

        self._file_cache = files
        self._cache_time = now

        return files

    def _fetch_with_retry(self, url: str) -> Optional[bytes]:
        """Fetch URL with retry logic, returning None on 404."""
        from urllib.error import HTTPError

        try:
            return fetch_with_retry(url)
        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"File not found: {url}")
                return None
            logger.warning(f"HTTP error {e.code} fetching {url}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


class NemwebStreamDataSource(DataSource):
    """
    Unified PySpark Data Source for NEMWEB electricity market data.

    Supports both batch reads (spark.read) and streaming reads (spark.readStream)
    for all 8 supported NEMWEB tables.

    Usage:
        spark.dataSource.register(NemwebStreamDataSource)

        # Batch read
        df = (spark.read.format("nemweb_stream")
              .option("table", "DISPATCH_UNIT_SCADA")
              .option("start_date", "2024-01-01")
              .option("end_date", "2024-01-07")
              .load())

        # Streaming read
        df = (spark.readStream
              .format("nemweb_stream")
              .option("table", "DISPATCHREGIONSUM")
              .option("regions", "NSW1,VIC1")
              .load())

    Supported tables:
        DISPATCHREGIONSUM, DISPATCHPRICE, TRADINGPRICE, DISPATCH_UNIT_SCADA,
        ROOFTOP_PV_ACTUAL, DISPATCH_REGION, DISPATCH_INTERCONNECTOR,
        DISPATCH_INTERCONNECTOR_TRADING

    Options:
        table: MMS table name (default: DISPATCHREGIONSUM)
        regions: Comma-separated region IDs (only applied to tables with REGIONID)
        start_date: Start date YYYY-MM-DD (batch only)
        end_date: End date YYYY-MM-DD (batch only)
        poll_interval_seconds: How often to check for new files (streaming, default: 60)
        max_files_per_batch: Maximum files per microbatch (streaming, default: 50)
        start_from: Specific filename to start from (streaming, optional)
        startup_catchup_hours: Catch-up window when no start_from/checkpoint is present
                              (streaming, default: 24)
    """

    @classmethod
    def name(cls) -> str:
        return "nemweb_stream"

    def schema(self) -> StructType:
        """Return schema for the requested table."""
        table = self.options.get("table", "DISPATCHREGIONSUM")
        schema_config = SCHEMAS.get(table, SCHEMAS["DISPATCHREGIONSUM"])

        return StructType([
            StructField(name, dtype, True)
            for name, dtype in schema_config["fields"]
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        """Return a batch reader for spark.read."""
        return NemwebBatchReader(schema, self.options)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """Return a stream reader for spark.readStream."""
        return NemwebStreamReader(schema, self.options)
