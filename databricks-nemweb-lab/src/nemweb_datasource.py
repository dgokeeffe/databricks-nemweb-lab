"""
NEMWEB Custom PySpark Data Source

This module implements a custom data source for AEMO NEMWEB electricity market data
using Databricks' Python Data Source API (GA in DBR 15.4+/Spark 4.0).

Usage:
    # Batch reading
    spark.dataSource.register(NemwebDataSource)
    df = spark.read.format("nemweb").option("table", "DISPATCHREGIONSUM").load()

    # Batch with checkpoint-based resumability
    df = (spark.read.format("nemweb")
          .option("checkpoint_table", "main.nemweb.checkpoints")
          .option("start_date", "2024-01-01")
          .option("end_date", "2024-06-30")
          .load())

    # Write to target table
    df.write.mode("append").saveAsTable("main.nemweb.bronze")

    # Update checkpoints after successful write (REQUIRED for resumability)
    from nemweb_datasource import update_checkpoint, get_partition_ids
    partition_ids = get_partition_ids("2024-01-01", "2024-06-30", ["NSW1","VIC1","QLD1","SA1","TAS1"])
    update_checkpoint(spark, "main.nemweb.checkpoints", partition_ids)

    # Streaming with automatic checkpointing (Spark manages offsets)
    df = spark.readStream.format("nemweb").load()

References:
    - Python Data Source API: https://docs.databricks.com/en/pyspark/datasources.html
    - MMS Data Model: https://nemweb.com.au/Reports/Current/MMSDataModelReport/
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import StructType
from typing import Iterator, Tuple, Optional
from datetime import datetime, timedelta
import hashlib
import logging

# Support both package and standalone imports
try:
    from .nemweb_utils import fetch_nemweb_data, parse_nemweb_csv, get_nemweb_schema
    from .nemweb_ingest import list_downloaded_files
except ImportError:
    from nemweb_utils import fetch_nemweb_data, parse_nemweb_csv, get_nemweb_schema
    from nemweb_ingest import list_downloaded_files

logger = logging.getLogger(__name__)


def update_checkpoint(
    spark,
    checkpoint_table: str,
    partition_ids: list[str],
    metadata: dict = None
) -> None:
    """
    Update checkpoint table after successful batch write.

    The Python Data Source API doesn't have a callback for partition completion,
    so checkpoints must be updated by the pipeline after successful writes.

    Usage:
        # Read with checkpoint filtering
        df = (spark.read.format("nemweb")
              .option("checkpoint_table", "main.nemweb.checkpoints")
              .option("start_date", "2024-01-01")
              .option("end_date", "2024-06-30")
              .load())

        # Write to target
        df.write.mode("append").saveAsTable("main.nemweb.bronze")

        # Update checkpoints after successful write
        from nemweb_datasource import update_checkpoint, get_partition_ids
        partition_ids = get_partition_ids("2024-01-01", "2024-06-30", ["NSW1", "VIC1"])
        update_checkpoint(spark, "main.nemweb.checkpoints", partition_ids)

    Args:
        spark: SparkSession
        checkpoint_table: Fully qualified table name (catalog.schema.table)
        partition_ids: List of partition IDs that were successfully processed
        metadata: Optional dict with additional columns (e.g., {"job_id": "123"})
    """
    from pyspark.sql.functions import current_timestamp, lit

    # Ensure checkpoint table exists
    _ensure_batch_checkpoint_table(spark, checkpoint_table)

    # Create checkpoint records
    records = [{"partition_id": pid} for pid in partition_ids]
    df = spark.createDataFrame(records)
    df = df.withColumn("completed_at", current_timestamp())

    # Add optional metadata columns
    if metadata:
        for key, value in metadata.items():
            df = df.withColumn(key, lit(value))

    # Create temp view for MERGE source
    df.createOrReplaceTempView("_checkpoint_updates")

    # MERGE: upsert all partition IDs in one operation
    # This is idempotent - re-running updates completed_at but doesn't duplicate
    merge_sql = f"""
        MERGE INTO {checkpoint_table} AS target
        USING _checkpoint_updates AS source
        ON target.partition_id = source.partition_id
        WHEN MATCHED THEN
            UPDATE SET completed_at = source.completed_at
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)
    logger.info(f"Updated checkpoint with {len(partition_ids)} partition(s)")


def _ensure_batch_checkpoint_table(spark, checkpoint_table: str) -> None:
    """Create batch checkpoint table if it doesn't exist."""
    if not spark.catalog.tableExists(checkpoint_table):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {checkpoint_table} (
                partition_id STRING NOT NULL,
                completed_at TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
        """)


def get_partition_ids(
    start_date: str,
    end_date: str,
    regions: list[str],
    table: str = "DISPATCHREGIONSUM"
) -> list[str]:
    """
    Generate partition IDs for a date range and regions.

    Use this to get the IDs that should be marked as complete after a successful write.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        regions: List of region IDs
        table: MMS table name

    Returns:
        List of partition ID strings
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    partition_ids = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        for region in regions:
            id_string = f"{table}:{region}:{date_str}"
            partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]
            partition_ids.append(partition_id)
        current += timedelta(days=1)

    return partition_ids


class NemwebPartition(InputPartition):
    """
    Represents a partition of NEMWEB data to be read.

    Each partition corresponds to a specific region and single date,
    enabling parallel reads across Spark executors.

    Best Practice: Fine-grained partitions (one per region+day) allow Spark
    to maximize parallelism. With 5 regions and 180 days, we get 900 partitions
    that can execute concurrently across available executor cores.

    Attributes:
        partition_id: Unique identifier for checkpoint tracking
        region: NEM region ID (NSW1, VIC1, etc.)
        date: Single date in YYYY-MM-DD format
        table: MMS table name
    """

    def __init__(
        self,
        region: str,
        date: str,
        table: str,
        partition_id: Optional[str] = None
    ):
        self.region = region
        self.date = date
        self.table = table

        # Generate deterministic partition ID for checkpointing
        if partition_id:
            self.partition_id = partition_id
        else:
            id_string = f"{table}:{region}:{date}"
            self.partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]


class NemwebDataSourceReader(DataSourceReader):
    """
    Reader implementation for NEMWEB data source.

    Handles partition planning and data fetching from NEMWEB HTTP endpoints.
    Creates fine-grained partitions (one per region+day) for maximum parallelism.

    Options:
        table: MMS table name (default: DISPATCHREGIONSUM)
        regions: Comma-separated region IDs (default: all 5 NEM regions)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        checkpoint_table: Delta table to track completed partitions
        skip_completed: Whether to skip already-completed partitions (default: True)
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.regions = [r.strip() for r in options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")]
        self.start_date = options.get("start_date", "2024-01-01")
        self.end_date = options.get("end_date", "2024-01-07")

        # Checkpoint options
        self.checkpoint_table = options.get("checkpoint_table")
        self.skip_completed = options.get("skip_completed", "true").lower() == "true"

    def _generate_date_range(self, start_date: str, end_date: str) -> list[str]:
        """Generate list of dates between start and end (inclusive)."""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        return dates

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        Creates one partition per (region, date) combination for maximum parallelism.
        This allows Spark to fetch multiple days concurrently across executors.

        Example: 5 regions × 180 days = 900 partitions
        With 16 executor cores, up to 16 HTTP requests run in parallel.

        Returns:
            List of NemwebPartition objects
        """
        dates = self._generate_date_range(self.start_date, self.end_date)

        all_partitions = []
        for region in self.regions:
            for date in dates:
                all_partitions.append(NemwebPartition(
                    region=region,
                    date=date,
                    table=self.table
                ))

        logger.info(
            f"Partition planning: {len(self.regions)} regions × {len(dates)} days = "
            f"{len(all_partitions)} partitions"
        )

        # Filter out completed partitions if checkpointing enabled
        if self.checkpoint_table and self.skip_completed:
            completed = self._get_completed_partitions()
            partitions = [p for p in all_partitions if p.partition_id not in completed]
            logger.info(
                f"After checkpoint filter: {len(completed)} completed, "
                f"{len(partitions)} remaining"
            )
            return partitions

        return all_partitions

    def _get_completed_partitions(self) -> set:
        """Load completed partition IDs from checkpoint table."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                return set()

            df = spark.read.table(self.checkpoint_table)
            return set(
                row.partition_id
                for row in df.select("partition_id").collect()
            )
        except Exception as e:
            logger.warning(f"Could not read checkpoint table: {e}")
            return set()

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """
        Read data for a single partition (one region, one day).

        This method runs on Spark executors. Each executor can process
        multiple partitions concurrently, enabling parallel HTTP fetches.

        Args:
            partition: NemwebPartition containing region and date

        Yields:
            Tuples matching the schema
        """
        from datetime import datetime as dt

        # Debug log to file (visible on workers)
        def _log(msg):
            try:
                with open("/tmp/nemweb_datasource_debug.log", "a") as f:
                    f.write(f"{msg}\n")
            except:
                pass

        _log(f"=== read() called for {partition.region}/{partition.date} ===")

        try:
            # Fetch single day of data for single region
            raw_data = fetch_nemweb_data(
                table=partition.table,
                region=partition.region,
                start_date=partition.date,
                end_date=partition.date  # Same date = single day
            )

            _log(f"Fetched {len(raw_data)} raw rows")

            row_count = 0
            for row in parse_nemweb_csv(raw_data, self.schema):
                row_count += 1

                # Validate first element (SETTLEMENTDATE) is datetime or None
                if row[0] is not None:
                    if not isinstance(row[0], dt):
                        _log(f"ROW {row_count} INVALID: row[0] is {type(row[0]).__name__} = {repr(row[0])}")
                        # Fix it by converting to None
                        row = (None,) + row[1:]

                if row_count <= 3:
                    _log(f"Row {row_count}: type(row[0])={type(row[0]).__name__}")

                yield row

            _log(f"Yielded {row_count} rows total")

        except Exception as e:
            _log(f"ERROR: {e}")
            logger.error(
                f"Error reading partition {partition.partition_id} "
                f"({partition.region}/{partition.date}): {e}"
            )
            raise


class NemwebStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for NEMWEB data with custom checkpoint management.

    IMPORTANT: Native Structured Streaming checkpointing does NOT work with
    custom data sources. This reader implements its own progress tracking
    via _load_progress() and _save_progress() methods.

    Options:
        table: MMS table name (default: DISPATCHREGIONSUM)
        regions: Comma-separated region IDs
        checkpoint_table: Delta table for storing offsets (REQUIRED for resumability)
        stream_id: Unique identifier for this stream (default: "nemweb_stream")
        start_timestamp: Initial timestamp if no checkpoint exists

    Usage:
        (spark.readStream
            .format("nemweb")
            .option("table", "DISPATCHREGIONSUM")
            .option("checkpoint_table", "main.nemweb.stream_checkpoints")
            .option("stream_id", "nemweb_ingest_prod")
            .load()
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/checkpoints/nemweb")  # Spark's internal checkpoint
            .toTable("nemweb_bronze"))
    """

    def __init__(self, options: dict):
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.regions = [r.strip() for r in options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")]
        self.schema = get_nemweb_schema(self.table)

        # Checkpoint configuration
        self.checkpoint_table = options.get("checkpoint_table")
        self.stream_id = options.get("stream_id", "nemweb_stream")

        # Load saved progress if checkpoint table exists
        self._saved_offset = self._load_progress()

    def _load_progress(self) -> Optional[str]:
        """Load saved offset from checkpoint table."""
        if not self.checkpoint_table:
            return None

        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                return None

            # Check if table exists
            if not spark.catalog.tableExists(self.checkpoint_table):
                logger.info(f"Checkpoint table {self.checkpoint_table} doesn't exist yet")
                return None

            # With MERGE, there's exactly one row per stream_id
            row = (spark.read.table(self.checkpoint_table)
                   .filter(f"stream_id = '{self.stream_id}'")
                   .collect())

            if row:
                offset = row[0]["offset_timestamp"]
                logger.info(f"Loaded checkpoint for {self.stream_id}: {offset}")
                return offset
            return None
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
            return None

    def _save_progress(self, offset_timestamp: str) -> None:
        """
        Save offset to checkpoint table using MERGE (upsert).

        This keeps exactly one row per stream_id, avoiding table growth
        and ensuring atomic updates.
        """
        if not self.checkpoint_table:
            return

        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                return

            # Ensure checkpoint table exists
            self._ensure_checkpoint_table(spark)

            # MERGE: update if exists, insert if not
            spark.sql(f"""
                MERGE INTO {self.checkpoint_table} AS target
                USING (
                    SELECT
                        '{self.stream_id}' AS stream_id,
                        '{offset_timestamp}' AS offset_timestamp,
                        '{self.table}' AS source_table,
                        '{",".join(self.regions)}' AS regions,
                        current_timestamp() AS updated_at
                ) AS source
                ON target.stream_id = source.stream_id
                WHEN MATCHED THEN
                    UPDATE SET
                        offset_timestamp = source.offset_timestamp,
                        source_table = source.source_table,
                        regions = source.regions,
                        updated_at = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (stream_id, offset_timestamp, source_table, regions, updated_at)
                    VALUES (source.stream_id, source.offset_timestamp, source.source_table,
                            source.regions, source.updated_at)
            """)
            logger.info(f"Saved checkpoint for {self.stream_id}: {offset_timestamp}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _ensure_checkpoint_table(self, spark) -> None:
        """Create checkpoint table if it doesn't exist."""
        if not spark.catalog.tableExists(self.checkpoint_table):
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.checkpoint_table} (
                    stream_id STRING NOT NULL,
                    offset_timestamp STRING,
                    source_table STRING,
                    regions STRING,
                    updated_at TIMESTAMP
                )
                USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
            """)

    def initialOffset(self) -> dict:
        """
        Return the initial offset for streaming.

        Priority: 1) Saved checkpoint, 2) start_timestamp option, 3) 1 hour ago
        """
        # First, check for saved progress
        if self._saved_offset:
            return {"timestamp": self._saved_offset}

        # Then check for explicit start timestamp
        start = self.options.get("start_timestamp")
        if start:
            return {"timestamp": start}

        # Default: start from 1 hour ago
        initial_time = datetime.now() - timedelta(hours=1)
        return {"timestamp": initial_time.strftime("%Y-%m-%d %H:%M:%S")}

    def latestOffset(self) -> dict:
        """Return the latest available offset (current time)."""
        return {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    def read(self, start: dict) -> Tuple[Iterator[Tuple], dict]:
        """
        Read data from start offset to latest, return data and new offset.

        Args:
            start: Starting offset {"timestamp": "YYYY-MM-DD HH:MM:SS"}

        Returns:
            Tuple of (data iterator, new offset)
        """
        start_time = datetime.strptime(start["timestamp"], "%Y-%m-%d %H:%M:%S")
        end_time = datetime.now()

        logger.info(f"Streaming read: {start_time} to {end_time}")

        all_rows = []

        for region in self.regions:
            try:
                raw_data = fetch_nemweb_data(
                    table=self.table,
                    region=region,
                    start_date=start_time.strftime("%Y-%m-%d"),
                    end_date=end_time.strftime("%Y-%m-%d"),
                    use_sample=False
                )

                # Filter to rows after start timestamp
                for row in raw_data:
                    row_time_str = row.get("SETTLEMENTDATE", "")
                    if row_time_str:
                        try:
                            # Parse NEMWEB timestamp formats
                            for fmt in ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                                try:
                                    row_time = datetime.strptime(row_time_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                continue

                            if row_time > start_time:
                                all_rows.append(row)
                        except Exception:
                            continue

            except Exception as e:
                logger.warning(f"Error fetching {region}: {e}")
                continue

        # Parse rows to tuples
        parsed = list(parse_nemweb_csv(all_rows, self.schema))

        new_offset = {"timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")}

        return iter(parsed), new_offset

    def commit(self, end: dict) -> None:
        """
        Called when Spark commits the offset after successful microbatch.

        This is where we persist our custom checkpoint to ensure resumability.
        """
        offset_timestamp = end["timestamp"]
        logger.info(f"Committing offset: {offset_timestamp}")

        # Save progress to checkpoint table
        self._save_progress(offset_timestamp)


class NemwebFilePartition(InputPartition):
    """
    Partition representing a single ZIP file from UC Volume.

    Each partition corresponds to one day's data file.
    """

    def __init__(self, file_path: str, table: str):
        self.file_path = file_path
        self.table = table
        self.partition_id = hashlib.md5(file_path.encode()).hexdigest()[:12]


class NemwebVolumeReader(DataSourceReader):
    """
    Reader that reads NEMWEB data from pre-downloaded files in UC Volume.

    This is the recommended approach for production:
    1. Download files to UC Volume using nemweb_ingest.download_nemweb_files()
    2. Read from Volume using this reader (avoids HTTP issues in Spark jobs)

    Options:
        volume_path: Path to UC Volume containing downloaded files
        table: MMS table name (default: DISPATCHREGIONSUM)
        regions: Optional comma-separated region filter
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.volume_path = options.get("volume_path")
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.regions = options.get("regions", "").split(",") if options.get("regions") else None

    def partitions(self) -> list[InputPartition]:
        """Create one partition per ZIP file."""
        import os

        table_path = os.path.join(self.volume_path, self.table.lower())

        if not os.path.exists(table_path):
            logger.warning(f"Volume path does not exist: {table_path}")
            return []

        files = sorted([
            os.path.join(table_path, f)
            for f in os.listdir(table_path)
            if f.endswith('.zip')
        ])

        logger.info(f"Found {len(files)} ZIP files in {table_path}")

        return [NemwebFilePartition(f, self.table) for f in files]

    def read(self, partition: NemwebFilePartition) -> Iterator[Tuple]:
        """
        Read and parse a single ZIP file.

        This runs on Spark executors - reads from local Volume path.
        """
        import zipfile
        import io
        import csv
        from datetime import datetime as dt

        # Table config for record type filtering
        TABLE_CONFIG = {
            "DISPATCHREGIONSUM": "DISPATCH,REGIONSUM",
            "DISPATCHPRICE": "DISPATCH,PRICE",
            "TRADINGPRICE": "TRADING,PRICE",
        }

        record_type = TABLE_CONFIG.get(partition.table)

        try:
            with open(partition.file_path, 'rb') as f:
                zip_data = io.BytesIO(f.read())

            rows = []
            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    # Handle nested ZIPs (archive files)
                    if name.endswith(".zip") or name.endswith(".ZIP"):
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
            if self.regions:
                rows = [r for r in rows if r.get("REGIONID") in self.regions]

            # Identify timestamp column indices for validation
            from pyspark.sql.types import TimestampType
            from datetime import datetime as dt
            timestamp_indices = [
                idx for idx, field in enumerate(self.schema.fields)
                if isinstance(field.dataType, TimestampType)
            ]

            # Convert to tuples matching schema
            for row in rows:
                try:
                    values = []
                    for field in self.schema.fields:
                        raw_val = row.get(field.name)
                        converted = self._convert_value(raw_val, field.dataType)
                        values.append(converted)

                    # FINAL VALIDATION: Ensure timestamp columns are datetime or None
                    # Spark's Arrow serializer will crash with AssertionError otherwise
                    for ts_idx in timestamp_indices:
                        val = values[ts_idx]
                        if val is not None and not isinstance(val, dt):
                            values[ts_idx] = None  # Force invalid timestamps to None

                    yield tuple(values)
                except Exception as e:
                    logger.warning(f"Skipping row: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error reading {partition.file_path}: {e}")
            raise

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

    def _convert_value(self, value, spark_type):
        """Convert string value to appropriate Python type.

        IMPORTANT: For TimestampType, this MUST return either datetime.datetime
        or None. Spark's Arrow serializer will fail with AssertionError otherwise.
        """
        from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType
        from datetime import datetime as dt

        if value is None or value == "":
            return None

        str_val = str(value).strip()
        if str_val == "":
            return None

        if isinstance(spark_type, StringType):
            return str_val

        elif isinstance(spark_type, DoubleType):
            try:
                return float(str_val)
            except (ValueError, TypeError):
                return None

        elif isinstance(spark_type, IntegerType):
            try:
                return int(float(str_val))
            except (ValueError, TypeError):
                return None

        elif isinstance(spark_type, TimestampType):
            # NEMWEB timestamp formats - must return datetime or None
            formats = [
                "%Y/%m/%d %H:%M:%S",      # 2024/01/01 00:05:00
                "%Y-%m-%d %H:%M:%S",      # 2024-01-01 00:05:00
                "%Y/%m/%d %H:%M",         # 2024/01/01 00:05 (no seconds)
                "%Y-%m-%d %H:%M",         # 2024-01-01 00:05 (no seconds)
                "%d/%m/%Y %H:%M:%S",      # 01/01/2024 00:05:00 (AU format)
                "%d/%m/%Y %H:%M",         # 01/01/2024 00:05 (AU format)
            ]
            for fmt in formats:
                try:
                    result = dt.strptime(str_val, fmt)
                    # Final type check - must be datetime
                    if isinstance(result, dt):
                        return result
                except (ValueError, TypeError):
                    continue
            # Return None if no format matched - never return string for timestamp
            return None

        # Default: return as string (for any unrecognized types)
        return str_val


class NemwebDataSource(DataSource):
    """
    Custom PySpark Data Source for AEMO NEMWEB electricity market data.

    Supports both batch and streaming reads with automatic failure recovery.

    **Recommended usage (Volume-based):**
        # First, download files to UC Volume
        from nemweb_ingest import download_nemweb_files
        download_nemweb_files("/Volumes/main/nemweb/raw", start_date="2024-01-01", end_date="2024-06-30")

        # Then read from Volume
        df = spark.read.format("nemweb").option("volume_path", "/Volumes/main/nemweb/raw").load()

    **Legacy usage (HTTP-based):**
        df = spark.read.format("nemweb").option("start_date", "2024-01-01").load()

    Batch Options:
        volume_path (str): Path to UC Volume with downloaded files (RECOMMENDED)
        table (str): MMS table name (default: DISPATCHREGIONSUM)
        regions (str): Comma-separated region IDs (default: all 5 NEM regions)
        start_date (str): Start date in YYYY-MM-DD format (HTTP mode only)
        end_date (str): End date in YYYY-MM-DD format (HTTP mode only)

    Streaming Options:
        table (str): MMS table name
        regions (str): Comma-separated region IDs
        start_timestamp (str): Initial timestamp (default: 1 hour ago)
        lookback_minutes (str): How far back to look for data (default: "30")

    Parallelism:
        For a 6-month date range with 5 regions:
        - 180 days × 5 regions = 900 partitions
        - Each partition fetches one day of data for one region
        - Spark executes partitions in parallel across executor cores
        - With 16 cores, up to 16 HTTP requests run concurrently

    Batch Example:
        df = (spark.read
              .format("nemweb")
              .option("table", "DISPATCHREGIONSUM")
              .option("regions", "NSW1,VIC1")
              .option("start_date", "2024-01-01")
              .option("end_date", "2024-06-30")
              .load())

    Streaming Example:
        (spark.readStream
            .format("nemweb")
            .option("table", "DISPATCHREGIONSUM")
            .load()
            .writeStream
            .option("checkpointLocation", "/checkpoints/nemweb")
            .toTable("nemweb_bronze"))
    """

    @classmethod
    def name(cls) -> str:
        """Return the format name for this data source."""
        return "nemweb"

    def schema(self) -> StructType:
        """
        Return the schema for the requested NEMWEB table.

        Schema is based on the MMS Data Model:
        https://nemweb.com.au/Reports/Current/MMSDataModelReport/
        """
        table = self.options.get("table", "DISPATCHREGIONSUM")
        return get_nemweb_schema(table)

    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a batch reader for this data source."""
        # Use Volume reader if volume_path is provided (recommended)
        if self.options.get("volume_path"):
            return NemwebVolumeReader(schema, self.options)
        # Fall back to HTTP reader
        return NemwebDataSourceReader(schema, self.options)

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        """Create a streaming reader for this data source."""
        return NemwebStreamReader(self.options)
