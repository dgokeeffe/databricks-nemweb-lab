"""
NEMWEB Custom PySpark Data Source

This module implements a custom data source for AEMO NEMWEB electricity market data
using Databricks' Python Data Source API (GA in DBR 15.4+/Spark 4.0).

Usage:
    # Batch reading
    spark.dataSource.register(NemwebDataSource)
    df = spark.read.format("nemweb").option("table", "DISPATCHREGIONSUM").load()

    # Streaming with automatic checkpointing
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
except ImportError:
    from nemweb_utils import fetch_nemweb_data, parse_nemweb_csv, get_nemweb_schema

logger = logging.getLogger(__name__)


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
        logger.debug(f"Reading partition: {partition.region} / {partition.date}")

        try:
            # Fetch single day of data for single region
            raw_data = fetch_nemweb_data(
                table=partition.table,
                region=partition.region,
                start_date=partition.date,
                end_date=partition.date  # Same date = single day
            )

            for row in parse_nemweb_csv(raw_data, self.schema):
                yield row

        except Exception as e:
            logger.error(
                f"Error reading partition {partition.partition_id} "
                f"({partition.region}/{partition.date}): {e}"
            )
            raise


class NemwebStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for NEMWEB data with automatic offset management.

    Spark handles checkpointing automatically when using streaming.
    The offset tracks the latest timestamp processed.

    Usage:
        (spark.readStream
            .format("nemweb")
            .option("table", "DISPATCHREGIONSUM")
            .load()
            .writeStream
            .format("delta")
            .option("checkpointLocation", "/checkpoints/nemweb")
            .toTable("nemweb_bronze"))
    """

    def __init__(self, options: dict):
        self.options = options
        self.table = options.get("table", "DISPATCHREGIONSUM")
        self.regions = [r.strip() for r in options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")]
        self.schema = get_nemweb_schema(self.table)

        # Polling interval (how far back to look for new data)
        self.lookback_minutes = int(options.get("lookback_minutes", "30"))

    def initialOffset(self) -> dict:
        """
        Return the initial offset for streaming.

        Can be overridden with start_timestamp option.
        """
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
        Called when Spark commits the offset to checkpoint.

        Can be used for cleanup or logging.
        """
        logger.info(f"Committed offset: {end['timestamp']}")


class NemwebDataSource(DataSource):
    """
    Custom PySpark Data Source for AEMO NEMWEB electricity market data.

    Supports both batch and streaming reads with automatic failure recovery.
    Uses fine-grained partitioning (one partition per region+day) for
    maximum parallelism during batch reads.

    Batch Options:
        table (str): MMS table name (default: DISPATCHREGIONSUM)
        regions (str): Comma-separated region IDs (default: all 5 NEM regions)
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        checkpoint_table (str): Delta table for tracking completed partitions
        skip_completed (str): "true" to skip completed partitions (default: "true")

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
        return NemwebDataSourceReader(schema, self.options)

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        """Create a streaming reader for this data source."""
        return NemwebStreamReader(self.options)
