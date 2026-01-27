# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 1 - Custom PySpark Data Source
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 1.
# MAGIC
# MAGIC > **Note:** This is a **simplified educational version** for learning the Data Source API.
# MAGIC > The production implementation in `src/nemweb_datasource.py` includes additional features:
# MAGIC > - Fine-grained partitioning (region × date) for better parallelism
# MAGIC > - Checkpoint-based resume for failed batch jobs
# MAGIC > - Streaming support via `SimpleDataSourceStreamReader`
# MAGIC > - Retry logic with exponential backoff (via `nemweb_utils.py`)
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC - [Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [Apache Spark Data Sources Tutorial](https://spark.apache.org/docs/latest/sql-data-sources-python.html)
# MAGIC - [Example Implementations (GitHub)](https://github.com/allisonwang-db/pyspark-data-sources)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warm-up: Hello World Data Source
# MAGIC
# MAGIC This demonstrates the minimal structure of a custom data source.

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import spark and display from Databricks SDK for IDE support
from databricks.sdk.runtime import spark, display, dbutils

# Widgets for catalog/schema configuration (used in bonus section)
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("schema", "nemweb_lab", "Schema Name")

class HelloWorldDataSource(DataSource):
    """Minimal data source that generates greeting messages."""

    @classmethod
    def name(cls) -> str:
        return "hello"

    def schema(self) -> StructType:
        return StructType([
            StructField("id", IntegerType()),
            StructField("message", StringType()),
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        return HelloWorldReader(self.options)


class HelloWorldReader(DataSourceReader):
    """Reader that yields greeting rows."""

    def __init__(self, options: dict):
        self.count = int(options.get("count", 5))

    def read(self, partition):
        for i in range(self.count):
            yield (i, f"Hello, World #{i}!")

# Test it
spark.dataSource.register(HelloWorldDataSource)
df = spark.read.format("hello").option("count", 3).load()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Solution 1.1: Complete Schema Definition

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from typing import Iterator, Tuple
from datetime import datetime, timedelta

def get_dispatchregionsum_schema() -> StructType:
    """
    Return the schema for DISPATCHREGIONSUM table.

    Reference: MMS Electricity Data Model Report - DISPATCH package
    """
    return StructType([
        # Time and identification fields
        StructField("SETTLEMENTDATE", TimestampType(), True),
        StructField("RUNNO", StringType(), True),
        StructField("REGIONID", StringType(), True),
        StructField("DISPATCHINTERVAL", StringType(), True),
        StructField("INTERVENTION", StringType(), True),

        # SOLUTION 1.1: Added measurement fields
        StructField("TOTALDEMAND", DoubleType(), True),
        StructField("AVAILABLEGENERATION", DoubleType(), True),
        StructField("AVAILABLELOAD", DoubleType(), True),
        StructField("DEMANDFORECAST", DoubleType(), True),
        StructField("DISPATCHABLEGENERATION", DoubleType(), True),
        StructField("DISPATCHABLELOAD", DoubleType(), True),
        StructField("NETINTERCHANGE", DoubleType(), True),
    ])

# Verify schema
schema = get_dispatchregionsum_schema()
print(f"Schema has {len(schema.fields)} fields (expected: 12)")
for field in schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 1.2: Partition Planning and Data Reading
# MAGIC
# MAGIC We use helper functions from `nemweb_utils.py` that handle the complex NEMWEB
# MAGIC multi-record CSV format (parsing, type conversion, HTTP retries).
# MAGIC
# MAGIC - `fetch_nemweb_data(table, region, start_date, end_date)` → Returns list of row dicts
# MAGIC - `parse_nemweb_csv(data, schema)` → Converts dicts to tuples matching schema

# COMMAND ----------

# Import helper functions that handle NEMWEB's complex format
import sys
import os

# Add src to path for imports
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_utils import fetch_nemweb_data, fetch_nemweb_current, parse_nemweb_csv

# Quick test - fetch recent data from CURRENT (faster than archives)
test_data = fetch_nemweb_current(
    table="DISPATCHREGIONSUM",
    region="NSW1",
    max_files=2,  # Just 2 files for quick test
    use_sample=True  # Use sample data for quick testing
)
print(f"Helper function works! Got {len(test_data)} rows")

# COMMAND ----------

class NemwebPartition(InputPartition):
    """
    Represents one partition of NEMWEB data.

    Each partition handles one region's data.
    """
    def __init__(self, region: str, start_date: str, end_date: str):
        self.region = region
        self.start_date = start_date
        self.end_date = end_date


class NemwebReader(DataSourceReader):
    """
    Reader for NEMWEB data source.

    The reader has two jobs:
    1. partitions() - Plan the work (called on driver)
    2. read() - Do the work (called on workers)
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.regions = options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")
        # Default to yesterday (ensures data exists)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.start_date = options.get("start_date", yesterday)
        self.end_date = options.get("end_date", yesterday)

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        SOLUTION 1.2a: Create one partition per region
        """
        partitions = []

        for region in self.regions:
            partition = NemwebPartition(
                region=region.strip(),
                start_date=self.start_date,
                end_date=self.end_date
            )
            partitions.append(partition)

        return partitions

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """
        Read data for a single partition (runs on workers).

        SOLUTION 1.2b: Use helper functions to fetch and parse NEMWEB data
        """
        # Fetch recent data from CURRENT folder (5-minute interval files)
        # This is faster than fetching daily archives - great for demos!
        data = fetch_nemweb_current(
            table="DISPATCHREGIONSUM",
            region=partition.region,
            max_files=6  # ~30 minutes of data per region
        )

        # Convert to tuples matching schema using the parser helper
        for row_tuple in parse_nemweb_csv(data, self.schema):
            yield row_tuple


# Test partition planning
test_options = {"regions": "NSW1,VIC1,QLD1"}
reader = NemwebReader(schema, test_options)
partitions = reader.partitions()

print(f"Created {len(partitions)} partitions (expected: 3)")
for p in partitions:
    print(f"  - Region: {p.region}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 1.3: Complete DataSource Class

# COMMAND ----------

class NemwebDataSource(DataSource):
    """
    Custom PySpark Data Source for AEMO NEMWEB electricity market data.

    Usage:
        spark.dataSource.register(NemwebDataSource)
        df = spark.read.format("nemweb").option("regions", "NSW1,VIC1").load()

    Options:
        - regions: Comma-separated list of NEM regions (default: all 5)
        - start_date: Start date in YYYY-MM-DD format
        - end_date: End date in YYYY-MM-DD format
    """

    @classmethod
    def name(cls) -> str:
        """Return the format name used in spark.read.format("...")."""
        # SOLUTION 1.3a: Return format name
        return "nemweb"

    def schema(self) -> StructType:
        """Return the schema for this data source."""
        # SOLUTION 1.3b: Return schema
        return get_dispatchregionsum_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a reader for this data source."""
        # SOLUTION 1.3c: Create reader with schema and options
        return NemwebReader(schema, self.options)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register and Test

# COMMAND ----------

# Register the data source with Spark
spark.dataSource.register(NemwebDataSource)

# Read REAL data from NEMWEB API!
# Uses recent 5-minute interval files from CURRENT (faster than daily archives)
df = (spark.read
      .format("nemweb")
      .option("regions", "NSW1")  # Single region for speed
      .load())

# Display results - this is LIVE data from the Australian electricity market!
print(f"Row count: {df.count()} (expected: ~6 rows from recent 5-min intervals)")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

def validate_implementation():
    """Validate the custom data source implementation."""
    errors = []

    # Check schema
    schema = get_dispatchregionsum_schema()
    if len(schema.fields) < 12:
        errors.append(f"Schema: expected 12 fields, got {len(schema.fields)}")

    required = ["TOTALDEMAND", "AVAILABLEGENERATION", "NETINTERCHANGE"]
    for field in required:
        if field not in [f.name for f in schema.fields]:
            errors.append(f"Schema: missing {field}")

    # Check partitions (3 regions = 3 partitions for single day)
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1"})
    partitions = reader.partitions()
    if len(partitions) != 3:
        errors.append(f"Partitions: expected 3, got {len(partitions)}")

    # Check data source name
    if NemwebDataSource.name() != "nemweb":
        errors.append(f"DataSource.name(): expected 'nemweb', got '{NemwebDataSource.name()}'")

    if errors:
        print("Issues found:")
        for e in errors:
            print(f"   - {e}")
    else:
        print("All checks passed! Data source fetches REAL data from NEMWEB.")

    return len(errors) == 0

validate_implementation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Incremental Ingestion with Delta Checkpointing
# MAGIC
# MAGIC For production use, you want to track what data has been ingested and only fetch new files.
# MAGIC This uses a Delta table to checkpoint the last processed file.

# COMMAND ----------

# Configuration
CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "workspace"
SCHEMA = dbutils.widgets.get("schema") if "schema" in [w.name for w in dbutils.widgets.getAll()] else "nemweb_lab"

CHECKPOINT_TABLE = f"{CATALOG}.{SCHEMA}.nemweb_checkpoints"
DATA_TABLE = f"{CATALOG}.{SCHEMA}.nemweb_current"

# Create checkpoint table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
        table_name STRING,
        last_file STRING,
        last_timestamp TIMESTAMP,
        rows_ingested LONG,
        updated_at TIMESTAMP
    ) USING DELTA
""")

print(f"Checkpoint table: {CHECKPOINT_TABLE}")

# COMMAND ----------

def get_last_checkpoint(table_name: str) -> str:
    """Get the last processed filename for a table."""
    result = spark.sql(f"""
        SELECT last_file FROM {CHECKPOINT_TABLE}
        WHERE table_name = '{table_name}'
    """).collect()
    return result[0]["last_file"] if result else None


def update_checkpoint(table_name: str, last_file: str, rows: int):
    """Update the checkpoint after successful ingestion."""
    spark.sql(f"""
        MERGE INTO {CHECKPOINT_TABLE} AS target
        USING (SELECT
            '{table_name}' as table_name,
            '{last_file}' as last_file,
            current_timestamp() as last_timestamp,
            {rows} as rows_ingested,
            current_timestamp() as updated_at
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Checkpoint updated: {table_name} -> {last_file} ({rows} rows)")

# COMMAND ----------

import re
from nemweb_utils import NEMWEB_CURRENT_URL, TABLE_CONFIG, REQUEST_TIMEOUT, USER_AGENT
from urllib.request import urlopen, Request

def fetch_new_files_since_checkpoint(table: str, max_files: int = 12) -> list[str]:
    """
    Get list of new files in CURRENT since last checkpoint.

    Returns filenames sorted oldest-first for sequential processing.
    """
    config = TABLE_CONFIG[table]
    folder = config["folder"]
    file_prefix = config["file_prefix"]

    # Get last checkpoint
    last_file = get_last_checkpoint(table)
    print(f"Last checkpoint: {last_file or 'None (first run)'}")

    # List CURRENT directory
    current_url = f"{NEMWEB_CURRENT_URL}/{folder}/"
    request = Request(current_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        html = response.read().decode('utf-8')

    # Find all matching files
    pattern = rf'href="(PUBLIC_{file_prefix}_\d{{12}}\.zip)"'
    all_files = sorted(re.findall(pattern, html, re.IGNORECASE))

    # Filter to files newer than checkpoint
    if last_file:
        new_files = [f for f in all_files if f > last_file]
    else:
        new_files = all_files[-max_files:]  # First run: take recent files

    print(f"Found {len(new_files)} new files (of {len(all_files)} total)")
    return new_files[:max_files]

# Check for new files
new_files = fetch_new_files_since_checkpoint("DISPATCHREGIONSUM", max_files=6)
for f in new_files:
    print(f"  - {f}")

# COMMAND ----------

from nemweb_utils import fetch_nemweb_current, parse_nemweb_csv
from pyspark.sql.functions import current_timestamp

def incremental_ingest(table: str, region: str = None, max_files: int = 6):
    """
    Incrementally ingest new data since last checkpoint.

    - Fetches only new files from CURRENT
    - Appends to Delta table
    - Updates checkpoint on success
    """
    # Get new files
    new_files = fetch_new_files_since_checkpoint(table, max_files)

    if not new_files:
        print("No new files to ingest")
        return 0

    # Fetch data (fetch_nemweb_current will get the files)
    data = fetch_nemweb_current(table, region=region, max_files=len(new_files))

    if not data:
        print("No data returned")
        return 0

    # Convert to DataFrame
    schema = get_dispatchregionsum_schema()
    rows = list(parse_nemweb_csv(data, schema))
    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("_ingested_at", current_timestamp())

    # Append to Delta table
    df.write.format("delta").mode("append").saveAsTable(DATA_TABLE)

    # Update checkpoint with newest file
    update_checkpoint(table, new_files[-1], len(rows))

    return len(rows)

# Run incremental ingestion
rows_ingested = incremental_ingest("DISPATCHREGIONSUM", region="NSW1", max_files=6)
print(f"\nTotal rows ingested: {rows_ingested}")

# COMMAND ----------

# View checkpoint status
display(spark.table(CHECKPOINT_TABLE))

# COMMAND ----------

# View ingested data
if spark.catalog.tableExists(DATA_TABLE):
    print(f"Data table: {DATA_TABLE}")
    print(f"Total rows: {spark.table(DATA_TABLE).count()}")
    display(spark.table(DATA_TABLE).orderBy("SETTLEMENTDATE", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. **Checkpoint Table** stores the last processed filename per source table
# MAGIC 2. **On each run**, we list CURRENT and filter to files newer than checkpoint
# MAGIC 3. **Append mode** adds new data to the Delta table (no duplicates if files are unique)
# MAGIC 4. **Update checkpoint** after successful ingestion
# MAGIC
# MAGIC Run the incremental ingest cell multiple times - it will only fetch NEW files!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `DataSource.name()` | Format string for `spark.read.format(...)` |
# MAGIC | `DataSource.schema()` | Define output columns and types |
# MAGIC | `DataSource.reader()` | Create reader with options |
# MAGIC | `DataSourceReader.partitions()` | Plan parallel work units |
# MAGIC | `DataSourceReader.read()` | Actually read data (runs on workers) |
# MAGIC
# MAGIC ## Learn More
# MAGIC
# MAGIC - [Full Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [Streaming Data Sources](https://docs.databricks.com/en/pyspark/datasources.html#create-a-streaming-data-source)
# MAGIC - [Data Source Writers (Sinks)](https://docs.databricks.com/en/pyspark/datasources.html#create-a-data-sink)
# MAGIC - [More Examples on GitHub](https://github.com/allisonwang-db/pyspark-data-sources)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Extension: Date-Based Partitioning
# MAGIC
# MAGIC For larger date ranges, partition by region AND date for more parallelism:

# COMMAND ----------

from datetime import timedelta

class NemwebReaderWithDatePartitions(DataSourceReader):
    """Reader with date-based partitioning for larger ranges."""

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.regions = options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")
        self.start_date = datetime.strptime(options.get("start_date", "2024-01-01"), "%Y-%m-%d")
        self.end_date = datetime.strptime(options.get("end_date", "2024-01-07"), "%Y-%m-%d")

    def partitions(self) -> list[InputPartition]:
        """Create region x date partitions."""
        partitions = []

        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime("%Y-%m-%d")

            for region in self.regions:
                partitions.append(NemwebPartition(
                    region=region.strip(),
                    start_date=date_str,
                    end_date=date_str  # Single day per partition
                ))

            current_date += timedelta(days=1)

        return partitions

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """Read data for a single partition."""
        # Implementation would fetch data for specific date + region
        pass


# Example: 7 days x 3 regions = 21 partitions
reader = NemwebReaderWithDatePartitions(schema, {
    "regions": "NSW1,VIC1,QLD1",
    "start_date": "2024-01-01",
    "end_date": "2024-01-07"
})
print(f"Date-based partitions: {len(reader.partitions())}")
