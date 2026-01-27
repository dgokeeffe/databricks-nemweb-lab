# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 1 - Building a Custom PySpark Data Source
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warm-up: Hello World Data Source

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from databricks.sdk.runtime import spark, display

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
    def __init__(self, options: dict):
        self.count = int(options.get("count", 5))

    def read(self, partition):
        for i in range(self.count):
            yield (i, f"Hello, World #{i}!")

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

# COMMAND ----------

# Import helper functions
import sys
import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_utils import fetch_nemweb_current, parse_nemweb_csv, get_version

# Output version for debugging
print(f"nemweb_utils version: {get_version()}")

# Quick test
test_data = fetch_nemweb_current(
    table="DISPATCHREGIONSUM",
    region="NSW1",
    max_files=2,
    use_sample=True
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

        SOLUTION 1.2b: Fetch and parse NEMWEB data
        """
        # Fetch live data from NEMWEB CURRENT folder
        data = fetch_nemweb_current(
            table="DISPATCHREGIONSUM",
            region=partition.region,
            max_files=6,
            debug=True  # Print debug info to help troubleshoot
        )

        # Convert to tuples matching schema
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

# Read data using your custom data source!
df = (spark.read
      .format("nemweb")
      .option("regions", "NSW1")
      .load())

print(f"Row count: {df.count()}")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

def validate_implementation():
    """Validate the custom data source implementation."""
    print("=" * 60)
    print("FINAL VALIDATION")
    print("=" * 60)

    checks = {
        "Part 1 - Schema (12 fields)": False,
        "Part 2 - Partitions": False,
        "Part 2 - Read": False,
        "Part 3 - DataSource.name()": False,
        "Part 3 - DataSource.schema()": False,
        "Part 3 - DataSource.reader()": False,
    }

    # Part 1: Schema
    schema = get_dispatchregionsum_schema()
    required = ["TOTALDEMAND", "AVAILABLEGENERATION", "NETINTERCHANGE"]
    schema_ok = (
        len(schema.fields) >= 12 and
        all(f in [field.name for field in schema.fields] for f in required)
    )
    checks["Part 1 - Schema (12 fields)"] = schema_ok

    # Part 2: Partitions
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1"})
    partitions = reader.partitions()
    checks["Part 2 - Partitions"] = partitions is not None and len(partitions) == 3

    # Part 2: Read
    if partitions:
        try:
            test_partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-01")
            result = list(reader.read(test_partition))
            checks["Part 2 - Read"] = len(result) > 0
        except:
            pass

    # Part 3: DataSource
    try:
        checks["Part 3 - DataSource.name()"] = NemwebDataSource.name() == "nemweb"
    except:
        pass

    try:
        ds = NemwebDataSource(options={})
        checks["Part 3 - DataSource.schema()"] = len(ds.schema().fields) >= 12
    except:
        pass

    try:
        ds = NemwebDataSource(options={})
        checks["Part 3 - DataSource.reader()"] = ds.reader(schema) is not None
    except:
        pass

    # Print results
    print()
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check}")

    all_passed = all(checks.values())
    print()
    if all_passed:
        print("=" * 60)
        print("🎉 All checks passed!")
        print("=" * 60)

    return all_passed

validate_implementation()

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
# MAGIC | `DataSourceReader.read()` | Fetch and yield data (runs on workers) |
# MAGIC
# MAGIC ## Compare to Production
# MAGIC
# MAGIC Your implementation is a simplified version. The production code in
# MAGIC `src/nemweb_datasource_arrow.py` adds:
# MAGIC - **PyArrow RecordBatch** for zero-copy transfer (Serverless compatible)
# MAGIC - **Volume mode** with parallel downloads to UC Volume
# MAGIC - **Multiple tables** (DISPATCHREGIONSUM, DISPATCHPRICE, TRADINGPRICE)
# MAGIC - **Retry logic** with exponential backoff
