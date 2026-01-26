# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Building a Custom PySpark Data Source
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll implement a custom PySpark data source for AEMO NEMWEB
# MAGIC electricity market data using the Python Data Source API (GA in DBR 15.4+/Spark 4.0).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand the Python Data Source API architecture
# MAGIC 2. Implement schema definition based on MMS Data Model
# MAGIC 3. Create partition-aware data reading
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC - [Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [Apache Spark Data Sources Tutorial](https://spark.apache.org/docs/latest/sql-data-sources-python.html)
# MAGIC - [Example Implementations (GitHub)](https://github.com/allisonwang-db/pyspark-data-sources)
# MAGIC - [MMS Data Model Report](https://nemweb.com.au/Reports/Current/MMSDataModelReport/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warm-up: Hello World Data Source (2 minutes)
# MAGIC
# MAGIC Let's start with the **simplest possible** custom data source. This example generates
# MAGIC synthetic data - no external APIs, no partitions, just the bare minimum to understand the API.
# MAGIC
# MAGIC A custom data source needs just **3 things**:
# MAGIC 1. A `name()` - the format string you'll use in `spark.read.format("name")`
# MAGIC 2. A `schema()` - what columns and types your data has
# MAGIC 3. A `reader()` - how to actually read the data
# MAGIC
# MAGIC > **Reference:** [Python Data Source API Quickstart](https://docs.databricks.com/en/pyspark/datasources.html#create-a-simple-data-source)

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

# The simplest possible data source - generates "Hello, World!" rows
class HelloWorldDataSource(DataSource):
    """
    Minimal data source that generates greeting messages.

    Usage:
        spark.dataSource.register(HelloWorldDataSource)
        df = spark.read.format("hello").option("count", 3).load()
    """

    @classmethod
    def name(cls) -> str:
        return "hello"  # spark.read.format("hello")

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
        # Yield tuples matching the schema: (id, message)
        for i in range(self.count):
            yield (i, f"Hello, World #{i}!")

# Register and test it!
spark.dataSource.register(HelloWorldDataSource)
df = spark.read.format("hello").option("count", 3).load()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Insight
# MAGIC
# MAGIC That's it! A custom data source is just:
# MAGIC - **DataSource class**: Declares the name, schema, and creates a reader
# MAGIC - **DataSourceReader class**: Has a `read()` method that yields tuples
# MAGIC
# MAGIC The tuples you yield **must match** the schema field order.
# MAGIC
# MAGIC > **More examples:** See the [pyspark-data-sources repo](https://github.com/allisonwang-db/pyspark-data-sources) for
# MAGIC > data sources that read from GitHub, Google Sheets, Hugging Face, and more!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Now Let's Build a Real One: NEMWEB Data Source
# MAGIC
# MAGIC AEMO NEMWEB publishes Australia's National Electricity Market data. We'll create a
# MAGIC data source that **fetches live data** from the NEMWEB HTTP API.
# MAGIC
# MAGIC The main difference from our Hello World example:
# MAGIC - **Real schema** based on AEMO's data model
# MAGIC - **Partitions** for parallel reading (one per NEM region)
# MAGIC - **HTTP fetching** from https://www.nemweb.com.au/REPORTS/CURRENT/
# MAGIC - **ZIP/CSV parsing** - NEMWEB data comes as CSV files inside ZIP archives

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Define the Schema (3 minutes)
# MAGIC
# MAGIC The DISPATCHREGIONSUM table contains regional dispatch summary data.
# MAGIC
# MAGIC > **Reference:** [MMS Data Model Report - DISPATCH package](https://nemweb.com.au/Reports/Current/MMSDataModelReport/)
# MAGIC
# MAGIC ### TODO 1.1: Add the missing numeric fields
# MAGIC
# MAGIC We've given you the identification fields. Add the measurement fields as `DoubleType`.

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
        # Time and identification fields (provided)
        StructField("SETTLEMENTDATE", TimestampType(), True),
        StructField("RUNNO", StringType(), True),
        StructField("REGIONID", StringType(), True),
        StructField("DISPATCHINTERVAL", StringType(), True),
        StructField("INTERVENTION", StringType(), True),

        # TODO 1.1: Add the 7 measurement fields below as DoubleType
        # Field names: TOTALDEMAND, AVAILABLEGENERATION, AVAILABLELOAD, DEMANDFORECAST,
        #              DISPATCHABLEGENERATION, DISPATCHABLELOAD, NETINTERCHANGE
        #
        # Hint: Follow the same pattern as the fields above:
        #   StructField("FIELD_NAME", DoubleType(), True),
        #
        # Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html

    ])

# Test your schema
schema = get_dispatchregionsum_schema()
print(f"Schema has {len(schema.fields)} fields (expected: 12)")
for field in schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Add Partitioning and Data Reading (5 minutes)
# MAGIC
# MAGIC Spark achieves parallelism by dividing work into **partitions**. Each partition
# MAGIC can be processed independently on different cores/nodes.
# MAGIC
# MAGIC For NEMWEB, we'll create one partition per NEM region:
# MAGIC - NSW1 (New South Wales)
# MAGIC - QLD1 (Queensland)
# MAGIC - SA1 (South Australia)
# MAGIC - VIC1 (Victoria)
# MAGIC - TAS1 (Tasmania)
# MAGIC
# MAGIC You need to implement two methods:
# MAGIC 1. **`partitions()`** - Plan the work (runs on driver)
# MAGIC 2. **`read()`** - Do the work (runs on workers)
# MAGIC
# MAGIC > **Reference:** [DataSourceReader.partitions()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSourceReader.partitions.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions (Provided)
# MAGIC
# MAGIC NEMWEB data comes as CSV files inside ZIP archives with a complex multi-record format.
# MAGIC We've provided helper functions that handle the HTTP fetching and parsing for you,
# MAGIC so you can focus on learning the **Data Source API**.
# MAGIC
# MAGIC - `fetch_nemweb_data(table, region, start_date, end_date)` → Returns list of row dicts
# MAGIC - `parse_nemweb_csv(data, schema)` → Converts dicts to tuples matching schema

# COMMAND ----------

# Import helper functions that handle NEMWEB's complex format
# These are provided so you can focus on the Data Source API, not HTTP/ZIP parsing
import sys
import os

# Add src to path for imports
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_utils import fetch_nemweb_data, parse_nemweb_csv

# Quick test - fetch one region's data
test_data = fetch_nemweb_data(
    table="DISPATCHREGIONSUM",
    region="NSW1",
    start_date="2025-12-01",
    end_date="2025-12-01",
    use_sample=True  # Use sample data for quick testing
)
print(f"Helper function works! Got {len(test_data)} rows")

# COMMAND ----------

class NemwebPartition(InputPartition):
    """
    Represents one partition of NEMWEB data.

    Each partition handles one region's data.
    InputPartition must be picklable (sent to workers).
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
        # Parse options with sensible defaults
        self.regions = options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")
        # Default to yesterday (ensures data exists)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.start_date = options.get("start_date", yesterday)
        self.end_date = options.get("end_date", yesterday)

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        TODO 1.2a: Return a list with one NemwebPartition for each region in self.regions.

        Hint: Create an empty list, loop through self.regions, and append a
        NemwebPartition for each one. Use self.start_date and self.end_date.

        Example structure:
            partitions = []
            for region in self.regions:
                partition = NemwebPartition(region, self.start_date, self.end_date)
                partitions.append(partition)
            return partitions

        Docs: https://docs.databricks.com/en/pyspark/datasources.html#partition-data-for-parallel-reads
        """
        # TODO: Implement this method
        pass

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """
        Read data for a single partition (runs on workers).

        This method is called once per partition, potentially on different
        executor nodes in parallel.

        TODO 1.2b: Use the helper functions to fetch and parse data.

        Steps:
        1. Call fetch_nemweb_data() with the partition's region and dates
        2. Call parse_nemweb_csv() to convert rows to tuples
        3. Yield each tuple

        Example:
            data = fetch_nemweb_data(
                table="DISPATCHREGIONSUM",
                region=partition.region,
                start_date=partition.start_date,
                end_date=partition.end_date
            )
            for row_tuple in parse_nemweb_csv(data, self.schema):
                yield row_tuple
        """
        # TODO: Implement this method using the helper functions
        pass


# Test partition planning
test_options = {"regions": "NSW1,VIC1,QLD1"}
reader = NemwebReader(schema, test_options)
partitions = reader.partitions()

print(f"Created {len(partitions)} partitions (expected: 3)")
for p in partitions:
    print(f"  - Region: {p.region}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Complete the Data Source (5 minutes)
# MAGIC
# MAGIC Now bring it all together! The DataSource class is the entry point that Spark calls.
# MAGIC
# MAGIC > **Reference:** [DataSource class](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html)

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

    # TODO 1.3: Implement these three methods
    # Hint: Look at HelloWorldDataSource above for the pattern!
    # Docs: https://docs.databricks.com/en/pyspark/datasources.html#create-a-simple-data-source

    @classmethod
    def name(cls) -> str:
        """Return the format name used in spark.read.format("...")."""
        # TODO 1.3a: Return "nemweb" so users can call spark.read.format("nemweb")
        pass

    def schema(self) -> StructType:
        """Return the schema for this data source."""
        # TODO 1.3b: Call and return the get_dispatchregionsum_schema() function from Part 1
        pass

    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a reader for this data source."""
        # TODO 1.3c: Return a new NemwebReader instance
        # Hint: Pass 'schema' and 'self.options' to the NemwebReader constructor
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Register and Test

# COMMAND ----------

# Register the data source with Spark
spark.dataSource.register(NemwebDataSource)

# Use yesterday's date (guaranteed to exist in CURRENT folder)
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Read REAL data from NEMWEB API!
df = (spark.read
      .format("nemweb")
      .option("regions", "NSW1")  # Single region for speed
      .option("start_date", yesterday)
      .option("end_date", yesterday)
      .load())

# Display results - this is LIVE data from the Australian electricity market!
print(f"Row count: {df.count()} (expected: ~288 rows for 24hrs of 5-min intervals)")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Run this cell to check your implementation:

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
        print("All checks passed! Your data source fetches REAL data from NEMWEB.")

    return len(errors) == 0

validate_implementation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've built a custom PySpark data source that fetches **live data** from NEMWEB!
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `DataSource.name()` | Format string for `spark.read.format(...)` |
# MAGIC | `DataSource.schema()` | Define output columns and types |
# MAGIC | `DataSource.reader()` | Create reader with options |
# MAGIC | `DataSourceReader.partitions()` | Plan parallel work units |
# MAGIC | `DataSourceReader.read()` | Fetch and parse data (runs on workers) |
# MAGIC
# MAGIC ## Learn More
# MAGIC
# MAGIC - [Full Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [Streaming Data Sources](https://docs.databricks.com/en/pyspark/datasources.html#create-a-streaming-data-source)
# MAGIC - [Data Source Writers (Sinks)](https://docs.databricks.com/en/pyspark/datasources.html#create-a-data-sink)
# MAGIC - [More Examples on GitHub](https://github.com/allisonwang-db/pyspark-data-sources)
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 02** to integrate your data source with Lakeflow Declarative Pipelines.
