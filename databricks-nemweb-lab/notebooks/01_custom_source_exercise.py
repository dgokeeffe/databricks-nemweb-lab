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
# MAGIC data source that can read this data with options for region filtering.
# MAGIC
# MAGIC The main difference from our Hello World example:
# MAGIC - **Real schema** based on AEMO's data model
# MAGIC - **Partitions** for parallel reading (one per NEM region)
# MAGIC - **Options** for filtering by region and date

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
from datetime import datetime

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
        # TOTALDEMAND, AVAILABLEGENERATION, AVAILABLELOAD, DEMANDFORECAST,
        # DISPATCHABLEGENERATION, DISPATCHABLELOAD, NETINTERCHANGE
        # Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html


    ])

# Test your schema
schema = get_dispatchregionsum_schema()
print(f"Schema has {len(schema.fields)} fields (expected: 12)")
for field in schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Add Partitioning (5 minutes)
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
# MAGIC > **Reference:** [DataSourceReader.partitions()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSourceReader.partitions.html)

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
        self.start_date = options.get("start_date", "2024-01-01")
        self.end_date = options.get("end_date", "2024-01-07")

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        TODO 1.2: Return a list with one NemwebPartition for each region in self.regions.
        Use self.start_date and self.end_date for the date range.
        Docs: https://docs.databricks.com/en/pyspark/datasources.html#partition-data-for-parallel-reads
        """
        pass

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """
        Read data for a single partition (runs on workers).

        This is provided - it returns sample data for the partition's region.
        In production, this would fetch from NEMWEB HTTP API.
        """
        # Sample data for demonstration
        sample_data = [
            (datetime(2024, 1, 1, 0, 5), "1", partition.region, "1", "0",
             7500.5, 8000.0, 0.0, 7400.0, 7800.0, 0.0, -200.5),
            (datetime(2024, 1, 1, 0, 10), "1", partition.region, "2", "0",
             7520.3, 8000.0, 0.0, 7450.0, 7850.0, 0.0, -180.2),
        ]
        for row in sample_data:
            yield row


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
    # Docs: https://docs.databricks.com/en/pyspark/datasources.html#create-a-simple-data-source

    @classmethod
    def name(cls) -> str:
        # Return the format name for spark.read.format("???")
        pass

    def schema(self) -> StructType:
        # Return the schema (use the function defined in Part 1)
        pass

    def reader(self, schema: StructType) -> DataSourceReader:
        # Return a NemwebReader with schema and self.options
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Register and Test

# COMMAND ----------

# Register the data source with Spark
spark.dataSource.register(NemwebDataSource)

# Read data using your new format!
df = (spark.read
      .format("nemweb")
      .option("regions", "NSW1,VIC1")
      .load())

# Display results
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

    # Check partitions
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1"})
    partitions = reader.partitions()
    if len(partitions) != 3:
        errors.append(f"Partitions: expected 3, got {len(partitions)}")

    # Check data source
    if NemwebDataSource.name() != "nemweb":
        errors.append(f"DataSource.name(): expected 'nemweb', got '{NemwebDataSource.name()}'")

    if errors:
        print("❌ Issues found:")
        for e in errors:
            print(f"   - {e}")
    else:
        print("✅ All checks passed! Your data source is working.")

    return len(errors) == 0

validate_implementation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've built a custom PySpark data source! The key components are:
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
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 02** to integrate your data source with Lakeflow Declarative Pipelines.
