# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 1 - Custom PySpark Data Source
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 1.
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

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

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
from datetime import datetime

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
# MAGIC ## Solution 1.2: Partition Planning

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
        self.start_date = options.get("start_date", "2024-01-01")
        self.end_date = options.get("end_date", "2024-01-07")

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        SOLUTION 1.2: Create one partition per region
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
        """Read data for a single partition (runs on workers)."""
        # Sample data for demonstration
        sample_data = [
            (datetime(2024, 1, 1, 0, 5), "1", partition.region, "1", "0",
             7500.5, 8000.0, 0.0, 7400.0, 7800.0, 0.0, -200.5),
            (datetime(2024, 1, 1, 0, 10), "1", partition.region, "2", "0",
             7520.3, 8000.0, 0.0, 7450.0, 7850.0, 0.0, -180.2),
            (datetime(2024, 1, 1, 0, 15), "1", partition.region, "3", "0",
             7540.8, 8050.0, 0.0, 7480.0, 7900.0, 0.0, -175.5),
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

# Read data using your new format!
df = (spark.read
      .format("nemweb")
      .option("regions", "NSW1,VIC1,QLD1")
      .load())

# Display results
print(f"Row count: {df.count()}")
print(f"Partitions: {df.rdd.getNumPartitions()}")
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

    # Check partitions
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1"})
    partitions = reader.partitions()
    if len(partitions) != 3:
        errors.append(f"Partitions: expected 3, got {len(partitions)}")

    # Check data source
    if NemwebDataSource.name() != "nemweb":
        errors.append(f"DataSource.name(): expected 'nemweb', got '{NemwebDataSource.name()}'")

    if errors:
        print("Issues found:")
        for e in errors:
            print(f"   - {e}")
    else:
        print("All checks passed!")

    return len(errors) == 0

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
