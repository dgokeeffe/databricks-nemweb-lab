# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Building a Custom PySpark Data Source
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll build a custom PySpark data source for AEMO NEMWEB
# MAGIC electricity market data using the Python Data Source API (GA in DBR 15.4+/Spark 4.0).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand the Python Data Source API architecture
# MAGIC 2. Implement schema definition for external data
# MAGIC 3. Create partition-aware data reading for parallelism
# MAGIC 4. Register and use your custom data source
# MAGIC
# MAGIC ## Reference
# MAGIC - [Python Data Source API Docs](https://docs.databricks.com/en/pyspark/datasources.html)
# MAGIC - [Example Implementations (GitHub)](https://github.com/allisonwang-db/pyspark-data-sources)
# MAGIC - Production implementation: `src/nemweb_datasource_arrow.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warm-up: Hello World Data Source (2 minutes)
# MAGIC
# MAGIC Let's start with the **simplest possible** custom data source to understand the API.
# MAGIC
# MAGIC A custom data source needs just **3 components**:
# MAGIC 1. `name()` - The format string for `spark.read.format("name")`
# MAGIC 2. `schema()` - What columns and types your data has
# MAGIC 3. `reader()` - Creates a reader that fetches the data

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from databricks.sdk.runtime import spark, display

# The simplest possible data source - generates synthetic data
class HelloWorldDataSource(DataSource):
    """Minimal data source that generates greeting messages."""

    @classmethod
    def name(cls) -> str:
        return "hello"  # Used in spark.read.format("hello")

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
# MAGIC That's it! A custom data source is:
# MAGIC - **DataSource class**: Declares the name, schema, and creates a reader
# MAGIC - **DataSourceReader class**: Has a `read()` method that yields tuples
# MAGIC
# MAGIC The tuples you yield **must match** the schema field order.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Now Let's Build a Real One: NEMWEB Data Source
# MAGIC
# MAGIC AEMO NEMWEB publishes Australia's National Electricity Market data.
# MAGIC We'll create a data source that fetches **live data** from their HTTP API.
# MAGIC
# MAGIC Key differences from Hello World:
# MAGIC - **Real schema** based on AEMO's MMS data model
# MAGIC - **Partitions** for parallel reading (one per NEM region)
# MAGIC - **HTTP fetching** from https://www.nemweb.com.au/
# MAGIC
# MAGIC > **Reference:** See how the production code does it in `src/nemweb_datasource_arrow.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Define the Schema (3 minutes)
# MAGIC
# MAGIC The DISPATCHREGIONSUM table contains regional dispatch summary data.
# MAGIC Let's define its schema using Spark types.
# MAGIC
# MAGIC > **Reference:** [MMS Data Model - DISPATCH package](https://nemweb.com.au/Reports/Current/MMSDataModelReport/)

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
        # Field names: TOTALDEMAND, AVAILABLEGENERATION, AVAILABLELOAD,
        #              DEMANDFORECAST, DISPATCHABLEGENERATION,
        #              DISPATCHABLELOAD, NETINTERCHANGE
        #
        # Example: StructField("TOTALDEMAND", DoubleType(), True),

    ])

# Test your schema
schema = get_dispatchregionsum_schema()
print(f"Schema has {len(schema.fields)} fields (expected: 12)")
for field in schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint 1: Validate Schema
# MAGIC
# MAGIC Run this cell to check your schema before moving on.

# COMMAND ----------

def check_schema():
    """Validate Part 1 schema definition."""
    schema = get_dispatchregionsum_schema()
    required_fields = {
        "SETTLEMENTDATE": "TimestampType",
        "REGIONID": "StringType",
        "TOTALDEMAND": "DoubleType",
        "AVAILABLEGENERATION": "DoubleType",
        "NETINTERCHANGE": "DoubleType",
    }

    missing = []
    for field_name, expected_type in required_fields.items():
        field = next((f for f in schema.fields if f.name == field_name), None)
        if not field:
            missing.append(f"Missing field: {field_name}")
        elif expected_type not in str(field.dataType):
            missing.append(f"{field_name}: expected {expected_type}, got {field.dataType}")

    if len(schema.fields) < 12:
        missing.append(f"Schema has {len(schema.fields)} fields, expected 12")

    if missing:
        print("Part 1 Issues:")
        for m in missing:
            print(f"  ❌ {m}")
        return False
    else:
        print("✅ Part 1 COMPLETE - Schema is correct!")
        return True

check_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Add Partitioning and Data Reading (5 minutes)
# MAGIC
# MAGIC Spark achieves parallelism by dividing work into **partitions**.
# MAGIC Each partition can be processed independently on different cores/nodes.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions (Provided)
# MAGIC
# MAGIC NEMWEB data comes as CSV files inside ZIP archives with a complex multi-record format.
# MAGIC We've provided helper functions so you can focus on the **Data Source API**.
# MAGIC
# MAGIC **Note:** The helper functions fetch **REAL data** from AEMO NEMWEB CURRENT folder
# MAGIC (last ~7 days of 5-minute interval files). This ensures you're working with actual
# MAGIC electricity market data.
# MAGIC
# MAGIC **Important:** Real AEMO DISPATCHREGIONSUM data contains ~130+ columns, but our schema
# MAGIC only defines 12 key fields. The `parse_nemweb_csv()` function automatically filters
# MAGIC to only the fields in your schema - extra columns are ignored, and missing columns
# MAGIC are set to `None` (which is fine since all fields are nullable).

# COMMAND ----------

# Import helper functions from our utility module
import sys
import os

from nemweb_utils import fetch_nemweb_current, parse_nemweb_csv

# Quick test - these helpers handle HTTP fetching and CSV parsing
# This fetches REAL data from AEMO NEMWEB CURRENT folder (last ~7 days)
test_data = fetch_nemweb_current(
    table="DISPATCHREGIONSUM",
    region="NSW1",
    max_files=2,
    use_sample=False,  # Fetch real current files from AEMO
    debug=True  # Print debug info
)
print(f"Helper function works! Got {len(test_data)} rows")
if test_data:
    # Note: Real AEMO data has MANY more columns (~130+) than our 12-field schema
    # parse_nemweb_csv() will filter to only the fields in our schema
    all_keys = list(test_data[0].keys())
    print(f"Raw CSV has {len(all_keys)} columns (AEMO includes many fields)")
    print(f"Sample row keys (first 10): {all_keys[:10]}...")
    print(f"Sample SETTLEMENTDATE: {test_data[0].get('SETTLEMENTDATE')}")
    
    # Verify key fields exist in real data
    required_fields = ["SETTLEMENTDATE", "REGIONID", "RUNNO", "TOTALDEMAND", 
                      "AVAILABLEGENERATION", "NETINTERCHANGE"]
    missing = [f for f in required_fields if f not in all_keys]
    if missing:
        print(f"⚠️  WARNING: Missing fields in real data: {missing}")
    else:
        print(f"✅ All required fields present in real data")

# COMMAND ----------

class NemwebPartition(InputPartition):
    """
    Represents one partition of NEMWEB data.

    Each partition handles one region's data.
    InputPartition must be picklable (serializable to send to workers).
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
        yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        self.start_date = options.get("start_date", yesterday)
        self.end_date = options.get("end_date", yesterday)

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        TODO 1.2a: Return a list with one NemwebPartition for each region.

        Steps:
        1. Create an empty list called `partitions`
        2. Loop through self.regions
        3. For each region, create a NemwebPartition and append it
        4. Return the list

        Example:
            partitions = []
            for region in self.regions:
                partition = NemwebPartition(
                    region=region.strip(),
                    start_date=self.start_date,
                    end_date=self.end_date
                )
                partitions.append(partition)
            return partitions
        """
        # TODO: Implement this method
        pass

    def read(self, partition: NemwebPartition) -> Iterator[Tuple]:
        """
        Read data for a single partition (runs on workers).

        TODO 1.2b: Fetch and parse data for this partition's region.

        Steps:
        1. Call fetch_nemweb_current() with:
           - table="DISPATCHREGIONSUM"
           - region=partition.region
           - max_files=6
           - use_sample=False  # Fetch real current files from AEMO
           - debug=True  # Print debug info to help diagnose issues
           - target_date=self.start_date  # Filter files by date (handles timezone issues)

        2. Call parse_nemweb_csv(data, self.schema) to convert to tuples

        3. Yield each tuple from the result

        Note: The target_date parameter filters files by date extracted from filenames
        (format: PUBLIC_DISPATCHIS_YYYYMMDDHHMM_*.zip). This ensures we get files
        from the correct date even with timezone differences.

        Example:
            data = fetch_nemweb_current(
                table="DISPATCHREGIONSUM",
                region=partition.region,
                max_files=6,
                use_sample=False,  # Fetch real current files from AEMO
                debug=True,  # Print debug info (visible in worker logs)
                target_date=self.start_date  # Filter to files from this date
            )
            # Log to help diagnose empty results (visible in Spark UI logs)
            if not data:
                import sys
                print(f"[WORKER] WARNING: No data fetched for region {partition.region}, date {self.start_date}", file=sys.stderr)
            for row_tuple in parse_nemweb_csv(data, self.schema):
                yield row_tuple
        """
        # TODO: Implement this method
        pass


# Test partition planning
test_options = {"regions": "NSW1,VIC1,QLD1"}
reader = NemwebReader(schema, test_options)
partitions = reader.partitions()

if partitions:
    print(f"Created {len(partitions)} partitions (expected: 3)")
    for p in partitions:
        print(f"  - Region: {p.region}")
else:
    print("partitions() returned None - implement TODO 1.2a")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint 2: Validate Partitions and Reading

# COMMAND ----------

def check_partitions_and_read():
    """Validate Part 2 implementation."""
    issues = []

    # Check partitions
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1"})
    partitions = reader.partitions()

    if partitions is None:
        issues.append("partitions() returns None - did you forget 'return partitions'?")
    elif len(partitions) != 2:
        issues.append(f"partitions(): expected 2 for 2 regions, got {len(partitions)}")
    else:
        p = partitions[0]
        if not hasattr(p, 'region'):
            issues.append("Partition missing 'region' attribute")

    # Check read method
    if not issues:
        test_partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-01")
        try:
            result = list(reader.read(test_partition))
            if not result:
                issues.append("read() returned empty - check fetch_nemweb_current() call")
            elif len(result[0]) != len(schema.fields):
                issues.append(f"read() tuple has {len(result[0])} fields, expected {len(schema.fields)}")
        except Exception as e:
            issues.append(f"read() error: {e}")

    if issues:
        print("Part 2 Issues:")
        for i in issues:
            print(f"  ❌ {i}")
        return False
    else:
        print("✅ Part 2 COMPLETE - Partitions and reading work!")
        return True

check_partitions_and_read()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Complete the Data Source (5 minutes)
# MAGIC
# MAGIC Now bring it all together! The DataSource class is the entry point that Spark calls.

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
        # TODO 1.3a: Return "nemweb"
        pass

    def schema(self) -> StructType:
        """Return the schema for this data source."""
        # TODO 1.3b: Call and return get_dispatchregionsum_schema()
        pass

    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a reader for this data source."""
        # TODO 1.3c: Return NemwebReader(schema, self.options)
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Register and Test
# MAGIC
# MAGIC **Note:** The tuple-based datasource implementation above works for learning,
# MAGIC but Spark Connect (Serverless) has strict datetime serialization requirements
# MAGIC that can cause `AssertionError` issues. For production use, see the Arrow-based
# MAGIC datasource below which uses PyArrow RecordBatches to avoid serialization issues.

# COMMAND ----------

# Option 1: Test your tuple-based datasource (may have Spark Connect issues)
# Uncomment to test your implementation:
# spark.dataSource.register(NemwebDataSource)
# df = (spark.read
#       .format("nemweb")
#       .option("regions", "NSW1")
#       .load())
# print(f"Row count: {df.count()}")
# display(df.limit(5))

# Option 2: Use the production Arrow datasource (works perfectly with Spark Connect)
# This uses PyArrow RecordBatches which bypasses Python datetime serialization
from nemweb_datasource_arrow import NemwebArrowDataSource
from datetime import datetime, timedelta

spark.dataSource.register(NemwebArrowDataSource)

# Use 2 days ago for CURRENT files (CURRENT has ~7 days of data, but timezone differences mean yesterday may not be available yet)
yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

# Read data using the Arrow datasource - fetches real current files
df = (spark.read
      .format("nemweb_arrow")
      .option("table", "DISPATCHREGIONSUM")
      .option("regions", "NSW1")  # Single region for speed
      .option("start_date", yesterday)  # Use recent date for CURRENT files
      .option("end_date", yesterday)
      .load())

# Display results
print(f"Row count: {df.count()}")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation

# COMMAND ----------

def validate_implementation():
    """Validate the complete custom data source implementation."""
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
        print("🎉 CONGRATULATIONS! All checks passed!")
        print("   Your custom data source fetches REAL data from NEMWEB.")
        print("=" * 60)
    else:
        failed = [k for k, v in checks.items() if not v]
        print(f"Complete the remaining tasks: {', '.join(failed)}")

    return all_passed

validate_implementation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You learned how to build a custom PySpark data source! The concepts you learned:
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `DataSource.name()` | Format string for `spark.read.format(...)` |
# MAGIC | `DataSource.schema()` | Define output columns and types |
# MAGIC | `DataSource.reader()` | Create reader with options |
# MAGIC | `DataSourceReader.partitions()` | Plan parallel work units |
# MAGIC | `DataSourceReader.read()` | Fetch and yield data (runs on workers) |
# MAGIC
# MAGIC ## Production Implementation
# MAGIC
# MAGIC For production use, see `src/nemweb_datasource_arrow.py` which uses:
# MAGIC - **PyArrow RecordBatch** for zero-copy transfer (Serverless compatible)
# MAGIC - **Avoids datetime serialization issues** by using Arrow's native types
# MAGIC - **Volume mode** with parallel downloads to UC Volume
# MAGIC - **Multiple tables** (DISPATCHREGIONSUM, DISPATCHPRICE, TRADINGPRICE)
# MAGIC - **Retry logic** with exponential backoff
# MAGIC
# MAGIC **Note:** The tuple-based approach above works for learning, but Spark Connect
# MAGIC (Serverless) has strict datetime serialization requirements. The Arrow datasource
# MAGIC bypasses Python serialization entirely by using PyArrow RecordBatches, which is
# MAGIC why it works perfectly in production.
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Exercise 2** to integrate your data source with Lakeflow Pipelines.
