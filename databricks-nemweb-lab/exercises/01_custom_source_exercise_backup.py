# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Building a Custom PySpark Data Source (Backup Version)
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll build a custom PySpark data source that generates
# MAGIC synthetic electricity market data. This teaches the same concepts as the
# MAGIC NEMWEB exercise without external dependencies.
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

from databricks.sdk.runtime import spark, display, dbutils

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
        # Yield tuples matching the schema
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
# MAGIC **Important:** For simplicity and reliability, we use **StringType** for most fields.
# MAGIC You can cast to proper types in Spark SQL after loading.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Now Let's Build a Real One: Fake NEMWEB Data Source
# MAGIC
# MAGIC We'll create a data source that generates **synthetic electricity market data**
# MAGIC similar to what AEMO NEMWEB provides. This teaches the same concepts without
# MAGIC external dependencies.
# MAGIC
# MAGIC Key features:
# MAGIC - **Real schema** based on AEMO's MMS data model
# MAGIC - **Partitions** for parallel reading (one per NEM region)
# MAGIC - **Synthetic data** that looks like real electricity market data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Define the Schema (3 minutes)
# MAGIC
# MAGIC The DISPATCHREGIONSUM table contains regional dispatch summary data.
# MAGIC Let's define its schema using Spark types.
# MAGIC
# MAGIC > **Note:** We use StringType for all fields for simplicity. In production,
# MAGIC > you would cast to proper types (TimestampType, DoubleType) after loading.

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from typing import Iterator
from datetime import datetime, timedelta
import random

def get_dispatchregionsum_schema() -> StructType:
    """
    Return the schema for DISPATCHREGIONSUM table.

    Reference: MMS Electricity Data Model Report - DISPATCH package

    Note: All fields are StringType for simplicity. Cast to proper types
    in Spark SQL after loading using to_timestamp() and cast().
    """
    return StructType([
        # Time and identification fields (provided)
        StructField("SETTLEMENTDATE", StringType(), True),
        StructField("RUNNO", StringType(), True),
        StructField("REGIONID", StringType(), True),
        StructField("DISPATCHINTERVAL", StringType(), True),
        StructField("INTERVENTION", StringType(), True),

        # TODO 1.1: Add the 7 measurement fields below as StringType
        # Field names: TOTALDEMAND, AVAILABLEGENERATION, AVAILABLELOAD,
        #              DEMANDFORECAST, DISPATCHABLEGENERATION,
        #              DISPATCHABLELOAD, NETINTERCHANGE
        #
        # Example: StructField("TOTALDEMAND", StringType(), True),

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
    required_fields = [
        "SETTLEMENTDATE", "REGIONID", "TOTALDEMAND",
        "AVAILABLEGENERATION", "NETINTERCHANGE"
    ]

    missing = []
    for field_name in required_fields:
        field = next((f for f in schema.fields if f.name == field_name), None)
        if not field:
            missing.append(f"Missing field: {field_name}")

    if len(schema.fields) < 12:
        missing.append(f"Schema has {len(schema.fields)} fields, expected 12")

    if missing:
        print("Part 1 Issues:")
        for m in missing:
            print(f"  - {m}")
        return False
    else:
        print("Part 1 COMPLETE - Schema is correct!")
        return True

check_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Add Partitioning and Data Reading (5 minutes)
# MAGIC
# MAGIC Spark achieves parallelism by dividing work into **partitions**.
# MAGIC Each partition can be processed independently on different cores/nodes.
# MAGIC
# MAGIC For our fake NEMWEB, we'll create one partition per NEM region:
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
# MAGIC ### Helper Function: Generate Fake Data
# MAGIC
# MAGIC This function generates realistic-looking electricity market data.

# COMMAND ----------

def generate_fake_nemweb_row(region: str, timestamp: datetime) -> tuple:
    """
    Generate a single fake NEMWEB row for a region.

    Returns a tuple matching the schema field order.
    """
    # Base demand varies by region (MW)
    base_demand = {
        "NSW1": 8000, "QLD1": 6500, "VIC1": 5500, "SA1": 1800, "TAS1": 1200
    }.get(region, 5000)

    # Add some randomness (+/- 20%)
    demand = base_demand * (0.8 + random.random() * 0.4)
    generation = demand * (1.0 + random.random() * 0.1)  # Slightly more than demand
    interchange = (random.random() - 0.5) * 500  # -250 to +250 MW

    return (
        timestamp.strftime("%Y/%m/%d %H:%M:%S"),  # SETTLEMENTDATE
        "1",                                       # RUNNO
        region,                                    # REGIONID
        "1",                                       # DISPATCHINTERVAL
        "0",                                       # INTERVENTION
        f"{demand:.1f}",                          # TOTALDEMAND
        f"{generation:.1f}",                      # AVAILABLEGENERATION
        f"{generation * 0.1:.1f}",                # AVAILABLELOAD
        f"{demand * 1.02:.1f}",                   # DEMANDFORECAST
        f"{generation * 0.95:.1f}",               # DISPATCHABLEGENERATION
        f"{generation * 0.08:.1f}",               # DISPATCHABLELOAD
        f"{interchange:.1f}",                     # NETINTERCHANGE
    )

# Test the helper
test_row = generate_fake_nemweb_row("NSW1", datetime.now())
print(f"Generated row with {len(test_row)} fields:")
for i, field in enumerate(schema.fields):
    if i < len(test_row):
        print(f"  {field.name}: {test_row[i]}")

# COMMAND ----------

class NemwebPartition(InputPartition):
    """
    Represents one partition of NEMWEB data.

    Each partition handles one region's data.
    InputPartition must be picklable (serializable to send to workers).
    """
    def __init__(self, region: str, num_rows: int):
        self.region = region
        self.num_rows = num_rows


class NemwebReader(DataSourceReader):
    """
    Reader for fake NEMWEB data source.

    The reader has two jobs:
    1. partitions() - Plan the work (called on driver)
    2. read() - Do the work (called on workers)
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.regions = options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")
        self.rows_per_region = int(options.get("rowsPerRegion", 10))

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
                    num_rows=self.rows_per_region
                )
                partitions.append(partition)
            return partitions
        """
        # TODO: Implement this method
        pass

    def read(self, partition: NemwebPartition):
        """
        Read data for a single partition (runs on workers).

        TODO 1.2b: Generate fake data and yield tuples.

        Steps:
        1. Import random and datetime at the top of the method
        2. Create a base timestamp (e.g., datetime.now())
        3. Loop `partition.num_rows` times
        4. Generate a row using generate_fake_nemweb_row()
        5. Yield the row tuple

        Example:
            import random
            from datetime import datetime, timedelta

            base_time = datetime.now()
            for i in range(partition.num_rows):
                # Each row is 5 minutes apart
                timestamp = base_time - timedelta(minutes=i * 5)
                row = generate_fake_nemweb_row(partition.region, timestamp)
                yield row
        """
        # TODO: Implement this method
        pass


# Test partition planning
test_options = {"regions": "NSW1,VIC1,QLD1", "rowsPerRegion": "5"}
reader = NemwebReader(schema, test_options)
partitions = reader.partitions()

if partitions:
    print(f"Created {len(partitions)} partitions (expected: 3)")
    for p in partitions:
        print(f"  - Region: {p.region}, Rows: {p.num_rows}")
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
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1", "rowsPerRegion": "3"})
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
        test_partition = NemwebPartition("NSW1", 3)
        try:
            result = list(reader.read(test_partition))
            if not result:
                issues.append("read() returned empty - check your loop")
            elif not isinstance(result[0], tuple):
                issues.append(f"read() should yield tuples, got {type(result[0]).__name__}")
            elif len(result) != 3:
                issues.append(f"Expected 3 rows, got {len(result)}")
        except Exception as e:
            issues.append(f"read() error: {e}")

    if issues:
        print("Part 2 Issues:")
        for i in issues:
            print(f"  - {i}")
        return False
    else:
        print("Part 2 COMPLETE - Partitions and reading work!")
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
    Custom PySpark Data Source for fake NEMWEB electricity market data.

    Usage:
        spark.dataSource.register(NemwebDataSource)
        df = spark.read.format("nemweb_fake").option("regions", "NSW1,VIC1").load()

    Options:
        - regions: Comma-separated list of NEM regions (default: all 5)
        - rowsPerRegion: Number of rows to generate per region (default: 10)
    """

    @classmethod
    def name(cls) -> str:
        """Return the format name used in spark.read.format("...")."""
        # TODO 1.3a: Return "nemweb_fake"
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
# MAGIC Now let's test your implementation!

# COMMAND ----------

# Register and test your datasource
spark.dataSource.register(NemwebDataSource)

# Read fake data using your custom datasource
df = (spark.read
      .format("nemweb_fake")
      .option("regions", "NSW1,VIC1")
      .option("rowsPerRegion", "5")
      .load())

# Display results
print(f"Row count: {df.count()}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus: Cast to Proper Types
# MAGIC
# MAGIC Since we used StringType for simplicity, here's how to cast to proper types:

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# Cast string columns to proper types
df_typed = (df
    .withColumn("SETTLEMENTDATE", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss"))
    .withColumn("TOTALDEMAND", col("TOTALDEMAND").cast("double"))
    .withColumn("AVAILABLEGENERATION", col("AVAILABLEGENERATION").cast("double"))
    .withColumn("NETINTERCHANGE", col("NETINTERCHANGE").cast("double"))
)

display(df_typed)
df_typed.printSchema()

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
        "Part 2 - Read (tuples)": False,
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
    reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1", "rowsPerRegion": "3"})
    partitions = reader.partitions()
    checks["Part 2 - Partitions"] = partitions is not None and len(partitions) == 3

    # Part 2: Read
    if partitions:
        try:
            test_partition = NemwebPartition("NSW1", 3)
            result = list(reader.read(test_partition))
            checks["Part 2 - Read (tuples)"] = (
                len(result) == 3 and isinstance(result[0], tuple)
            )
        except Exception as e:
            print(f"Read check error: {e}")

    # Part 3: DataSource
    try:
        checks["Part 3 - DataSource.name()"] = NemwebDataSource.name() == "nemweb_fake"
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
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {check}")

    all_passed = all(checks.values())
    print()
    if all_passed:
        print("=" * 60)
        print("CONGRATULATIONS! All checks passed!")
        print("Your custom data source generates synthetic NEMWEB data!")
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
# MAGIC | `DataSourceReader.read()` | Generate/fetch data and yield tuples |
# MAGIC
# MAGIC **Key Pattern:** Using StringType for all fields avoids serialization issues.
# MAGIC Cast to proper types in Spark SQL after loading.
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Exercise 2** to integrate your data source with Lakeflow Pipelines.
