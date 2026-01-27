# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 1 - Building a Custom PySpark Data Source (Backup Version)
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 1 (backup version).

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from typing import Iterator
from datetime import datetime, timedelta
import random

from databricks.sdk.runtime import spark, display, dbutils

def get_dispatchregionsum_schema() -> StructType:
    """
    Return the schema for DISPATCHREGIONSUM table.
    """
    return StructType([
        # Time and identification fields
        StructField("SETTLEMENTDATE", StringType(), True),
        StructField("RUNNO", StringType(), True),
        StructField("REGIONID", StringType(), True),
        StructField("DISPATCHINTERVAL", StringType(), True),
        StructField("INTERVENTION", StringType(), True),

        # SOLUTION 1.1: Added measurement fields
        StructField("TOTALDEMAND", StringType(), True),
        StructField("AVAILABLEGENERATION", StringType(), True),
        StructField("AVAILABLELOAD", StringType(), True),
        StructField("DEMANDFORECAST", StringType(), True),
        StructField("DISPATCHABLEGENERATION", StringType(), True),
        StructField("DISPATCHABLELOAD", StringType(), True),
        StructField("NETINTERCHANGE", StringType(), True),
    ])

# Verify schema
schema = get_dispatchregionsum_schema()
print(f"Schema has {len(schema.fields)} fields (expected: 12)")

# COMMAND ----------

def generate_fake_nemweb_row(region: str, timestamp: datetime) -> tuple:
    """Generate a single fake NEMWEB row for a region."""
    base_demand = {
        "NSW1": 8000, "QLD1": 6500, "VIC1": 5500, "SA1": 1800, "TAS1": 1200
    }.get(region, 5000)

    demand = base_demand * (0.8 + random.random() * 0.4)
    generation = demand * (1.0 + random.random() * 0.1)
    interchange = (random.random() - 0.5) * 500

    return (
        timestamp.strftime("%Y/%m/%d %H:%M:%S"),
        "1",
        region,
        "1",
        "0",
        f"{demand:.1f}",
        f"{generation:.1f}",
        f"{generation * 0.1:.1f}",
        f"{demand * 1.02:.1f}",
        f"{generation * 0.95:.1f}",
        f"{generation * 0.08:.1f}",
        f"{interchange:.1f}",
    )

# COMMAND ----------

class NemwebPartition(InputPartition):
    """Represents one partition of NEMWEB data."""
    def __init__(self, region: str, num_rows: int):
        self.region = region
        self.num_rows = num_rows


class NemwebReader(DataSourceReader):
    """Reader for fake NEMWEB data source."""

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.regions = options.get("regions", "NSW1,QLD1,SA1,VIC1,TAS1").split(",")
        self.rows_per_region = int(options.get("rowsPerRegion", 10))

    def partitions(self) -> list[InputPartition]:
        """
        Plan partitions for parallel reading.

        SOLUTION 1.2a: Create one partition per region
        """
        partitions = []
        for region in self.regions:
            partition = NemwebPartition(
                region=region.strip(),
                num_rows=self.rows_per_region
            )
            partitions.append(partition)
        return partitions

    def read(self, partition: NemwebPartition):
        """
        Read data for a single partition.

        SOLUTION 1.2b: Generate fake data and yield tuples
        """
        base_time = datetime.now()
        for i in range(partition.num_rows):
            timestamp = base_time - timedelta(minutes=i * 5)
            row = generate_fake_nemweb_row(partition.region, timestamp)
            yield row


# Test partition planning
reader = NemwebReader(schema, {"regions": "NSW1,VIC1,QLD1", "rowsPerRegion": "5"})
partitions = reader.partitions()
print(f"Created {len(partitions)} partitions (expected: 3)")

# COMMAND ----------

class NemwebDataSource(DataSource):
    """Custom PySpark Data Source for fake NEMWEB electricity market data."""

    @classmethod
    def name(cls) -> str:
        # SOLUTION 1.3a: Return the format name
        return "nemweb_fake"

    def schema(self) -> StructType:
        # SOLUTION 1.3b: Return the schema
        return get_dispatchregionsum_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        # SOLUTION 1.3c: Create and return the reader
        return NemwebReader(schema, self.options)


print("NemwebDataSource defined successfully!")
print(f"  - name(): {NemwebDataSource.name()}")

# COMMAND ----------

# Register and use the custom data source
spark.dataSource.register(NemwebDataSource)

df = (spark.read
      .format("nemweb_fake")
      .option("regions", "NSW1,VIC1")
      .option("rowsPerRegion", "5")
      .load())

print(f"Row count: {df.count()}")
display(df)

# COMMAND ----------

# Cast to proper types
from pyspark.sql.functions import to_timestamp, col

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
# MAGIC ## Summary
# MAGIC
# MAGIC You built a custom PySpark data source that generates synthetic data!
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `DataSource.name()` | Format string for `spark.read.format(...)` |
# MAGIC | `DataSource.schema()` | Define output columns and types |
# MAGIC | `DataSource.reader()` | Create reader with options |
# MAGIC | `DataSourceReader.partitions()` | Plan parallel work units |
# MAGIC | `DataSourceReader.read()` | Generate data and yield tuples |
# MAGIC
# MAGIC **Key Pattern:** Using StringType for all fields avoids serialization issues.
# MAGIC Cast to proper types in Spark SQL after loading.
