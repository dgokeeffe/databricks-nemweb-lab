"""
Minimal repro for datetime serialization issue in Spark Connect.

This demonstrates the fix: use Row objects instead of tuples.
"""

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, TimestampType
from pyspark.sql import Row
from datetime import datetime


class MyDataSource(DataSource):
    @classmethod
    def name(cls):
        return "my_source"
    
    def schema(self):
        return StructType([StructField("timestamp", TimestampType(), True)])
    
    def reader(self, schema):
        return MyReader(schema, self.options)


class MyReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
    
    def read(self, partition: InputPartition):
        # FIXED: Use Row objects instead of tuples
        # This avoids AssertionError in Spark Connect/Serverless
        yield Row(timestamp=datetime(2026, 1, 28, 0, 12))


# BROKEN VERSION (causes AssertionError in Spark Connect):
class BrokenReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
    
    def read(self, partition: InputPartition):
        # This causes: AssertionError in conversion.py
        # assert isinstance(value, datetime.datetime)
        yield (datetime(2026, 1, 28, 0, 12),)  # tuple with datetime


if __name__ == "__main__":
    from databricks.sdk.runtime import spark
    
    # Register and test the fixed version
    spark.dataSource.register(MyDataSource)
    df = spark.read.format("my_source").load()
    df.show()
    
    print("\n✅ Success! Row objects work correctly in Spark Connect/Serverless")
