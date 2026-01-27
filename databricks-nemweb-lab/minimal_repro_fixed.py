"""
Minimal repro - FIXED version

The issue: Tuples with datetime objects cause AssertionError in Spark Connect
The fix: Use Row objects instead of tuples
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
        # FIX: Add __init__ method
        self.schema = schema
        self.options = options
    
    def read(self, partition: InputPartition):
        # FIX: Use Row objects instead of tuples
        # Tuples with datetime cause AssertionError in Spark Connect
        yield Row(timestamp=datetime(2026, 1, 28, 0, 12))


# Usage:
# spark.dataSource.register(MyDataSource)
# spark.read.format("my_source").load().show()
