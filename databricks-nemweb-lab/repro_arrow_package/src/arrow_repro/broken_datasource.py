"""
Minimal reproduction of Arrow serialization error on Databricks Serverless.

Testing if pure datetime.datetime objects also trigger the error.
"""

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
from datetime import datetime


class BrokenArrowDataSource(DataSource):
    """
    Datasource that yields tuples with datetime.datetime for TimestampType.
    """

    @classmethod
    def name(cls):
        return "broken_arrow"

    def schema(self):
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("region", StringType(), True),
            StructField("demand", DoubleType(), True),
        ])

    def reader(self, schema):
        return BrokenReader(schema, self.options)


class BrokenReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def partitions(self):
        return [InputPartition(value=None)]

    def read(self, partition: InputPartition):
        # All rows use pure datetime.datetime objects
        yield (datetime(2026, 1, 28, 0, 5), "NSW1", 7500.5)
        yield (datetime(2026, 1, 28, 0, 10), "VIC1", 5200.3)
        yield (datetime(2026, 1, 28, 0, 15), "QLD1", 6100.8)
