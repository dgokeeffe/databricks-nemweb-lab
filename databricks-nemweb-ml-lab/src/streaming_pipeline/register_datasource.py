"""Register the custom nemweb_stream PySpark data source.

This file is loaded BEFORE the transformation glob to ensure
spark.dataSource.register() runs before any readStream calls.
"""

from databricks.sdk.runtime import spark
from nemweb_datasource_stream import NemwebStreamDataSource

spark.dataSource.register(NemwebStreamDataSource)

print(f"Registered nemweb_stream data source")
