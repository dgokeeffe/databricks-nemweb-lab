# Arrow Serialization Error Reproduction

Minimal package to reproduce the Arrow serialization error on Databricks Serverless.

## The Error

When a custom PySpark datasource returns an invalid type for `TimestampType`, Serverless fails with:

```
File "/databricks/spark/python/_engine_pyspark.zip/_engine_pyspark/sql/conversion.py", line 307, in convert_timestamp
    assert isinstance(value, datetime.datetime)
AssertionError
```

## Root Cause

The `convert_timestamp` function in Spark's Arrow serialization path asserts that values for `TimestampType` columns are `datetime.datetime` objects. If you return a string (e.g., when timestamp parsing fails), the assertion fails.

## Build

```bash
cd repro_arrow_package
uv build --wheel --out-dir dist
```

## Upload to Databricks

```bash
databricks fs cp dist/arrow_repro-*.whl dbfs:/Volumes/catalog/schema/volume/
databricks sync . /Workspace/Users/you@company.com/repro_arrow_package
```

## Test Cases

| Version | `broken_arrow` behavior | Result |
|---------|------------------------|--------|
| 0.1.5 | Returns STRING for timestamp | **FAILS** with AssertionError |
| 0.1.6 | Returns datetime.datetime | TBD |

## The Fix

Always return `datetime.datetime` or `None` for `TimestampType` columns:

```python
def _convert_value(value, spark_type):
    if isinstance(spark_type, TimestampType):
        try:
            return datetime.strptime(value, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            return None  # NOT the raw string!
```
