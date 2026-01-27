# Testing Arrow Datasource with Databricks Connect Serverless

This guide explains how to test the Arrow datasource fixes for Serverless compatibility from your local machine.

## Prerequisites

1. **Python 3.12** (or compatible version)
2. **Databricks Connect** installed
3. **Databricks workspace** with Serverless compute enabled
4. **Authentication** configured

## Quick Start

### Step 1: Install Databricks Connect

```bash
pip install "databricks-connect==17.3.*"
```

### Step 2: Configure Authentication

**Option A: OAuth (Recommended)**

```bash
databricks auth login --host <your-workspace-url>
# Example: databricks auth login --host https://dbc-a1b2345c-d6e7.cloud.databricks.com
```

**Option B: Environment Variables**

```bash
export DATABRICKS_HOST=<your-workspace-url>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

### Step 3: Configure Serverless Compute

Edit `~/.databrickscfg` (or `%USERPROFILE%\.databrickscfg` on Windows):

```ini
[DEFAULT]
host                  = https://your-workspace.cloud.databricks.com
auth_type             = databricks-cli
serverless_compute_id = auto
```

### Step 4: Run the Test Script

```bash
cd databricks-nemweb-lab
python test_serverless_arrow.py
```

## What the Test Does

The test script (`test_serverless_arrow.py`) performs the following checks:

1. **Creates Spark Session**: Connects to Databricks Serverless compute
2. **Registers Datasource**: Registers the Arrow datasource with Spark
3. **Reads Data**: Fetches a small date range (2024-01-01 to 2024-01-02) for NSW1 region
4. **Verifies Types**: Checks that timestamps are pure Python `datetime.datetime` objects
5. **Tests Coercion**: Validates that pandas/numpy types are coerced correctly

## Expected Output

On success, you should see:

```
======================================================================
Testing Arrow Datasource with Databricks Connect Serverless
======================================================================

1. Creating Spark session with Serverless compute...
   ✓ Spark session created successfully

2. Registering Arrow datasource...
   ✓ Datasource registered successfully

3. Testing HTTP mode (fetching small date range)...
   ✓ Successfully read 288 rows

   Sample data:
   +-------------------+--------+-----------+----+
   |SETTLEMENTDATE     |REGIONID|TOTALDEMAND|RRP |
   +-------------------+--------+-----------+----+
   |2024-01-01 00:05:00|NSW1    |7500.5    |50.2|
   ...

4. Verifying timestamp types...
   ✓ Timestamp type: datetime
   ✓ Timestamp value: 2024-01-01 00:05:00
   ✓ Timestamp is pure Python datetime (correct!)

5. Checking Arrow configuration...
   Arrow enabled: true
   ✓ Arrow fast path is enabled (this is where type checks happen)

======================================================================
Test completed successfully!
======================================================================
```

## Troubleshooting

### Error: "databricks-connect not installed"

```bash
pip install "databricks-connect==17.3.*"
```

### Error: "Failed to create Spark session"

1. Check authentication:
   ```bash
   databricks auth login --host <workspace-url>
   ```

2. Verify Serverless is enabled in your workspace

3. Check your `.databrickscfg` file has `serverless_compute_id = auto`

### Error: "Could not import Arrow datasource"

Make sure you're running from the `databricks-nemweb-lab` directory:

```bash
cd databricks-nemweb-lab
python test_serverless_arrow.py
```

### Error: "assert isinstance(value, datetime.datetime)"

This is the exact error we're fixing! If you see this:

1. Check that you've installed the updated code with coercion fixes
2. Verify the `_to_python_scalar()` function is being called
3. Check the debug logs at `/tmp/nemweb_debug.log` on the worker

### Arrow Fast Path Disabled

If Arrow is disabled, the type coercion won't be tested. To enable it:

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

## Testing Type Coercion Locally

The test script also includes local type coercion tests that don't require a Databricks connection:

```python
# Test pandas Timestamp → datetime.datetime
pd_ts = pd.Timestamp("2024-01-01 12:00:00")
coerced = reader._to_python_scalar(pd_ts, TimestampType())
assert isinstance(coerced, datetime.datetime)

# Test numpy.datetime64 → datetime.datetime
np_ts = np.datetime64("2024-01-01T12:00:00")
coerced = reader._to_python_scalar(np_ts, TimestampType())
assert isinstance(coerced, datetime.datetime)

# Test numpy.float64 → float
np_float = np.float64(123.45)
coerced = reader._to_python_scalar(np_float, DoubleType())
assert isinstance(coerced, float)
assert not isinstance(coerced, np.floating)
```

## Manual Testing in Databricks Notebook

You can also test directly in a Databricks notebook:

```python
# Install the package
%pip install -e /Workspace/Repos/your-repo/databricks-nemweb-lab/src

# Register datasource
from nemweb_datasource_arrow import NemwebArrowDataSource
spark.dataSource.register(NemwebArrowDataSource)

# Test with Arrow enabled (default on Serverless)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Read data
df = (spark.read
      .format("nemweb_arrow")
      .option("table", "DISPATCHREGIONSUM")
      .option("start_date", "2024-01-01")
      .option("end_date", "2024-01-02")
      .option("regions", "NSW1")
      .load())

# Verify types
df.show(5)
df.printSchema()

# Check timestamp type
from pyspark.sql.functions import col
sample = df.select("SETTLEMENTDATE").first()
print(f"Type: {type(sample[0])}")
print(f"Value: {sample[0]}")
```

## References

- [Databricks Connect Serverless Tutorial](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/tutorial-serverless)
- [Python Data Source API Documentation](https://docs.databricks.com/en/pyspark/datasources.html)
- [Arrow Fast Path Documentation](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html)
