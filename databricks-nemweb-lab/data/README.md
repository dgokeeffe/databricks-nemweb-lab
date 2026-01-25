# Sample NEMWEB Data

This folder contains sample data for offline development and testing.

## Files

### sample_nemweb_data.csv

Sample dispatch region summary data from the DISPATCHREGIONSUM table.

**Schema:**
| Column | Type | Description |
|--------|------|-------------|
| SETTLEMENTDATE | timestamp | Trading interval timestamp |
| RUNNO | string | Dispatch run number |
| REGIONID | string | NEM region (NSW1, VIC1, QLD1, SA1, TAS1) |
| DISPATCHINTERVAL | string | 5-minute interval number (1-288) |
| INTERVENTION | string | Intervention flag (0 or 1) |
| TOTALDEMAND | double | Total regional demand (MW) |
| AVAILABLEGENERATION | double | Available generation capacity (MW) |
| AVAILABLELOAD | double | Available load (MW) |
| DEMANDFORECAST | double | Forecasted demand (MW) |
| DISPATCHABLEGENERATION | double | Dispatchable generation (MW) |
| DISPATCHABLELOAD | double | Dispatchable load (MW) |
| NETINTERCHANGE | double | Net flow to/from region (MW, negative = export) |

**Coverage:**
- Date: 2024-01-01
- Intervals: First 6 trading intervals (30 minutes)
- Regions: All 5 NEM regions
- Rows: 30 total

## Data Source

Real NEMWEB data is available from:
- Current: https://www.nemweb.com.au/REPORTS/CURRENT/
- Archive: https://www.nemweb.com.au/REPORTS/ARCHIVE/

## Schema Reference

Full table schemas are documented in the MMS Electricity Data Model Report:
https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report.htm

## Usage

```python
# In Databricks
df = spark.read.csv("/path/to/sample_nemweb_data.csv", header=True, inferSchema=True)

# Or with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("SETTLEMENTDATE", TimestampType(), True),
    StructField("RUNNO", StringType(), True),
    StructField("REGIONID", StringType(), True),
    # ... etc
])

df = spark.read.csv("/path/to/sample_nemweb_data.csv", header=True, schema=schema)
```
