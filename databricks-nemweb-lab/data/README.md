# Sample NEMWEB Data

This folder contains sample data for offline development and testing.

## Files

### sample_dispatchis_raw.csv (Multi-Record Format)

**This is how NEMWEB data actually looks!** A single DISPATCHIS file contains
multiple record types (tables) in one CSV. Each row is prefixed with a record
type identifier.

**Record Types in DISPATCHIS Files:**
| Record Type | Description |
|-------------|-------------|
| `DISPATCH,REGIONSUM` | Regional demand and generation summary |
| `DISPATCH,PRICE` | Regional Reference Price (RRP) |
| `DISPATCH,INTERCONNECTORRES` | Interconnector flows |
| `DISPATCH,CONSTRAINT` | Network constraint results |
| `DISPATCH,CASESOLUTION` | Dispatch solution metadata |
| `DISPATCH,LOCALAREAPRICE` | Local area pricing |
| `DISPATCH,MNSPBIDTRK` | MNSP bid tracking |

**Row Prefixes:**
- `C` - Comment/header row
- `I` - Information row (column headers for a record type)
- `D` - Data row

**Example Structure:**
```
C,NEMP.WORLD,DISPATCH,LOAD,...           <- File header
I,DISPATCH,REGIONSUM,4,SETTLEMENTDATE,...  <- Column headers for REGIONSUM
D,DISPATCH,REGIONSUM,4,2025/01/22,...      <- Data row for REGIONSUM
D,DISPATCH,REGIONSUM,4,2025/01/22,...      <- Another REGIONSUM row
I,DISPATCH,PRICE,4,SETTLEMENTDATE,...      <- Column headers for PRICE
D,DISPATCH,PRICE,4,2025/01/22,...          <- Data row for PRICE
C,END OF REPORT,38                         <- File footer
```

Our custom data source parses this format and extracts the specific table you request.

---

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
