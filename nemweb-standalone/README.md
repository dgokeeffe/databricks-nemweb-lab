# NEMWEB Standalone Data Loader

A simple, dependency-free utility for fetching AEMO NEMWEB electricity market data.

## Features

- **Zero dependencies** - uses only Python 3.9+ stdlib
- **Python API** - clean, simple interface
- **CLI** - fetch data from the command line
- **Optional DataFrame output** - pandas or polars if installed
- **Offline mode** - sample data for testing without network

## Installation

Just copy `nemweb.py` to your project - no installation required.

```bash
# Option 1: Copy the file
cp nemweb.py /path/to/your/project/

# Option 2: Download directly
curl -O https://raw.githubusercontent.com/.../standalone/nemweb.py
```

## Python API

```python
from nemweb import fetch, list_tables, NEM_REGIONS

# List available tables
for table in list_tables():
    print(f"{table['name']}: {table['description']}")

# Fetch last hour of price data (default)
data = fetch("DISPATCHPRICE")

# Fetch last 6 hours
data = fetch("DISPATCHPRICE", hours=6)

# Fetch last 7 days (uses ARCHIVE - slower but more data)
data = fetch("DISPATCHREGIONSUM", days=7)

# Specific date range
data = fetch("DISPATCHPRICE", start_date="2024-01-01", end_date="2024-01-07")

# Filter by regions
data = fetch("DISPATCHPRICE", hours=1, regions=["NSW1", "VIC1"])

# Sample data (no network required)
data = fetch("DISPATCHPRICE", sample=True)

# Get as pandas DataFrame (if pandas installed)
df = fetch("DISPATCHPRICE", hours=1, as_pandas=True)

# Get as polars DataFrame (if polars installed)
df = fetch("DISPATCHPRICE", hours=1, as_polars=True)
```

## CLI usage

```bash
# Fetch last hour of prices (JSON to stdout)
python nemweb.py DISPATCHPRICE --hours 1

# Fetch last 7 days, save as CSV
python nemweb.py DISPATCHPRICE --days 7 --output prices.csv

# Specific date range for NSW and VIC
python nemweb.py DISPATCHREGIONSUM --start 2024-01-01 --end 2024-01-07 --regions NSW1,VIC1

# List available tables
python nemweb.py --list-tables

# Sample data (offline testing)
python nemweb.py DISPATCHPRICE --sample

# Verbose output
python nemweb.py DISPATCHPRICE --hours 1 -v
```

## Available tables

| Table | Description |
|-------|-------------|
| `DISPATCHPRICE` | Regional spot prices - RRP (5-min) |
| `DISPATCHREGIONSUM` | Regional demand, generation, interchange (5-min) |
| `TRADINGPRICE` | Trading period prices (30-min) |
| `DISPATCH_UNIT_SCADA` | Real-time unit generation per DUID (5-min) |
| `DISPATCH_REGION` | Comprehensive dispatch with FCAS prices (5-min) |
| `DISPATCH_INTERCONNECTOR` | Interconnector dispatch details (5-min) |
| `DISPATCH_INTERCONNECTOR_TRADING` | Metered interconnector flows (5-min) |
| `ROOFTOP_PV_ACTUAL` | Rooftop solar generation estimates |

## NEM regions

- `NSW1` - New South Wales
- `VIC1` - Victoria
- `QLD1` - Queensland
- `SA1` - South Australia
- `TAS1` - Tasmania

## Data sources

- **CURRENT** (default) - Recent 5-minute interval files (~7 days available)
- **ARCHIVE** - Daily consolidated files (historical data)

The utility automatically selects CURRENT for `hours` and ARCHIVE for `days`/date ranges.

## Use in Databricks

```python
# In a Databricks notebook - no custom datasource required
%pip install pandas  # Optional

import sys
sys.path.append("/Workspace/path/to/standalone/")

from nemweb import fetch

# Fetch as list of dicts
data = fetch("DISPATCHPRICE", hours=1)

# Convert to Spark DataFrame
df = spark.createDataFrame(data)
display(df)

# Or use pandas
df = fetch("DISPATCHPRICE", hours=1, as_pandas=True)
spark_df = spark.createDataFrame(df)
```

## NEMWEB data format

NEMWEB CSV files use a multi-record format:

```
C,... = Comment/metadata row
I,CATEGORY,RECORD_TYPE,VERSION,COL1,COL2,... = Header row
D,CATEGORY,RECORD_TYPE,VERSION,VAL1,VAL2,... = Data row
```

The utility handles this automatically, extracting only the relevant record type for each table.

## Error handling

```python
from nemweb import fetch
from urllib.error import HTTPError

try:
    data = fetch("DISPATCHPRICE", days=1)
except ValueError as e:
    print(f"Invalid table: {e}")
except HTTPError as e:
    if e.code == 404:
        print("Data not available for this date")
    else:
        print(f"Network error: {e}")
```

## License

MIT License - use freely for any purpose.
