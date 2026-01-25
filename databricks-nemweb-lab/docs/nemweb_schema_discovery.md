# NEMWEB Schema Discovery Guide

This guide explains how to discover and derive schemas from AEMO NEMWEB data sources.

## Overview

NEMWEB provides electricity market data through multiple interfaces:
- **Current:** Recent files (last 7 days)
- **Archive:** Historical files (older than 7 days)
- **MMSDM:** Monthly historical packages

The schema for each file type is defined in the MMS (Market Management System) Electricity Data Model.

## Key Documentation Sources

### 1. NEMWEB Market Data Landing Page

**URL:** https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb

**What you'll find:**
- Overview of available datasets
- Links to Current, Archive, and MMSDM directories
- Data categories: Dispatch, Pre-dispatch, Bids, Settlements, etc.

### 2. MMS Electricity Data Model Report

**URL:** https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report.htm

**What you'll find:**
- Complete table definitions for all MMS tables
- Column names, data types, and descriptions
- Primary keys and relationships

**How to use:**
1. Navigate to the table you need (e.g., DISPATCHREGIONSUM)
2. Note all column names and types
3. Map to Spark types (see mapping table below)

### 3. Nemweb Help & Table Mappings

**Key insight:** File prefixes map to MMS tables:

| File Prefix | MMS Table | Description |
|-------------|-----------|-------------|
| PUBLIC_DISPATCHSCADA | DISPATCH_UNIT_SCADA | Unit-level SCADA data |
| PUBLIC_DISPATCHPRICE | DISPATCHPRICE | Regional dispatch prices |
| PUBLIC_DISPATCHREGIONSUM | DISPATCHREGIONSUM | Regional dispatch summary |
| PUBLIC_TRADINGPRICE | TRADINGPRICE | Trading interval prices |
| PUBLIC_ROOFTOPPV_ACTUAL | ROOFTOP_PV_ACTUAL | Rooftop PV estimates |

### 4. Data Interchange Framework

Explains relationships between:
- **Current:** Rolling 7-day window, files every 5 minutes
- **Archive:** Compressed historical files, daily granularity
- **MMSDM:** Monthly packages with all tables

**Naming conventions:**
- `PUBLIC_{TABLE}_{YYYYMMDDHHMM}.CSV` (Current)
- `PUBLIC_{TABLE}_{YYYYMMDD}.zip` (Archive)

## Schema Derivation Process

### Step 1: Identify the Table

Determine which MMS table you need based on your use case:

| Use Case | Table | Key Columns |
|----------|-------|-------------|
| Regional demand | DISPATCHREGIONSUM | TOTALDEMAND, REGIONID |
| Spot prices | DISPATCHPRICE | RRP, REGIONID |
| Generator output | DISPATCH_UNIT_SCADA | SCADAVALUE, DUID |
| Trading prices | TRADINGPRICE | RRP, PERIODID |
| Interconnector flows | DISPATCHINTERCONNECTORRES | MWFLOW, INTERCONNECTORID |

### Step 2: Find Table Definition

Navigate to MMS Data Model Report > Search for table name

Example for DISPATCHREGIONSUM:
- Package: DISPATCH
- Table: DISPATCHREGIONSUM
- Primary key: SETTLEMENTDATE, RUNNO, REGIONID, DISPATCHINTERVAL, INTERVENTION

### Step 3: Map to Spark Types

| MMS Type | Spark Type | Notes |
|----------|------------|-------|
| varchar2(n) | StringType | Fixed-length strings |
| number(p,s) | DoubleType | Numeric with decimals |
| number(p) | IntegerType/LongType | Whole numbers |
| date | TimestampType | NEMWEB uses timestamps |

### Step 4: Create Spark Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

DISPATCHREGIONSUM_SCHEMA = StructType([
    # Primary key fields
    StructField("SETTLEMENTDATE", TimestampType(), True),
    StructField("RUNNO", StringType(), True),
    StructField("REGIONID", StringType(), True),
    StructField("DISPATCHINTERVAL", StringType(), True),
    StructField("INTERVENTION", StringType(), True),

    # Demand and generation
    StructField("TOTALDEMAND", DoubleType(), True),
    StructField("AVAILABLEGENERATION", DoubleType(), True),
    StructField("DEMANDFORECAST", DoubleType(), True),

    # Frequency control ancillary services
    StructField("LOWER5MINDISPATCH", DoubleType(), True),
    StructField("LOWER60SECDISPATCH", DoubleType(), True),
    StructField("RAISE5MINDISPATCH", DoubleType(), True),
    StructField("RAISE60SECDISPATCH", DoubleType(), True),

    # ... additional fields as needed
])
```

## Common Tables Reference

### DISPATCHREGIONSUM

Regional dispatch summary, published every 5 minutes.

**Key columns:**
- `SETTLEMENTDATE`: Trading interval timestamp
- `REGIONID`: NSW1, VIC1, QLD1, SA1, TAS1
- `TOTALDEMAND`: Regional demand (MW)
- `AVAILABLEGENERATION`: Available generation (MW)
- `NETINTERCHANGE`: Net flow (negative = export)

### DISPATCHPRICE

Regional dispatch prices.

**Key columns:**
- `SETTLEMENTDATE`: Trading interval timestamp
- `REGIONID`: Region identifier
- `RRP`: Regional Reference Price ($/MWh)
- `EEP`: Excess Energy Price
- `APCFLAG`: Administered price cap flag

### DISPATCH_UNIT_SCADA

Individual generator SCADA readings.

**Key columns:**
- `SETTLEMENTDATE`: Timestamp
- `DUID`: Dispatchable Unit ID (generator identifier)
- `SCADAVALUE`: Output in MW

### TRADINGPRICE

30-minute trading interval prices.

**Key columns:**
- `SETTLEMENTDATE`: Trading period timestamp
- `REGIONID`: Region identifier
- `PERIODID`: Period within day (1-48)
- `RRP`: Regional Reference Price

## URL Patterns

### Current Directory

```
https://www.nemweb.com.au/REPORTS/CURRENT/{folder}/
```

Folders:
- `Dispatch_SCADA/` - SCADA and dispatch data
- `DispatchIS_Reports/` - Dispatch prices
- `TradingIS_Reports/` - Trading prices
- `ROOFTOP_PV/ACTUAL/` - Rooftop PV

### Archive Directory

```
https://www.nemweb.com.au/REPORTS/ARCHIVE/{folder}/{year}/
```

### MMSDM Packages

```
https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/
```

## Tips for Schema Discovery

1. **Start with sample files:** Download a sample CSV, inspect headers
2. **Cross-reference MMS model:** Verify column names and types
3. **Handle optional columns:** Some columns may be null for certain records
4. **Watch for type changes:** Occasionally AEMO updates schemas
5. **Use nullable types:** NEMWEB data can have missing values

## External Resources

### "A Hacker's Guide to AEMO & NEM Data"

**URL:** https://github.com/UNSW-CEEM/nem-data

Practical guide covering:
- How Current, Archive, and MMSDM relate
- Example code for fetching key tables
- Common gotchas and workarounds

### AEMO Data Dashboards

- https://aemo.com.au/en/energy-systems/electricity/national-electricity-market-nem/data-nem/data-dashboard-nem
- Interactive exploration of available data
- Good for understanding data meaning before schema work

## Schema Registry (Lab Reference)

The lab includes pre-defined schemas in `src/nemweb_utils.py`:

```python
from nemweb_utils import get_nemweb_schema

# Get schema for a table
schema = get_nemweb_schema("DISPATCHREGIONSUM")
print(schema)

# List available tables
from nemweb_utils import list_available_tables
print(list_available_tables())
```
