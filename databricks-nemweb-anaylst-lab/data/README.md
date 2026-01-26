# Sample Data Loading Instructions

This directory contains synthetic NEM price and demand data for the AGL Trading Analytics demo.

## File: sample_curated_nem_prices.csv

- **Rows**: ~10,080 (7 days x 288 intervals x 5 regions)
- **Date Range**: 7 days of 5-minute dispatch intervals
- **Regions**: NSW1, QLD1, VIC1, SA1, TAS1

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `interval_start` | TIMESTAMP | Start of NEM dispatch interval |
| `date` | DATE | Calendar date |
| `region_id` | STRING | NEM region identifier |
| `rrp` | DOUBLE | Regional Reference Price ($/MWh) |
| `demand_mw` | DOUBLE | Scheduled demand (MW) |
| `is_peak` | BOOLEAN | True if 17:00-21:00 |

### Data Characteristics

- Base prices: $45-60/MWh depending on region
- Peak hours (17:00-21:00): ~40% higher prices
- Occasional price spikes up to 15x base
- SA1 has highest volatility, TAS1 lowest
- Realistic intraday demand patterns

## Loading Options

### Option 1: Unity Catalog Volume (Recommended)

1. Create a volume in Unity Catalog:
   ```sql
   CREATE VOLUME IF NOT EXISTS agldata.trading.landing;
   ```

2. Upload the CSV file to the volume via Catalog Explorer UI or CLI:
   ```bash
   databricks fs cp sample_curated_nem_prices.csv dbfs:/Volumes/agldata/trading/landing/
   ```

3. Load into the table:
   ```sql
   COPY INTO agldata.trading.curated_nem_prices
   FROM '/Volumes/agldata/trading/landing/sample_curated_nem_prices.csv'
   FILEFORMAT = CSV
   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
   ```

### Option 2: DBFS Upload

1. Upload via Databricks UI: Data > Add Data > Upload File

2. Load from DBFS:
   ```sql
   COPY INTO agldata.trading.curated_nem_prices
   FROM 'dbfs:/FileStore/sample_curated_nem_prices.csv'
   FILEFORMAT = CSV
   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
   ```

### Option 3: Direct SQL Insert (Small datasets)

For very small test datasets, use INSERT statements from the SQL Editor.

## Verification

After loading, verify the data:

```sql
-- Check row counts by region
SELECT region_id, COUNT(*) as rows, MIN(date) as min_date, MAX(date) as max_date
FROM agldata.trading.curated_nem_prices
GROUP BY region_id
ORDER BY region_id;

-- Check price distribution
SELECT
  region_id,
  ROUND(AVG(rrp), 2) as avg_price,
  ROUND(MIN(rrp), 2) as min_price,
  ROUND(MAX(rrp), 2) as max_price
FROM agldata.trading.curated_nem_prices
GROUP BY region_id;
```

## Notes

- This is synthetic data based on realistic NEM patterns
- For production, replace with actual NEMWeb data via the AEMO API
- Data does not contain any confidential AGL trading positions
