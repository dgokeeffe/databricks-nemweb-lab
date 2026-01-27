# Sample Data (Fallback Only)

**Primary data source:** Real AEMO data loaded by the data engineering workshop.

The CSV file in this directory is **fallback synthetic data** for offline demos or testing.

## Recommended Approach

1. **Run the setup notebook first**:
   ```
   databricks-nemweb-lab/exercises/00_setup_and_validation.py
   ```

2. **Create the curated view**:
   ```sql
   -- Run sql/01_ddl_curated_nem_prices.sql
   -- This creates a view joining real AEMO price + demand data
   ```

This gives you **6 months of real AEMO data** instead of 7 days of synthetic data.

---

## Fallback: Loading Synthetic Data

Only use this if you can't run the data engineering setup.

### File: sample_curated_nem_prices.csv

- **Rows**: ~10,080 (7 days × 288 intervals × 5 regions)
- **Date Range**: 2025-01-18 to 2025-01-24
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

### Loading from Volume

```sql
-- Create schema and volume
CREATE SCHEMA IF NOT EXISTS workspace.nemweb_lab;
CREATE VOLUME IF NOT EXISTS workspace.nemweb_lab.landing;

-- Upload CSV to /Volumes/workspace/nemweb_lab/landing/ via UI

-- Load as table (not view)
CREATE OR REPLACE TABLE workspace.nemweb_lab.curated_nem_prices AS
SELECT
  to_timestamp(interval_start) as interval_start,
  to_date(date) as date,
  region_id,
  CAST(rrp AS DOUBLE) as rrp,
  CAST(demand_mw AS DOUBLE) as demand_mw,
  CAST(is_peak AS BOOLEAN) as is_peak
FROM csv.`/Volumes/workspace/nemweb_lab/landing/sample_curated_nem_prices.csv`
OPTIONS (header = 'true');
```

### Loading from DBFS

```sql
-- Upload CSV via UI: Data > Add Data > Upload File
-- Then load:
CREATE OR REPLACE TABLE workspace.nemweb_lab.curated_nem_prices AS
SELECT
  to_timestamp(interval_start) as interval_start,
  to_date(date) as date,
  region_id,
  CAST(rrp AS DOUBLE) as rrp,
  CAST(demand_mw AS DOUBLE) as demand_mw,
  CAST(is_peak AS BOOLEAN) as is_peak
FROM csv.`dbfs:/FileStore/sample_curated_nem_prices.csv`
OPTIONS (header = 'true');
```

## Verify Load

```sql
SELECT
  region_id,
  COUNT(*) as rows,
  MIN(date) as min_date,
  MAX(date) as max_date,
  ROUND(AVG(rrp), 2) as avg_price
FROM workspace.nemweb_lab.curated_nem_prices
GROUP BY region_id;
```

## Notes

- This is synthetic data based on realistic NEM patterns
- Price range: $17-495/MWh with occasional spikes
- SA1 has highest volatility, TAS1 lowest
- Data does not contain any confidential trading positions
