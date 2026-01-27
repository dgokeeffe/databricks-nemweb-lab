-- =============================================================================
-- DDL: curated_nem_prices
-- Purpose: Clean, analytics-friendly NEM price and demand data
-- Source: Real AEMO data loaded by 00_setup_and_validation.py
-- =============================================================================

-- Use the same catalog/schema as the data engineering workshop
-- This ensures the analyst workshop uses the real AEMO data
USE CATALOG workspace;
USE SCHEMA nemweb_lab;

-- =============================================================================
-- Option 1: Create a VIEW that joins prices + demand (recommended for demos)
-- Benefits: Always up-to-date, no data duplication, no maintenance
-- =============================================================================

CREATE OR REPLACE VIEW curated_nem_prices
COMMENT 'Curated NEM wholesale electricity prices and demand by region. Joins DISPATCHPRICE (RRP) with DISPATCHREGIONSUM (demand). Source: AEMO NEMWeb.'
AS
SELECT
  -- Timestamp fields
  to_timestamp(p.SETTLEMENTDATE) AS interval_start,
  DATE(to_timestamp(p.SETTLEMENTDATE)) AS date,

  -- Region
  p.REGIONID AS region_id,

  -- Price from DISPATCHPRICE
  p.RRP AS rrp,

  -- Demand from DISPATCHREGIONSUM
  r.TOTALDEMAND AS demand_mw,

  -- Peak hours: 17:00-21:00 (typical NEM peak definition)
  HOUR(to_timestamp(p.SETTLEMENTDATE)) BETWEEN 17 AND 20 AS is_peak

FROM nemweb_dispatch_prices p
INNER JOIN nemweb_dispatch_regionsum r
  ON p.SETTLEMENTDATE = r.SETTLEMENTDATE
  AND p.REGIONID = r.REGIONID
WHERE p.REGIONID IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1')
  AND p.RRP IS NOT NULL
  AND r.TOTALDEMAND IS NOT NULL;

-- Add table-level documentation for Genie context
-- Note: View properties require ALTER VIEW in some DBR versions
COMMENT ON VIEW curated_nem_prices IS
'Curated NEM wholesale electricity prices and demand by region.
Columns:
- interval_start: NEM dispatch interval (5-minute granularity)
- date: Calendar date
- region_id: NEM region (NSW1, QLD1, VIC1, SA1, TAS1)
- rrp: Regional Reference Price in $/MWh (wholesale spot price)
- demand_mw: Scheduled demand in megawatts
- is_peak: True if interval is peak hours (17:00-21:00)

Source: AEMO NEMWeb DISPATCHPRICE + DISPATCHREGIONSUM tables.
Updated: Real-time from source tables loaded by setup notebook.';


-- =============================================================================
-- Option 2: Create a TABLE (if you need separate catalog/schema)
-- Use this if you want to store in agldata.trading instead
-- =============================================================================

-- Uncomment below if you need a separate table in a different catalog:

-- CREATE CATALOG IF NOT EXISTS agldata;
-- CREATE SCHEMA IF NOT EXISTS agldata.trading;

-- CREATE OR REPLACE TABLE agldata.trading.curated_nem_prices
-- USING DELTA
-- COMMENT 'Curated NEM wholesale electricity prices and demand by region'
-- TBLPROPERTIES (
--   'delta.autoOptimize.optimizeWrite' = 'true',
--   'delta.autoOptimize.autoCompact' = 'true',
--   'data_domain' = 'trading',
--   'data_owner' = 'Trading Analytics',
--   'source_system' = 'AEMO NEMWeb'
-- )
-- AS
-- SELECT
--   to_timestamp(p.SETTLEMENTDATE) AS interval_start,
--   DATE(to_timestamp(p.SETTLEMENTDATE)) AS date,
--   p.REGIONID AS region_id,
--   p.RRP AS rrp,
--   r.TOTALDEMAND AS demand_mw,
--   HOUR(to_timestamp(p.SETTLEMENTDATE)) BETWEEN 17 AND 20 AS is_peak
-- FROM workspace.nemweb_lab.nemweb_dispatch_prices p
-- INNER JOIN workspace.nemweb_lab.nemweb_dispatch_regionsum r
--   ON p.SETTLEMENTDATE = r.SETTLEMENTDATE
--   AND p.REGIONID = r.REGIONID
-- WHERE p.REGIONID IN ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1');


-- =============================================================================
-- Verify the view
-- =============================================================================

SELECT
  region_id,
  COUNT(*) as rows,
  MIN(date) as min_date,
  MAX(date) as max_date,
  ROUND(AVG(rrp), 2) as avg_price,
  ROUND(AVG(demand_mw), 0) as avg_demand
FROM curated_nem_prices
GROUP BY region_id
ORDER BY region_id;
