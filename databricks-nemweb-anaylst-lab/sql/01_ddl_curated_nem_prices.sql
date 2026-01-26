-- =============================================================================
-- DDL: curated_nem_prices
-- Purpose: Clean, analytics-friendly NEM price and demand data for demos
-- =============================================================================

-- Create catalog and schema if not exists
CREATE CATALOG IF NOT EXISTS agldata;
CREATE SCHEMA IF NOT EXISTS agldata.trading;

-- Create the curated NEM prices table
CREATE TABLE IF NOT EXISTS agldata.trading.curated_nem_prices (
  interval_start TIMESTAMP COMMENT 'Start of NEM dispatch interval (5-minute granularity)',
  date DATE COMMENT 'Calendar date derived from interval_start',
  region_id STRING COMMENT 'NEM region identifier: NSW1, QLD1, VIC1, SA1, TAS1',
  rrp DOUBLE COMMENT 'Regional Reference Price in $/MWh - the wholesale spot price',
  demand_mw DOUBLE COMMENT 'Scheduled demand in megawatts for the region',
  is_peak BOOLEAN COMMENT 'True if interval falls within peak hours (17:00-21:00 local time)'
)
USING DELTA
COMMENT 'Curated NEM wholesale electricity prices and demand by region. Source: AEMO NEMWeb DISPATCHREGIONSUM. Updated every 5 minutes.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Add table-level documentation for Genie context
ALTER TABLE agldata.trading.curated_nem_prices SET TBLPROPERTIES (
  'data_domain' = 'trading',
  'data_owner' = 'Trading Analytics',
  'refresh_frequency' = '5 minutes',
  'source_system' = 'AEMO NEMWeb'
);

-- =============================================================================
-- Sample data load (from CSV)
-- Run this after uploading sample_curated_nem_prices.csv to a volume
-- =============================================================================

-- Option 1: Load from Unity Catalog Volume
-- COPY INTO agldata.trading.curated_nem_prices
-- FROM '/Volumes/agldata/trading/landing/sample_curated_nem_prices.csv'
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- Option 2: Load from DBFS
-- COPY INTO agldata.trading.curated_nem_prices
-- FROM 'dbfs:/FileStore/sample_curated_nem_prices.csv'
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- Verify load
-- SELECT region_id, COUNT(*) as rows, MIN(date) as min_date, MAX(date) as max_date
-- FROM agldata.trading.curated_nem_prices
-- GROUP BY region_id;
