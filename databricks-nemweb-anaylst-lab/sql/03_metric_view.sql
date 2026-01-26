-- =============================================================================
-- Metric View: mv_nem_price_metrics
-- Purpose: Semantic layer for NEM price analytics - defines measures and
--          dimensions used consistently across Genie, dashboards, and Power BI
-- Requires: SQL Warehouse or DBR 17.2+ with Unity Catalog
-- =============================================================================

-- Create metrics schema if preferred (optional - can use trading schema)
CREATE SCHEMA IF NOT EXISTS agldata.metrics;

-- Create the metric view with YAML syntax
CREATE OR REPLACE VIEW agldata.trading.mv_nem_price_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "NEM wholesale electricity analytics - standardised measures for price, volatility, and demand across Genie, SQL, dashboards, and Power BI"
source: agldata.trading.curated_nem_prices

dimensions:
  - name: region_id
    expr: region_id
    comment: "NEM region identifier (NSW1, QLD1, VIC1, SA1, TAS1)"

  - name: date
    expr: date
    comment: "Calendar date of the trading interval"

  - name: hour_of_day
    expr: HOUR(interval_start)
    comment: "Hour of day (0-23) for intraday analysis"

  - name: day_of_week
    expr: DATE_FORMAT(date, 'EEEE')
    comment: "Day of week name for weekly patterns"

  - name: is_peak
    expr: is_peak
    comment: "Peak period indicator (17:00-21:00)"

  - name: month
    expr: DATE_FORMAT(date, 'yyyy-MM')
    comment: "Month for seasonal analysis"

measures:
  - name: avg_price
    expr: AVG(rrp)
    comment: "Average Regional Reference Price ($/MWh)"

  - name: price_volatility
    expr: STDDEV(rrp)
    comment: "Standard deviation of price - measures price variability"

  - name: peak_price
    expr: MAX(rrp)
    comment: "Maximum price observed ($/MWh)"

  - name: min_price
    expr: MIN(rrp)
    comment: "Minimum price observed ($/MWh)"

  - name: avg_demand_mw
    expr: AVG(demand_mw)
    comment: "Average scheduled demand in megawatts"

  - name: peak_demand_mw
    expr: MAX(demand_mw)
    comment: "Maximum demand observed (MW)"

  - name: total_demand_mwh
    expr: SUM(demand_mw) / 12
    comment: "Total energy demand (MW * 5min intervals / 12)"

  - name: interval_count
    expr: COUNT(*)
    comment: "Number of 5-minute intervals"

  - name: price_range
    expr: MAX(rrp) - MIN(rrp)
    comment: "Difference between max and min price"
$$;

-- =============================================================================
-- Example: Query the metric view
-- =============================================================================

-- Basic usage: measures are calculated based on dimensions in SELECT
SELECT
  region_id,
  date,
  avg_price,
  price_volatility,
  peak_demand_mw
FROM agldata.trading.mv_nem_price_metrics
WHERE date >= current_date() - INTERVAL 7 DAYS
ORDER BY avg_price DESC;

-- With hour dimension for intraday patterns
SELECT
  region_id,
  hour_of_day,
  avg_price,
  avg_demand_mw
FROM agldata.trading.mv_nem_price_metrics
WHERE date >= current_date() - INTERVAL 7 DAYS
ORDER BY hour_of_day;

-- Peak vs Off-Peak comparison using is_peak dimension
SELECT
  region_id,
  is_peak,
  avg_price,
  price_volatility,
  peak_demand_mw
FROM agldata.trading.mv_nem_price_metrics
WHERE date >= current_date() - INTERVAL 7 DAYS
ORDER BY region_id, is_peak DESC;
