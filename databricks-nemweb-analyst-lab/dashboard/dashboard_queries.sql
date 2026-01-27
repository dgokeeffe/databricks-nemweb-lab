-- =============================================================================
-- Dashboard Queries for NEM Price Analytics
-- Use these queries to build a Databricks SQL Dashboard
-- =============================================================================

-- =============================================================================
-- Widget 1: KPI - Average Price (Last 7 Days)
-- Visualization: Counter
-- =============================================================================
SELECT
  ROUND(AVG(rrp), 2) AS avg_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS;


-- =============================================================================
-- Widget 2: KPI - Peak Demand (Last 7 Days)
-- Visualization: Counter
-- =============================================================================
SELECT
  ROUND(MAX(demand_mw), 0) AS peak_demand_mw
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS;


-- =============================================================================
-- Widget 3: KPI - Price Spikes Count (>$300)
-- Visualization: Counter
-- =============================================================================
SELECT
  COUNT(*) AS spike_count
FROM workspace.nemweb_lab.curated_nem_prices
WHERE rrp > 300
  AND date >= current_date() - INTERVAL 7 DAYS;


-- =============================================================================
-- Widget 4: Daily Average Price by Region
-- Visualization: Line Chart
-- X-axis: date, Y-axis: avg_price, Color: region_id
-- =============================================================================
SELECT
  date,
  region_id,
  ROUND(AVG(rrp), 2) AS avg_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 30 DAYS
GROUP BY date, region_id
ORDER BY date, region_id;


-- =============================================================================
-- Widget 5: Regional Summary Table
-- Visualization: Table
-- =============================================================================
SELECT
  region_id AS Region,
  COUNT(*) AS Intervals,
  ROUND(AVG(rrp), 2) AS `Avg Price ($/MWh)`,
  ROUND(MAX(rrp), 2) AS `Max Price`,
  ROUND(STDDEV(rrp), 2) AS `Volatility`,
  ROUND(AVG(demand_mw), 0) AS `Avg Demand (MW)`,
  ROUND(MAX(demand_mw), 0) AS `Peak Demand (MW)`
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id
ORDER BY `Avg Price ($/MWh)` DESC;


-- =============================================================================
-- Widget 6: Price Volatility by Region
-- Visualization: Bar Chart
-- X-axis: region_id, Y-axis: volatility
-- =============================================================================
SELECT
  region_id,
  ROUND(STDDEV(rrp), 2) AS volatility
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id
ORDER BY volatility DESC;


-- =============================================================================
-- Widget 7: Peak vs Off-Peak Comparison
-- Visualization: Grouped Bar Chart
-- X-axis: region_id, Y-axis: avg_price, Color: period
-- =============================================================================
SELECT
  region_id,
  CASE WHEN is_peak THEN 'Peak (17:00-21:00)' ELSE 'Off-Peak' END AS period,
  ROUND(AVG(rrp), 2) AS avg_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id, is_peak
ORDER BY region_id, is_peak DESC;


-- =============================================================================
-- Widget 8: Hourly Price Pattern
-- Visualization: Heatmap or Line Chart
-- X-axis: hour, Y-axis: avg_price (or region for heatmap)
-- =============================================================================
SELECT
  HOUR(interval_start) AS hour_of_day,
  region_id,
  ROUND(AVG(rrp), 2) AS avg_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY HOUR(interval_start), region_id
ORDER BY hour_of_day, region_id;


-- =============================================================================
-- Widget 9: Price Spike Events
-- Visualization: Table
-- =============================================================================
SELECT
  interval_start AS `Time`,
  region_id AS `Region`,
  ROUND(rrp, 2) AS `Price ($/MWh)`,
  ROUND(demand_mw, 0) AS `Demand (MW)`,
  is_peak AS `Peak Hour`
FROM workspace.nemweb_lab.curated_nem_prices
WHERE rrp > 200
  AND date >= current_date() - INTERVAL 7 DAYS
ORDER BY rrp DESC
LIMIT 20;


-- =============================================================================
-- Widget 10: Demand by Region (Latest)
-- Visualization: Bar Chart
-- =============================================================================
WITH latest AS (
  SELECT
    region_id,
    demand_mw,
    ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY interval_start DESC) AS rn
  FROM workspace.nemweb_lab.curated_nem_prices
)
SELECT
  region_id,
  ROUND(demand_mw, 0) AS current_demand_mw
FROM latest
WHERE rn = 1
ORDER BY current_demand_mw DESC;
