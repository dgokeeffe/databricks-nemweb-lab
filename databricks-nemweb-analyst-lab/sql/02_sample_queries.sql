-- =============================================================================
-- Sample Queries for AGL Trading Analytics Demo
-- Purpose: Demo queries showcasing curated NEM data analysis
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: 7-Day Regional Summary
-- Use: Demo Section 5.2 - showing curated data
-- -----------------------------------------------------------------------------
SELECT
  region_id,
  COUNT(*) AS interval_count,
  ROUND(AVG(rrp), 2) AS avg_price,
  ROUND(STDDEV(rrp), 2) AS price_volatility,
  ROUND(AVG(demand_mw), 0) AS avg_demand_mw,
  ROUND(MAX(demand_mw), 0) AS peak_demand_mw
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id
ORDER BY avg_price DESC;


-- -----------------------------------------------------------------------------
-- Query 2: Daily Price Trend by Region
-- Use: Demo Section 5.3 - Genie verification / line chart source
-- -----------------------------------------------------------------------------
SELECT
  date,
  region_id,
  ROUND(AVG(rrp), 2) AS daily_avg_price,
  ROUND(MAX(rrp), 2) AS daily_peak_price,
  COUNT(*) AS intervals
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 30 DAYS
GROUP BY date, region_id
ORDER BY date, region_id;


-- -----------------------------------------------------------------------------
-- Query 3: Top 5 Demand Peaks (VIC) with Prices
-- Use: Demo Section 5.3 - Genie question
-- -----------------------------------------------------------------------------
SELECT
  interval_start,
  region_id,
  ROUND(demand_mw, 0) AS demand_mw,
  ROUND(rrp, 2) AS price_aud_mwh,
  is_peak
FROM workspace.nemweb_lab.curated_nem_prices
WHERE region_id = 'VIC1'
  AND date >= current_date() - INTERVAL 7 DAYS
ORDER BY demand_mw DESC
LIMIT 5;


-- -----------------------------------------------------------------------------
-- Query 4: Peak vs Off-Peak Price Comparison
-- Use: Demo Section 5.4 - Assistant-generated query example
-- -----------------------------------------------------------------------------
SELECT
  region_id,
  is_peak,
  ROUND(AVG(rrp), 2) AS avg_price,
  ROUND(STDDEV(rrp), 2) AS price_volatility,
  ROUND(AVG(demand_mw), 0) AS avg_demand_mw,
  COUNT(*) AS interval_count
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY region_id, is_peak
ORDER BY region_id, is_peak DESC;


-- -----------------------------------------------------------------------------
-- Query 5: NSW vs QLD Daily Comparison
-- Use: Demo Section 5.4 - Assistant refinement example
-- -----------------------------------------------------------------------------
SELECT
  date,
  region_id,
  ROUND(AVG(rrp), 2) AS avg_price,
  ROUND(STDDEV(rrp), 2) AS price_volatility,
  ROUND(MAX(demand_mw), 0) AS peak_demand_mw
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
  AND region_id IN ('NSW1', 'QLD1')
GROUP BY date, region_id
ORDER BY date, region_id;


-- -----------------------------------------------------------------------------
-- Query 6: Hourly Price Pattern (for dashboard)
-- Use: Demo Section 5.5 - Understanding intraday patterns
-- -----------------------------------------------------------------------------
SELECT
  HOUR(interval_start) AS hour_of_day,
  region_id,
  ROUND(AVG(rrp), 2) AS avg_price,
  ROUND(AVG(demand_mw), 0) AS avg_demand_mw
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 7 DAYS
GROUP BY HOUR(interval_start), region_id
ORDER BY hour_of_day, region_id;


-- -----------------------------------------------------------------------------
-- Query 7: Price Spike Analysis (>$300/MWh)
-- Use: Demo Section 5.3 - Genie ad-hoc question
-- -----------------------------------------------------------------------------
SELECT
  date,
  region_id,
  COUNT(*) AS spike_intervals,
  ROUND(AVG(rrp), 2) AS avg_spike_price,
  ROUND(MAX(rrp), 2) AS max_spike_price,
  ROUND(AVG(demand_mw), 0) AS avg_demand_during_spikes
FROM workspace.nemweb_lab.curated_nem_prices
WHERE rrp > 300
  AND date >= current_date() - INTERVAL 30 DAYS
GROUP BY date, region_id
ORDER BY max_spike_price DESC;


-- -----------------------------------------------------------------------------
-- Query 8: Regional Volatility Ranking
-- Use: Demo Section 5.3 - Audience question: "Which region had highest volatility?"
-- -----------------------------------------------------------------------------
SELECT
  region_id,
  ROUND(STDDEV(rrp), 2) AS price_volatility,
  ROUND(AVG(rrp), 2) AS avg_price,
  ROUND(MAX(rrp) - MIN(rrp), 2) AS price_range,
  ROUND(MAX(rrp), 2) AS max_price,
  ROUND(MIN(rrp), 2) AS min_price
FROM workspace.nemweb_lab.curated_nem_prices
WHERE date >= current_date() - INTERVAL 30 DAYS
GROUP BY region_id
ORDER BY price_volatility DESC;
