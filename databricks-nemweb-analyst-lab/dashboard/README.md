# NEM Price Analytics Dashboard

This directory contains SQL queries for building a Databricks SQL Dashboard.

## Quick Start

1. **Open SQL Editor** in Databricks
2. **Create a new Dashboard**: Dashboards > Create Dashboard
3. **Add widgets** using the queries from `dashboard_queries.sql`

## Recommended Layout

```
┌─────────────────┬─────────────────┬─────────────────┐
│   Avg Price     │   Peak Demand   │  Spike Count    │
│   (Counter)     │   (Counter)     │   (Counter)     │
├─────────────────┴─────────────────┴─────────────────┤
│           Daily Average Price by Region             │
│                   (Line Chart)                      │
├─────────────────────────┬───────────────────────────┤
│   Regional Summary      │   Price Volatility        │
│      (Table)            │     (Bar Chart)           │
├─────────────────────────┴───────────────────────────┤
│           Peak vs Off-Peak Comparison               │
│              (Grouped Bar Chart)                    │
└─────────────────────────────────────────────────────┘
```

## Widget Guide

| Widget | Query | Visualization | Notes |
|--------|-------|---------------|-------|
| Avg Price KPI | Widget 1 | Counter | Show with $ prefix |
| Peak Demand KPI | Widget 2 | Counter | Show with MW suffix |
| Spike Count KPI | Widget 3 | Counter | Warning color if > 0 |
| Price Trend | Widget 4 | Line Chart | Color by region |
| Regional Summary | Widget 5 | Table | Sort by price desc |
| Volatility | Widget 6 | Bar Chart | Horizontal preferred |
| Peak vs Off-Peak | Widget 7 | Grouped Bar | 2 colors |
| Hourly Pattern | Widget 8 | Heatmap | Optional |
| Spike Events | Widget 9 | Table | Conditional formatting |
| Current Demand | Widget 10 | Bar Chart | Latest values |

## Creating the Dashboard

### Step 1: Create Dashboard

1. Go to **Dashboards** in the left sidebar
2. Click **Create Dashboard**
3. Name: "NEM Price Analytics"

### Step 2: Add KPI Counters

1. Click **Add** > **Visualization**
2. Paste query from Widget 1
3. Select visualization: **Counter**
4. Configure:
   - Value column: `avg_price`
   - Prefix: `$`
   - Suffix: `/MWh`

Repeat for Widgets 2 and 3.

### Step 3: Add Price Trend Chart

1. Click **Add** > **Visualization**
2. Paste query from Widget 4
3. Select visualization: **Line**
4. Configure:
   - X-axis: `date`
   - Y-axis: `avg_price`
   - Color: `region_id`

### Step 4: Add Tables and Bar Charts

Continue adding widgets using the SQL queries provided.

## Refresh Schedule

For production dashboards:

1. Click **Schedule** in the dashboard toolbar
2. Set refresh interval (e.g., every hour)
3. Configure alerts if needed (e.g., when spike_count > 0)

## Parameters (Optional)

Add a date range parameter:

```sql
SELECT ...
WHERE date >= current_date() - INTERVAL {{ days }} DAYS
```

Then create a dashboard parameter:
- Name: `days`
- Type: Dropdown
- Values: 7, 14, 30, 90

## Sharing

1. Click **Share** in dashboard toolbar
2. Add users/groups with View or Edit access
3. Optionally enable "Anyone with link can view"
