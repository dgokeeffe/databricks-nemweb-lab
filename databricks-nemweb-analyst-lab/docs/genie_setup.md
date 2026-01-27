# Genie Space Setup Guide

This guide covers configuring the AI/BI Genie space for the AGL Trading Analytics demo.

## Space Configuration

### Basic Settings

| Setting | Value |
|---------|-------|
| **Space Name** | AGL Trading Analytics - NEM |
| **Description** | Natural language analytics for NEM wholesale electricity prices and demand across Australian regions |

### Datasets

Add the following tables/views to the Genie space:

1. **workspace.nemweb_lab.curated_nem_prices**
   - Primary source table for NEM price and demand data

2. **workspace.nemweb_lab.mv_nem_price_metrics** (if metric views are supported in your workspace)
   - Semantic layer with pre-defined measures

## Table Instructions (Context for Genie)

Add these instructions to help Genie understand the data context:

### For curated_nem_prices

```
This table contains NEM (National Electricity Market) wholesale electricity data for Australia.

Key concepts:
- RRP (Regional Reference Price) is the spot price in $/MWh
- Regions are NSW1 (New South Wales), QLD1 (Queensland), VIC1 (Victoria), SA1 (South Australia), TAS1 (Tasmania)
- Data is at 5-minute dispatch interval granularity
- Peak hours are 17:00-21:00 local time (is_peak = true)
- Typical prices range $30-80/MWh, with occasional spikes above $300/MWh

Common analyses:
- Average price by region over time periods
- Price volatility (standard deviation)
- Peak vs off-peak price comparison
- Demand patterns and peaks
- Price spike identification (>$300/MWh)
```

### For mv_nem_price_metrics

```
This metric view provides standardised measures for NEM analytics.

Measures:
- avg_price: Average spot price ($/MWh)
- price_volatility: Standard deviation of price
- peak_price: Maximum price observed
- avg_demand_mw: Average demand (MW)
- peak_demand_mw: Maximum demand (MW)

Dimensions:
- region_id: NEM region
- date: Calendar date
- hour_of_day: For intraday analysis
- is_peak: Peak period indicator

Use this view for consistent KPI calculations across reports.
```

## Seeded Example Questions

Configure these example questions to guide users:

### Price Analysis
1. "What was the average spot price in NSW over the last 7 days?"
2. "How does that compare to QLD over the same period?"
3. "Which region had the highest price volatility last week?"
4. "Show a line chart of daily average price by region for the last 30 days"

### Demand Analysis
5. "When were the top 5 demand peaks in VIC this week, and what were the prices at those times?"
6. "What is the average demand during peak vs off-peak hours by region?"
7. "Which day had the highest total demand across all regions?"

### Price Spikes
8. "Show me all intervals where the price exceeded $300/MWh in the last month"
9. "What was happening to demand when we had price spikes in SA?"

### Comparisons
10. "Compare NSW and QLD average prices for each day last week"
11. "Which region has the lowest average price this month?"

## Access Configuration

### Authors (Can edit space)
- Workshop facilitator (you)
- Nominated AGL data lead

### Users (Can query)
- Trading Analytics team members
- Risk team personas

## Setup Steps

1. **Navigate to Genie**: In the Databricks workspace, go to the Genie section

2. **Create New Space**: Click "Create Space" or similar

3. **Configure Basic Info**:
   - Enter space name and description
   - Save initial configuration

4. **Add Datasets**:
   - Click "Add Dataset" or "Add Table"
   - Select `workspace.nemweb_lab.curated_nem_prices`
   - Repeat for `workspace.nemweb_lab.mv_nem_price_metrics`

5. **Add Table Instructions**:
   - For each dataset, add the context instructions above
   - This helps Genie understand domain-specific terminology

6. **Add Example Questions**:
   - Navigate to "Examples" or "Sample Questions"
   - Add the seeded questions above
   - For each question, optionally add the expected SQL/answer

7. **Configure Access**:
   - Add Authors and Users as described above
   - Verify permissions work correctly

8. **Test the Space**:
   - Ask each seeded question
   - Verify Genie generates correct SQL
   - Check visualisations render properly

## Verification Checklist

- [ ] Space is accessible to demo attendees
- [ ] Both datasets are visible and queryable
- [ ] Table instructions appear in Genie context
- [ ] Example questions return expected results
- [ ] Visualisations (line charts, bar charts) work correctly
- [ ] Generated SQL references correct table names

## Troubleshooting

### Genie returns incorrect results
- Check table instructions are specific enough
- Add more example questions with expected SQL
- Verify column comments in Unity Catalog

### Genie can't access tables
- Verify Unity Catalog permissions
- Check the Genie space has correct dataset assignments

### Slow responses
- Ensure SQL warehouse is running and appropriately sized
- Check table statistics are up to date (`ANALYZE TABLE`)

## Demo Tips

1. **Start with simple questions**: Build confidence with straightforward queries
2. **Show the SQL**: Click to reveal generated SQL - this builds trust
3. **Refine if needed**: If Genie misunderstands, rephrase and show how it adapts
4. **Invite audience questions**: Real engagement demonstrates capability
