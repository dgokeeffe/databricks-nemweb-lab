# Databricks Trading Analytics Demo for AGL

**60-minute demo-only workshop** introducing Databricks to AGL Trading Analytics team.

## Overview

This workshop demonstrates how Databricks can serve as the governed, AI-augmented data layer for NEM (National Electricity Market) analytics while keeping Excel and Power BI in the picture.

**Target Audience**: AGL Trading Analytics team - strong domain expertise in NEM/trading, heavy Excel/Power BI users, minimal Databricks experience.

**Format**: Live demo (presenter drives), interactive via Q&A and Genie questions.

## Pre-Workshop Setup

### 1. Create Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS agldata;
CREATE SCHEMA IF NOT EXISTS agldata.trading;
```

### 2. Create Curated NEM Table

Run the DDL in `sql/01_ddl_curated_nem_prices.sql`

### 3. Load Sample Data

1. Upload `data/sample_curated_nem_prices.csv` to a Unity Catalog Volume or DBFS
2. Follow instructions in `data/README.md` to load the data

### 4. Create Metric View

Run `sql/03_metric_view.sql` (requires DBR 17.2+)

### 5. Configure Genie Space

Follow `docs/genie_setup.md` to create the "AGL Trading Analytics - NEM" space

### 6. (Optional) Set Up Power BI

Follow `docs/powerbi_connection.md` to connect Power BI Desktop

## Directory Structure

```
databricks-nemweb-anaylst-lab/
├── README.md                              # This file
├── prd.md                                 # Product Requirements Document
├── sql/
│   ├── 01_ddl_curated_nem_prices.sql     # Table DDL with column comments
│   ├── 02_sample_queries.sql             # Demo queries for SQL Editor
│   └── 03_metric_view.sql                # Metric view definition
├── data/
│   ├── README.md                          # Data loading instructions
│   └── sample_curated_nem_prices.csv     # 7 days synthetic NEM data
├── notebooks/
│   └── 95th_percentile_analysis.py       # Optional PySpark analysis
└── docs/
    ├── demo_outline.md                   # Presenter guide (0-60 min)
    ├── genie_setup.md                    # Genie space configuration
    └── powerbi_connection.md             # Power BI connection steps
```

## Demo Agenda (60 min)

| Time | Section | Key Demo |
|------|---------|----------|
| 0-5 | Context | Databricks as governed AI layer |
| 5-12 | Curated Data | `curated_nem_prices` in Catalog Explorer |
| 12-25 | **Genie** | Natural language NEM queries |
| 25-35 | Assistant | SQL generation in SQL Editor |
| 35-45 | Metric Views | Semantic layer + dashboard |
| 45-55 | Workflow Mapping | Map Excel/Power BI to Databricks |
| 55-60 | Wrap-up | Next steps |

See `docs/demo_outline.md` for the full presenter script.

## Key Artefacts

### SQL Files

- **01_ddl_curated_nem_prices.sql**: Creates the `curated_nem_prices` table with column comments for Genie context
- **02_sample_queries.sql**: Ready-to-run queries for demo sections
- **03_metric_view.sql**: Metric view with standardised measures (avg_price, volatility, demand)

### Sample Data

- 7 days of synthetic NEM data (~10,000 rows)
- 5 regions: NSW1, QLD1, VIC1, SA1, TAS1
- Realistic price patterns including occasional spikes

### Genie Configuration

Seeded example questions:
1. "What was the average spot price in NSW over the last 7 days?"
2. "How does that compare to QLD over the same period?"
3. "Show a line chart of daily average price by region"
4. "When were the top 5 demand peaks in VIC?"

## Success Criteria

During the session:
- [ ] At least 2 audience questions answered live in Genie
- [ ] Participants identify 3+ Excel/Power BI use cases that could use Databricks

After the session:
- [ ] Agreement to identify first curated tables/metric views
- [ ] Interest in hands-on follow-up workshop

## Requirements

- Databricks workspace with Unity Catalog enabled
- AI/BI Genie and Assistant enabled
- SQL Warehouse (Serverless Environment Version 4+) or DBR 17.2+ for metric views
- (Optional) Power BI Desktop

## Resources

- [PRD](./prd.md) - Full Product Requirements Document
- [AEMO NEMWeb](https://www.nemweb.com.au/) - Source of NEM data
- [Databricks Genie Docs](https://docs.databricks.com/genie/)
- [Metric Views Documentation](https://docs.databricks.com/metric-views/)
