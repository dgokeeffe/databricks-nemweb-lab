# Databricks Trading Analytics Demo

**60-minute demo-only workshop** introducing Databricks AI/BI capabilities to trading analytics teams.

## Overview

This workshop demonstrates how Databricks can serve as the governed, AI-augmented data layer for NEM (National Electricity Market) analytics while keeping Excel and Power BI in the picture.

**Target Audience**: Trading Analytics team - strong domain expertise in NEM/trading, heavy Excel/Power BI users, minimal Databricks experience.

**Format**: Live demo (presenter drives), interactive via Q&A and Genie questions.

## Pre-Workshop Setup

### Prerequisites

**Run the data engineering workshop setup first** to load real AEMO data:

```
databricks-nemweb-lab/exercises/00_setup_and_validation.py
```

This creates:
- `workspace.nemweb_lab.nemweb_dispatch_prices` - RRP (Regional Reference Price) data
- `workspace.nemweb_lab.nemweb_dispatch_regionsum` - Demand and generation data

### 1. Create Curated View

Run the DDL to create the analyst-friendly view that joins price + demand:

```sql
-- In Databricks SQL Editor, run:
-- sql/01_ddl_curated_nem_prices.sql
```

This creates `workspace.nemweb_lab.curated_nem_prices` which joins the real AEMO data.

### 2. Create Metric View

Run `sql/03_metric_view.sql` (requires Serverless or DBR 17.2+)

This creates `workspace.nemweb_lab.mv_nem_price_metrics` with standardised measures.

### 3. Configure Genie Space

Follow `docs/genie_setup.md` to create the "NEM Trading Analytics" space.

### 4. (Optional) Set Up Power BI

Follow `docs/powerbi_connection.md` to connect Power BI Desktop.

## Directory Structure

```
databricks-nemweb-analyst-lab/
├── README.md                              # This file
├── prd.md                                 # Product Requirements Document
├── sql/
│   ├── 01_ddl_curated_nem_prices.sql     # Curated view DDL (joins real AEMO data)
│   ├── 02_sample_queries.sql             # Demo queries for SQL Editor
│   └── 03_metric_view.sql                # Metric view definition
├── data/
│   ├── README.md                          # Data loading instructions
│   └── sample_curated_nem_prices.csv     # Fallback synthetic data (if needed)
├── notebooks/
│   └── 95th_percentile_analysis.py       # Optional PySpark analysis
└── docs/
    ├── demo_outline.md                   # Presenter guide (0-60 min)
    ├── genie_setup.md                    # Genie space configuration
    └── powerbi_connection.md             # Power BI connection steps
```

## Data Sources

This workshop uses **real AEMO data** loaded by the data engineering workshop:

| Table | Source | Description |
|-------|--------|-------------|
| `nemweb_dispatch_prices` | DISPATCHPRICE | Regional Reference Price ($/MWh) |
| `nemweb_dispatch_regionsum` | DISPATCHREGIONSUM | Demand, generation, interconnector flows |
| `curated_nem_prices` | View | Analyst-friendly join of price + demand |
| `mv_nem_price_metrics` | Metric View | Semantic layer with standardised measures |

Data is 5-minute dispatch intervals for all 5 NEM regions (NSW1, QLD1, VIC1, SA1, TAS1).

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

- **01_ddl_curated_nem_prices.sql**: Creates view joining DISPATCHPRICE + DISPATCHREGIONSUM
- **02_sample_queries.sql**: 8 ready-to-run queries for demo sections
- **03_metric_view.sql**: Metric view with standardised measures (avg_price, volatility, demand)

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
- SQL Warehouse (Serverless) or DBR 17.2+ for metric views
- Real AEMO data loaded via `00_setup_and_validation.py`
- (Optional) Power BI Desktop

## Resources

- [PRD](./prd.md) - Full Product Requirements Document
- [AEMO NEMWeb](https://www.nemweb.com.au/) - Source of NEM data
- [Databricks Genie Docs](https://docs.databricks.com/genie/)
- [Metric Views Documentation](https://docs.databricks.com/metric-views/)
