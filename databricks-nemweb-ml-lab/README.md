# ML and Data Pipelines Workshop

A 60-minute demo-focused workshop for technical teams (Load Forecasting, Market Modelling) using real AEMO NEMWEB energy market data.

## Target audience

Technical teams who:
- Build and maintain forecasting systems (load, generation, battery optimization)
- Run Python scripts and manage custom data pipelines
- Pull data from multiple servers (AEMO, BOM, internal databases)
- Are evaluating whether Databricks could simplify or replace their infrastructure

## Workshop overview

| Section | Duration | Content |
|---------|----------|--------|
| Pain points + platform intro | 5 min | Discussion, Lakehouse architecture |
| Your data, one platform | 15 min | Real NEMWEB data in Lakeflow pipeline |
| ML on real energy data | 25 min | Feature engineering, MLflow tracking, model comparison |
| Production-ready | 10 min | Model serving, monitoring, scheduled retraining |
| Q&A / next steps | 5 min | Map pain points to what was shown |

## Quick start

**Deploy the ML lab bundle from this directory** (not the repo root). Use your CLI profile (e.g. `--profile daveok`).

### 1. Deploy the bundle (builds wheel, deploys pipelines + jobs)

```bash
cd databricks-nemweb-ml-lab
databricks bundle deploy --var="environment=dev" --profile daveok
```

### 2. Start the streaming pipeline (run before workshops to populate tables)

```bash
databricks bundle run streaming_pipeline_job --target dev --var="environment=dev" --profile daveok
```

### 3. Run the appropriate workshop

```bash
databricks bundle run load_forecast_workshop --target dev --var="environment=dev" --profile daveok
# OR
databricks bundle run market_modelling_workshop --target dev --var="environment=dev" --profile daveok
```

Alternatively, run data setup (downloads 30 days of NEMWEB + runs ML pipeline) then the generic ML workshop:

```bash
databricks bundle run ml_workshop_data_setup --target dev --var="environment=dev" --profile daveok
databricks bundle run ml_workshop --target dev --var="environment=dev" --profile daveok
```

Or open `workshops/01_ml_workshop.py` in the workspace and run cells interactively.

### 4. Build prediction tables for the app

```bash
databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --profile daveok
```

### 5. Start the Dash app (bundle-managed)

```bash
databricks bundle run nem_forecast --target dev --var="environment=dev" --profile daveok
databricks apps logs nem-forecast-dev --profile daveok
```

The app resource is deployed with the bundle and started with `bundle run`.

## File structure

```
databricks-nemweb-ml-lab/
  README.md                          # This file
  resources/                         # Jobs, pipelines, and app bundle resources
  src/                               # Streaming + ML + prediction pipeline code
  app/                               # Dash app (bundle-managed via resources/nem_forecast.app.yml)
  workshops/
    00_data_setup.py                 # Pre-workshop: download NEMWEB data, create schema
    01_ml_workshop.py                # THE workshop notebook (60 min)
  artifacts/                         # Wheel copied here by bundle build
```

## Data flow

```
NEMWEB CURRENT + BOM APIs
     |
     |  streaming_pipeline_job
     v
Streaming Bronze/Silver/Gold tables
  bronze_dispatch_stream, bronze_price_stream, bronze_bom_weather, ...
  silver_demand_weather, silver_forecast_vs_actual, silver_supply_stack
  gold_demand_hourly, gold_price_hourly, gold_forecast_accuracy, gold_interconnector_hourly
     |
     |  load_forecast_workshop + market_modelling_workshop
     v
UC Models + champion aliases
  {catalog}.{schema}.load_forecast_nsw@champion
  {catalog}.{schema}.price_forecast_nsw@champion
     |
     |  prediction_pipeline_job
     v
Prediction Tables
  demand_predictions
  price_predictions
     |
     |  nem_forecast app resource
     v
Dash App: nem-forecast-${environment}
```

## Bundle commands

Run from **databricks-nemweb-ml-lab/** and pass your profile (e.g. `--profile daveok`).

```bash
cd databricks-nemweb-ml-lab

# Validate configuration
databricks bundle validate --var="environment=dev" --profile daveok

# Deploy all resources (ML lab only)
databricks bundle deploy --var="environment=dev" --profile daveok

# Streaming pipeline (live NEMWEB + BOM weather)
databricks bundle run streaming_pipeline_job --target dev --var="environment=dev" --profile daveok

# Run data setup (before workshop)
databricks bundle run ml_workshop_data_setup --target dev --var="environment=dev" --profile daveok

# Run workshop notebook as a job
databricks bundle run ml_workshop --target dev --var="environment=dev" --profile daveok

# Load Forecast / Market Modelling workshops
databricks bundle run load_forecast_workshop --target dev --var="environment=dev" --profile daveok
databricks bundle run market_modelling_workshop --target dev --var="environment=dev" --profile daveok

# Build app prediction tables from champion models
databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --profile daveok

# Start the bundle-managed app
databricks bundle run nem_forecast --target dev --var="environment=dev" --profile daveok

# Run Auto Loader ML pipeline only (no re-download)
databricks bundle run nemweb_ml_pipeline_job --target dev --var="environment=dev" --profile daveok

# Clean up
databricks bundle destroy --target dev --var="environment=dev" --profile daveok
```

## Prerequisites

- Databricks workspace with Unity Catalog
- `databricks` CLI >= 0.283.0
- Serverless compute enabled (or DBR 15.4+ cluster)

All jobs and pipelines use **serverless environment version 4 (SVLS 4)** for consistency. Optionally, workspace admins can create a shared base environment: from the repo root run `uv run python config/create_base_environment.py` (uploads the spec via the Databricks SDK), then complete the one UI step at **Settings > Workspace admin > Compute > Base environments for serverless compute > Manage > Create new environment**. See [Manage serverless workspace base environments](https://learn.microsoft.com/en-gb/azure/databricks/admin/workspace-settings/base-environment).

## Facilitation tips

1. **Start with questions, not slides**
   - "What's the most annoying part of your current workflow?"
   - Write their answers down and reference them throughout

2. **Let them see their own data**
   - The NEMWEB data is real AEMO data they work with every day
   - Point out familiar table names, regions, and patterns

3. **Show the pipeline first, then the ML**
   - They care about data pipelines as much as ML
   - "This replaced 500 lines of Python scripts and 3 cron jobs"

4. **Be honest about limitations**
   - "This won't replace Plexos" - but it can orchestrate Plexos runs
   - Acknowledge the learning curve

5. **End with a concrete next step**
   - "What would you try first?"
   - Offer to help them run one of their own scripts in a notebook

## Customization

All jobs, pipelines, and the app use `${var.catalog}` and `${var.schema}`. To override defaults:

```bash
databricks bundle deploy --var="environment=dev" --var="catalog=your_catalog" --var="schema=your_schema" --var="warehouse_id=your_warehouse_id"
databricks bundle run load_forecast_workshop --target dev --var="environment=dev" --var="catalog=your_catalog" --var="schema=your_schema"
databricks bundle run market_modelling_workshop --target dev --var="environment=dev" --var="catalog=your_catalog" --var="schema=your_schema"
databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --var="catalog=your_catalog" --var="schema=your_schema"
databricks bundle run nem_forecast --target dev --var="environment=dev" --var="catalog=your_catalog" --var="schema=your_schema"
```

The workshop notebook has `CATALOG` and `SCHEMA` variables at the top that can be adjusted for interactive runs.
