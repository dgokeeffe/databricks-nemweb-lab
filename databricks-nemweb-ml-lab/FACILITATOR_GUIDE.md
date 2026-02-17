# NEMWEB ML Workshop Facilitator Guide

This guide supports a 60-minute delivery with two facilitator tracks:

- Track A: Load Forecasting (`workshops/02_load_forecast_workshop.py`)
- Track B: Market Modelling (`workshops/03_market_modelling_workshop.py`)

Use this for live customer enablement where the objective is to show reliable
Databricks Jobs execution, governed ML in Unity Catalog, and app consumption of
prediction outputs.

## Audience and Outcomes

Audience:

- Energy analytics, forecasting, and market operations teams
- Data scientists and ML engineers on Databricks
- Solution architects validating production patterns

Outcomes by the end of session:

- Understand the medallion flow from streaming data to ML features
- Train, register, and alias champion models in Unity Catalog
- Run notebooks as Jobs with parameterized widgets
- Refresh pipeline-managed prediction tables used by the Databricks App

## 60-Minute Agenda

- 0:00-0:05 - Introductions, architecture framing, success criteria
- 0:05-0:12 - Data and pipeline context (`silver_demand_weather` and feature flow)
- 0:12-0:30 - Track A: Load Forecasting workshop walk-through
- 0:30-0:48 - Track B: Market Modelling workshop walk-through
- 0:48-0:55 - Prediction pipeline refresh and app validation
- 0:55-1:00 - Q&A, recap, and next steps

## Prerequisites

Before the session:

- Bundle validates and deploys from `databricks-nemweb-ml-lab/`
- Streaming pipeline is active or recently refreshed
- Catalog/schema are available and writable for workshop user
- SQL warehouse is available for app queries
- Facilitator has permissions for Jobs, Pipelines, UC models, and Apps

Runtime assumptions:

- Workshops write isolated outputs:
  - `demand_predictions_workshop`
  - `price_predictions_workshop`
- Databricks App reads pipeline-managed tables:
  - `demand_predictions`
  - `price_predictions`

## Track A Facilitation Notes (Load Forecasting)

Focus points:

- Feature engineering from `silver_demand_weather`
- Training/evaluation metrics (MAE/RMSE/MAPE)
- MLflow experiment tracking and model registration
- Champion alias management and job-safe experiment path handling

Demo moments:

- Show widget parameters (`catalog`, `schema`, `history_days`, `experiment_path`)
- Show model version and alias in Unity Catalog
- Show generated `demand_predictions_workshop` sample rows

## Track B Facilitation Notes (Market Modelling)

Focus points:

- Price-specific feature engineering and volatility context
- UC function usage (`price_spike_band`, `market_stress_score`)
- Metric view behavior and compatibility notes
- Model registration and champion alias lifecycle

Demo moments:

- Show metric UDF outputs on recent rows
- Show model metrics and forecast error interpretation
- Show generated `price_predictions_workshop` sample rows

## Common Talk Track (Productionization)

Use this narrative at transition points:

- Workshops train and validate models in an interactive format
- Jobs provide reproducible, scheduled execution
- Prediction pipeline materializes app-facing prediction tables
- App visualizes current forecast quality and model behavior

## Delivery Tips

- Keep both tracks on NSW1 examples for consistency
- Call out explicit run order: streaming -> workshops -> prediction pipeline -> app
- If time is tight, complete Track A deeply and summarize Track B
- Keep a fallback screenshot or prior run URL ready for each key checkpoint

## Post-Session Next Steps

- Promote champion model selection policy by business KPI thresholds
- Add scheduled quality monitoring on prediction errors
- Extend to additional NEM regions after NSW1 validation
- Add alerting for data freshness and model drift
