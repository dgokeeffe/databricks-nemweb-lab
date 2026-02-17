# Databricks notebook source
# MAGIC %md
# MAGIC # ML and Data Pipelines Workshop
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused (instructor drives, participants observe and ask questions)
# MAGIC
# MAGIC **Audience:** Technical teams who build and maintain forecasting systems, run Python scripts, and manage custom data pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Section | Duration | Format |
# MAGIC |---------|----------|--------|
# MAGIC | **Pain points + platform intro** | 5 min | Discussion |
# MAGIC | **Your data, one platform** | 15 min | Demo - real NEMWEB data in a Lakeflow pipeline |
# MAGIC | **ML on real energy data** | 25 min | Demo - feature engineering, MLflow, model comparison |
# MAGIC | **Production-ready** | 10 min | Demo - serving, monitoring, scheduled retraining |
# MAGIC | **Q&A / next steps** | 5 min | Discussion |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Data:** This workshop uses **real AEMO NEMWEB data** - 5-minute dispatch intervals
# MAGIC with regional demand, generation, and spot prices from the National Electricity Market.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Pain points (5 min)
# MAGIC
# MAGIC > **"What's the most annoying part of your current workflow?"**
# MAGIC
# MAGIC Common answers we hear from teams like yours:
# MAGIC
# MAGIC | Pain point | Sound familiar? |
# MAGIC |------------|----------------|
# MAGIC | "We pull data from multiple servers via scripts" | Fragile, no retry, manual scheduling |
# MAGIC | "We don't know which model version is in production" | `model_final_FINAL_v2.pkl` on a shared drive |
# MAGIC | "When something fails overnight, we find out from users" | No alerting, no observability |
# MAGIC | "Comparing experiments means digging through old notebooks" | No systematic tracking |
# MAGIC | "Data is scattered across multiple servers and formats" | CSV, Parquet, Excel, databases |
# MAGIC
# MAGIC ```
# MAGIC Your current state:
# MAGIC
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │ AEMO Server │     │ BOM Weather │     │ Internal DB │
# MAGIC └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
# MAGIC        │                   │                   │
# MAGIC        ▼                   ▼                   ▼
# MAGIC    ┌───────────────────────────────────────────────┐
# MAGIC    │           Python Scripts (cron jobs)          │
# MAGIC    │  - No retry logic    - Manual scheduling      │
# MAGIC    │  - Silent failures   - No lineage tracking    │
# MAGIC    └───────────────────────────────────────────────┘
# MAGIC        │                   │                   │
# MAGIC        ▼                   ▼                   ▼
# MAGIC    ┌─────────┐       ┌─────────┐       ┌─────────┐
# MAGIC    │ CSV/DB  │       │ Parquet │       │ Excel   │
# MAGIC    └─────────┘       └─────────┘       └─────────┘
# MAGIC ```
# MAGIC
# MAGIC **Write down 1-2 pain points - we'll reference them throughout:**
# MAGIC
# MAGIC ```
# MAGIC Pain point 1: _______________________________________________
# MAGIC
# MAGIC Pain point 2: _______________________________________________
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Databricks Lakehouse - one platform for everything
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    Unity Catalog (Governance)                   │
# MAGIC │         Access Control │ Lineage │ Data Discovery │ Audit      │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                 │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
# MAGIC │  │   Lakeflow   │  │    MLflow    │  │   Mosaic AI  │          │
# MAGIC │  │  Pipelines   │  │   ML Ops     │  │    GenAI     │          │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────┘          │
# MAGIC │                                                                 │
# MAGIC │  ┌─────────────────────────────────────────────────────────────┐│
# MAGIC │  │                    Delta Lake (Storage)                     ││
# MAGIC │  │         ACID │ Time Travel │ Schema Evolution │ Z-Order    ││
# MAGIC │  └─────────────────────────────────────────────────────────────┘│
# MAGIC │                                                                 │
# MAGIC │  ┌─────────────────────────────────────────────────────────────┐│
# MAGIC │  │              Serverless Compute (No cluster mgmt)           ││
# MAGIC │  └─────────────────────────────────────────────────────────────┘│
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | Concept | What it means for you |
# MAGIC |---------|----------------------|
# MAGIC | **Unity Catalog** | One place for tables, models, files - with access control and lineage |
# MAGIC | **Lakeflow Pipelines** | Replace fragile Python scripts with declarative, monitored pipelines |
# MAGIC | **MLflow** | Track every experiment, compare models, deploy to production |
# MAGIC | **Serverless** | No cluster management - compute scales automatically |
# MAGIC | **Delta Lake** | Reliable storage with ACID transactions and time travel |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Your data, one platform (15 min)
# MAGIC
# MAGIC Let's look at **real AEMO NEMWEB data** that's already flowing through a Lakeflow pipeline.
# MAGIC
# MAGIC This is the same data your teams work with every day - 5-minute dispatch intervals
# MAGIC from the National Electricity Market.

# COMMAND ----------

# Configuration - uses widget parameters from bundle job, with defaults for interactive use
dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "ml_workshops", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Reading from: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Real energy data in Delta tables
# MAGIC
# MAGIC This data was ingested from NEMWEB via a streaming pipeline - no manual downloads.

# COMMAND ----------

# 5-minute regional demand and generation (streaming from NEMWEB)
df_demand = spark.table(f"{CATALOG}.{SCHEMA}.bronze_dispatch_stream")

row_count = df_demand.count()
# Column names are uppercase from NEMWEB source (REGIONID, SETTLEMENTDATE, etc.)
region_col = "REGIONID" if "REGIONID" in df_demand.columns else "region_id"
regions = [r[0] for r in df_demand.select(region_col).distinct().collect()]

print(f"Regional dispatch data: {row_count:,} rows")
print(f"NEM regions: {sorted(regions)}")

ts_col = "SETTLEMENTDATE" if "SETTLEMENTDATE" in df_demand.columns else "settlement_date"
display(df_demand.orderBy(ts_col, ascending=False).limit(20))

# COMMAND ----------

# 5-minute spot prices (streaming from NEMWEB)
df_price = spark.table(f"{CATALOG}.{SCHEMA}.bronze_price_stream")

ts_col_p = "SETTLEMENTDATE" if "SETTLEMENTDATE" in df_price.columns else "settlement_date"
print(f"Price data: {df_price.count():,} rows")
display(df_price.orderBy(ts_col_p, ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 The pipeline that brought this data in
# MAGIC
# MAGIC Instead of Python scripts pulling from multiple servers, we have a **declarative pipeline**:
# MAGIC
# MAGIC ```
# MAGIC NEMWEB CURRENT API (HTTP/ZIP/CSV)           BOM Weather API (JSON)
# MAGIC         │                                             │
# MAGIC         │  nemweb_stream (custom datasource)          │  bom_weather.py
# MAGIC         ▼                                             ▼
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │  Bronze                                                          │
# MAGIC │  ├── bronze_dispatch_stream     (5-min demand, generation)       │
# MAGIC │  ├── bronze_price_stream        (5-min spot prices)              │
# MAGIC │  ├── bronze_scada_stream        (unit-level MW output)           │
# MAGIC │  └── bronze_bom_weather         (temperature, humidity, wind)    │
# MAGIC │                                                                  │
# MAGIC │  Silver                                                          │
# MAGIC │  ├── silver_demand_weather      (demand + price + weather)       │
# MAGIC │  └── silver_supply_stack        (NEM-wide generation)            │
# MAGIC │                                                                  │
# MAGIC │  Gold                                                            │
# MAGIC │  ├── gold_demand_hourly         (hourly demand + weather)        │
# MAGIC │  └── gold_price_hourly          (hourly price stats)             │
# MAGIC └──────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **What you get for free:**
# MAGIC - Automatic retry and exactly-once processing
# MAGIC - Data quality expectations (bad rows get quarantined, not dropped silently)
# MAGIC - Full lineage: click any table to see where the data came from
# MAGIC - Schema evolution: new columns in source files are handled automatically
# MAGIC
# MAGIC **[Switch to Lakeflow UI to show the pipeline DAG, run history, and data quality]**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Silver layer - ML-ready joined data
# MAGIC
# MAGIC The pipeline joins demand and price data so it's ready for ML out of the box.

# COMMAND ----------

# This is what your ML model will train on (dispatch + price + weather joined)
df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")

print(f"ML-ready data: {df_silver.count():,} rows")
print(f"Columns: {df_silver.columns}")

display(
    df_silver
    .filter("region_id = 'NSW1'")
    .orderBy("settlement_date", ascending=False)
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Gold layer - quick exploration
# MAGIC
# MAGIC Pre-aggregated views for quick analysis without scanning raw data.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.gold_demand_hourly")
    .filter("region_id = 'NSW1'")
    .orderBy("hour", ascending=False)
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 What this replaces
# MAGIC
# MAGIC | Your current approach | Databricks equivalent |
# MAGIC |----------------------|----------------------|
# MAGIC | Python scripts pulling from AEMO, BOM, internal DBs | Lakeflow pipeline with Auto Loader |
# MAGIC | Cron job scheduling with no retry | Built-in orchestration with retry and alerting |
# MAGIC | "Did the job run last night?" - check logs manually | Pipeline UI with run history and data quality metrics |
# MAGIC | Data scattered across CSV, Parquet, databases | All in Delta tables under Unity Catalog |
# MAGIC | No lineage or audit trail | Automatic lineage tracking |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: ML on real energy data (25 min)
# MAGIC
# MAGIC Now let's build a **load forecasting model** using the real NEMWEB data we just explored.
# MAGIC
# MAGIC This is representative of what your Load Forecasting team does -
# MAGIC predict electricity demand using historical patterns and features.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure MLflow
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/ml_workshop_load_forecasting"
mlflow.set_experiment(experiment_name)

print(f"MLflow experiment: {experiment_name}")
print(f"Model registry: Unity Catalog ({CATALOG}.{SCHEMA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Load real demand data
# MAGIC
# MAGIC We'll use NSW demand from the silver table (already joined with prices).

# COMMAND ----------

# Load the silver table - demand + price + weather already joined
df_nsw = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .drop("_processed_at")
    .orderBy("settlement_date")
)

# Convert to pandas for ML
pdf = df_nsw.toPandas()
pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
pdf = pdf.set_index("settlement_date").sort_index()

print(f"NSW demand data: {len(pdf):,} rows")
print(f"Date range: {pdf.index.min()} to {pdf.index.max()}")
print(f"Days of data: {(pdf.index.max() - pdf.index.min()).days}")
print(f"\nColumns: {list(pdf.columns)}")

display(pdf.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Feature engineering
# MAGIC
# MAGIC This is the kind of feature engineering your teams already do -
# MAGIC time-based features, lag features, and rolling statistics.
# MAGIC
# MAGIC In production you'd also add weather data (BOM temperature, solar irradiance)
# MAGIC and calendar features (public holidays, school terms).

# COMMAND ----------

def create_demand_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for load forecasting from NEMWEB dispatch data.

    These are representative of what a Load Forecasting team would build.
    In production, you'd add weather features, calendar features, etc.
    """
    out = df.copy()

    # -- Time features --
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["month"] = out.index.month
    out["is_weekend"] = out.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding (captures the circular nature of time)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7)
    out["month_sin"] = np.sin(2 * np.pi * out["month"] / 12)
    out["month_cos"] = np.cos(2 * np.pi * out["month"] / 12)

    # -- Lag features (previous demand values) --
    # 5-min intervals: lag_1 = 5min ago, lag_12 = 1hr ago, lag_288 = 1 day ago
    for lag in [1, 2, 3, 6, 12, 48, 288]:
        out[f"demand_lag_{lag}"] = out["total_demand_mw"].shift(lag)

    # -- Rolling statistics --
    out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()
    out["demand_rolling_1h_std"] = out["total_demand_mw"].rolling(12).std()
    out["demand_rolling_1d_mean"] = out["total_demand_mw"].rolling(288).mean()

    # -- Price features (price-responsive demand) --
    out["rrp_lag_1"] = out["rrp"].shift(1)
    out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()

    # -- AEMO's own forecast as a feature --
    # (AEMO publishes demand forecasts; the delta is interesting)
    if "demand_forecast_mw" in out.columns:
        out["aemo_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    return out.dropna()


pdf_features = create_demand_features(pdf)

feature_cols = [c for c in pdf_features.columns if c != "total_demand_mw"]
print(f"Features ({len(feature_cols)}):")
for c in feature_cols:
    print(f"  - {c}")

print(f"\nRows after feature engineering: {len(pdf_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Train/test split
# MAGIC
# MAGIC For time series, we use a **temporal split** (not random) - train on the past, test on the future.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

# Prepare X and y
X = pdf_features[feature_cols]
y = pdf_features["total_demand_mw"]

# Time-based split - last 20% of data for testing
split_idx = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Training set: {len(X_train):,} rows ({X_train.index.min()} to {X_train.index.max()})")
print(f"Test set:     {len(X_test):,} rows ({X_test.index.min()} to {X_test.index.max()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Train models with MLflow tracking
# MAGIC
# MAGIC Now the key part - **every experiment is automatically tracked**.
# MAGIC
# MAGIC No more screenshots, no more "what parameters did I use?", no more `model_final_FINAL_v2.pkl`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model 1: XGBoost

# COMMAND ----------

import xgboost as xgb

with mlflow.start_run(run_name="xgboost_baseline"):
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
    }

    mlflow.log_params(params)
    mlflow.log_param("model_type", "xgboost")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = xgb.XGBRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100,
    }
    mlflow.log_metrics(metrics)

    # Log model
    mlflow.sklearn.log_model(model, "model")

    # Log feature importance
    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    mlflow.log_table(importance_df, "feature_importance.json")

    print(f"XGBoost results:")
    print(f"  MAE:  {metrics['mae']:.1f} MW")
    print(f"  RMSE: {metrics['rmse']:.1f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model 2: LightGBM

# COMMAND ----------

import lightgbm as lgb

with mlflow.start_run(run_name="lightgbm_baseline"):
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "verbose": -1,
    }

    mlflow.log_params(params)
    mlflow.log_param("model_type", "lightgbm")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = lgb.LGBMRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100,
    }
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    print(f"LightGBM results:")
    print(f"  MAE:  {metrics['mae']:.1f} MW")
    print(f"  RMSE: {metrics['rmse']:.1f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model 3: Gradient Boosting (scikit-learn)

# COMMAND ----------

from sklearn.ensemble import GradientBoostingRegressor

with mlflow.start_run(run_name="gradient_boosting_baseline"):
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
    }

    mlflow.log_params(params)
    mlflow.log_param("model_type", "sklearn_gradient_boosting")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = GradientBoostingRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100,
    }
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    print(f"Gradient Boosting results:")
    print(f"  MAE:  {metrics['mae']:.1f} MW")
    print(f"  RMSE: {metrics['rmse']:.1f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Compare all experiments
# MAGIC
# MAGIC Now the payoff - see all your runs in one place, compare them systematically.
# MAGIC
# MAGIC No more digging through old notebooks or spreadsheets.

# COMMAND ----------

runs_df = mlflow.search_runs(experiment_names=[experiment_name])

comparison = (
    runs_df[["run_id", "tags.mlflow.runName", "params.model_type",
             "metrics.mae", "metrics.rmse", "metrics.mape", "start_time"]]
    .copy()
)
comparison.columns = ["run_id", "run_name", "model_type", "mae_mw", "rmse_mw", "mape_pct", "timestamp"]
comparison = comparison.sort_values("mae_mw")

display(comparison)

print(f"\nBest model by MAE: {comparison.iloc[0]['run_name']} ({comparison.iloc[0]['mae_mw']:.1f} MW)")

# COMMAND ----------

# MAGIC %md
# MAGIC **[Click the Experiments icon in the left sidebar to show the MLflow UI]**
# MAGIC
# MAGIC In the UI you can:
# MAGIC - Compare runs visually (parameter vs metric charts)
# MAGIC - Inspect feature importance artifacts
# MAGIC - Download any model
# MAGIC - Share results with teammates
# MAGIC - See full reproducibility info (code version, environment, timestamps)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Register best model to Unity Catalog
# MAGIC
# MAGIC This solves the "which model is in production?" problem.
# MAGIC
# MAGIC Instead of `model_final_FINAL_v2.pkl` on a shared drive, you get a **versioned registry
# MAGIC with lineage** back to the training data.

# COMMAND ----------

# Get the best run
best_run = comparison.iloc[0]
best_run_id = best_run["run_id"]

# Register to Unity Catalog
model_name = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"
model_uri = f"runs:/{best_run_id}/model"

registered_model = mlflow.register_model(model_uri, model_name)

print(f"Model registered: {model_name}")
print(f"Version: {registered_model.version}")
print(f"Source run: {best_run['run_name']}")

# COMMAND ----------

# Set the champion alias
from mlflow import MlflowClient
client = MlflowClient()

client.update_registered_model(
    name=model_name,
    description=(
        "NSW Load Forecasting Model\n\n"
        "**Purpose:** Predict 5-minute electricity demand for NSW region\n\n"
        "**Features:** Time-based features + demand lags + rolling stats + price features\n\n"
        "**Training data:** NEMWEB DISPATCHREGIONSUM + DISPATCHPRICE + BOM Weather (silver_demand_weather)\n\n"
        "**Owner:** Load Forecasting Team"
    ),
)

client.set_registered_model_alias(model_name, "champion", registered_model.version)
print(f"Model '{model_name}' version {registered_model.version} aliased as 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What just happened - before and after
# MAGIC
# MAGIC | Aspect | Before | After (MLflow + Unity Catalog) |
# MAGIC |--------|--------|-------------------------------|
# MAGIC | **Experiment tracking** | Screenshots, notes in docs | Automatic logging of all params/metrics |
# MAGIC | **Model comparison** | Manual spreadsheets | `mlflow.search_runs()` + charts |
# MAGIC | **Model storage** | `model_final_FINAL_v2.pkl` | Versioned registry with lineage |
# MAGIC | **Production model** | "Ask Dave, he knows" | Clear alias: `@champion` |
# MAGIC | **Reproducibility** | "What hyperparams did I use?" | Full run history with code version |
# MAGIC | **Governance** | None | Unity Catalog access control + audit log |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Production-ready (10 min)
# MAGIC
# MAGIC Your model is tracked and registered. Now let's talk about getting it into production.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Model Serving - deploy as an API endpoint
# MAGIC
# MAGIC For load forecasting, most use cases are **batch** (generate daily forecasts).
# MAGIC But **real-time** endpoints are useful for:
# MAGIC - Intraday forecast updates
# MAGIC - Integration with trading systems
# MAGIC - Responding to sudden weather changes
# MAGIC
# MAGIC | Approach | Latency | Use case |
# MAGIC |----------|---------|----------|
# MAGIC | **Batch inference** | Minutes | Daily/weekly load forecast |
# MAGIC | **Real-time endpoint** | Milliseconds | On-demand "what's demand in 5 min?" |
# MAGIC | **Streaming** | Seconds | Continuous forecast updates |

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deploy as endpoint (what this looks like)
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="load-forecast-nsw",
# MAGIC     config={
# MAGIC         "served_entities": [{
# MAGIC             "entity_name": "workspace.ml_workshops.load_forecast_nsw",
# MAGIC             "entity_version": "1",
# MAGIC             "workload_size": "Small",
# MAGIC             "scale_to_zero_enabled": True,
# MAGIC         }]
# MAGIC     }
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Then call from anywhere:
# MAGIC ```bash
# MAGIC curl -X POST https://your-workspace.databricks.com/serving-endpoints/load-forecast-nsw/invocations \
# MAGIC   -H "Authorization: Bearer $TOKEN" \
# MAGIC   -d '{"dataframe_records": [{"hour": 14, "day_of_week": 2, "month": 7, ...}]}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Batch inference (more common for forecasting)
# MAGIC
# MAGIC Run daily to generate next-day forecasts:
# MAGIC
# MAGIC ```python
# MAGIC # Load champion model
# MAGIC model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
# MAGIC
# MAGIC # Score new data
# MAGIC predictions = model.predict(new_features_df)
# MAGIC
# MAGIC # Write to Delta table
# MAGIC spark.createDataFrame(predictions).write.mode("append").saveAsTable(
# MAGIC     f"{CATALOG}.{SCHEMA}.demand_forecasts"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Monitoring - know when things break
# MAGIC
# MAGIC Production ML needs observability. Lakehouse Monitoring gives you:
# MAGIC
# MAGIC | Feature | What it does |
# MAGIC |---------|-------------|
# MAGIC | **Inference tables** | Automatically capture every prediction + input |
# MAGIC | **Drift detection** | Alert when input data distribution changes |
# MAGIC | **Data quality** | Track null rates, schema changes, value distributions |
# MAGIC | **Custom metrics** | Monitor MAPE, MAE, or any metric you define |
# MAGIC | **Alerting** | Email/Slack when thresholds are breached |
# MAGIC
# MAGIC ```python
# MAGIC # Set up monitoring (one time)
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.quality_monitors.create(
# MAGIC     table_name=f"{CATALOG}.{SCHEMA}.demand_forecasts",
# MAGIC     assets_dir="/monitoring/load_forecast",
# MAGIC     schedule={"quartz_cron_expression": "0 0 * * * ?"},
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Scheduled retraining
# MAGIC
# MAGIC This notebook (or a production version) can run on a schedule:
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml
# MAGIC jobs:
# MAGIC   daily_load_forecast:
# MAGIC     tasks:
# MAGIC       - task_key: refresh_data
# MAGIC         pipeline_task:
# MAGIC           pipeline_id: ${resources.pipelines.nemweb_ml_pipeline.id}
# MAGIC
# MAGIC       - task_key: retrain_model
# MAGIC         depends_on: [refresh_data]
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./train_load_forecast.py
# MAGIC
# MAGIC       - task_key: evaluate
# MAGIC         depends_on: [retrain_model]
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./evaluate_and_promote.py
# MAGIC
# MAGIC     schedule:
# MAGIC       quartz_cron_expression: "0 0 6 * * ?"  # 6am daily
# MAGIC
# MAGIC     email_notifications:
# MAGIC       on_failure:
# MAGIC         - team@company.com
# MAGIC ```
# MAGIC
# MAGIC | Your cron jobs | Databricks Workflows |
# MAGIC |---------------|---------------------|
# MAGIC | Manual setup | Built-in scheduler |
# MAGIC | No retries | Automatic retry with backoff |
# MAGIC | Manual alerting | Email/Slack on failure |
# MAGIC | Always-on compute | Serverless (pay per use) |
# MAGIC | Log files (maybe) | Full run history with UI |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | What we covered | Takeaway |
# MAGIC |-----------------|----------|
# MAGIC | **Data pipelines** | Replace fragile Python scripts with monitored Lakeflow pipelines |
# MAGIC | **Real NEMWEB data** | Your actual data sources work natively (AEMO, BOM, internal) |
# MAGIC | **ML tracking** | MLflow tracks every experiment automatically |
# MAGIC | **Model comparison** | Compare XGBoost vs LightGBM vs sklearn systematically |
# MAGIC | **Model registry** | Unity Catalog gives versioning, lineage, and governance |
# MAGIC | **Production path** | Serving endpoints, monitoring, and scheduled retraining |
# MAGIC
# MAGIC ### Revisiting your pain points
# MAGIC
# MAGIC | Pain point | What we showed |
# MAGIC |------------|----------------|
# MAGIC | "Pull data from multiple servers" | Lakeflow pipeline with Auto Loader |
# MAGIC | "Which model is in production?" | Unity Catalog Model Registry with `@champion` alias |
# MAGIC | "Silent failures" | Workflow notifications + Lakehouse Monitoring |
# MAGIC | "Comparing experiments" | MLflow tracking UI |
# MAGIC | "Scattered data" | Everything in Unity Catalog |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A (5 min)
# MAGIC
# MAGIC **Discussion questions:**
# MAGIC
# MAGIC 1. Which part would be most useful for your current workflow?
# MAGIC 2. What would you try first?
# MAGIC 3. How does this compare to what you're using today (Plexos outputs, custom scripts)?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Concrete next steps:**
# MAGIC - Try running one of your own scripts in a Databricks notebook
# MAGIC - Wrap it with `mlflow.start_run()` - just 3 lines of code
# MAGIC - Explore the pipeline data in Unity Catalog
# MAGIC
# MAGIC **Resources:**
# MAGIC - MLflow docs: https://mlflow.org/docs/latest/index.html
# MAGIC - Lakeflow pipelines: Search "Lakeflow Declarative Pipelines" in Databricks docs
# MAGIC - This workshop: Available in the workshop repository
