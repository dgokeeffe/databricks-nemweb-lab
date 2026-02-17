# Databricks notebook source
# MAGIC %md
# MAGIC # Load Forecasting Workshop
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused (instructor drives, participants observe and ask questions)
# MAGIC
# MAGIC **Audience:** Load Forecasting team - technical engineers who build and maintain
# MAGIC their own forecasting systems, pulling data from multiple servers via Python scripts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Section | Duration | Format |
# MAGIC |---------|----------|--------|
# MAGIC | **Pain points + platform intro** | 5 min | Discussion |
# MAGIC | **Live data streaming** | 15 min | Demo - real NEMWEB + BOM weather data streaming in |
# MAGIC | **ML: Load forecasting** | 25 min | Demo - feature engineering, XGBoost, MLflow tracking |
# MAGIC | **Predictions + Databricks App** | 10 min | Demo - score live data, show forecast dashboard |
# MAGIC | **Q&A / next steps** | 5 min | Discussion |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Data:** This workshop uses **live AEMO NEMWEB data** (5-minute dispatch intervals)
# MAGIC and **BOM weather observations** (temperature, humidity, wind) - the same data sources
# MAGIC your team works with every day.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Pain Points (5 min)
# MAGIC
# MAGIC > **"What's the most annoying part of your current workflow?"**
# MAGIC
# MAGIC Common answers we hear from load forecasting teams:
# MAGIC
# MAGIC | Pain point | Sound familiar? |
# MAGIC |------------|----------------|
# MAGIC | "We pull data from AEMO, BOM, and internal DBs via scripts" | Fragile, no retry, manual scheduling |
# MAGIC | "We don't know which model version is in production" | `model_final_FINAL_v2.pkl` on a shared drive |
# MAGIC | "When the overnight forecast fails, we find out from traders" | No alerting, no observability |
# MAGIC | "Comparing experiments means digging through notebooks" | No systematic tracking |
# MAGIC | "Data is scattered across multiple servers and formats" | CSV, Parquet, databases, APIs |
# MAGIC
# MAGIC ```
# MAGIC Your current state:
# MAGIC
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │ AEMO NEMWEB │     │ BOM Weather │     │ Internal DB │
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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Live Data Streaming (15 min)
# MAGIC
# MAGIC Let's look at **real AEMO NEMWEB data streaming into Databricks right now**,
# MAGIC joined with **BOM weather observations** - no pre-download step needed.

# COMMAND ----------

# Configuration - uses widget parameters from bundle job, with defaults for interactive use
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ml_workshops", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Reading from: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Custom streaming data source
# MAGIC
# MAGIC We built a custom PySpark streaming data source that reads directly from
# MAGIC the NEMWEB CURRENT folder. New 5-minute files are picked up automatically.
# MAGIC
# MAGIC ```python
# MAGIC # This is all you need to stream live NEM data
# MAGIC spark.dataSource.register(NemwebStreamDataSource)
# MAGIC
# MAGIC df = (spark.readStream
# MAGIC       .format("nemweb_stream")
# MAGIC       .option("table", "DISPATCHREGIONSUM")
# MAGIC       .option("regions", "NSW1,VIC1,QLD1,SA1,TAS1")
# MAGIC       .load())
# MAGIC ```
# MAGIC
# MAGIC **[Switch to Lakeflow UI to show the streaming pipeline DAG]**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Live dispatch data (streaming from NEMWEB)

# COMMAND ----------

df_demand = spark.table(f"{CATALOG}.{SCHEMA}.bronze_dispatch_stream")

row_count = df_demand.count()
regions = [r.region_id for r in df_demand.select("region_id").distinct().collect()] if hasattr(df_demand.schema["REGIONID"] if "REGIONID" in df_demand.columns else df_demand.schema[0], "name") else []

# Handle both column naming conventions
if "REGIONID" in df_demand.columns:
    regions = [r.REGIONID for r in df_demand.select("REGIONID").distinct().collect()]
else:
    regions = [r.region_id for r in df_demand.select("region_id").distinct().collect()]

print(f"Live dispatch data: {row_count:,} rows")
print(f"NEM regions: {sorted(regions)}")

display(df_demand.orderBy(df_demand.columns[0], ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 BOM weather data (joined with dispatch)
# MAGIC
# MAGIC Temperature is the **#1 driver** of electricity demand. We fetch BOM observations
# MAGIC for capital city weather stations near each NEM region.

# COMMAND ----------

df_weather = spark.table(f"{CATALOG}.{SCHEMA}.bronze_bom_weather")

print(f"BOM weather observations: {df_weather.count():,} rows")
display(df_weather.orderBy("observation_time", ascending=False).limit(25))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Silver layer - ML-ready joined data
# MAGIC
# MAGIC The pipeline automatically joins dispatch + price + weather, creating
# MAGIC the table your ML model will train on.

# COMMAND ----------

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
# MAGIC ### 1.5 The pipeline that does all this
# MAGIC
# MAGIC ```
# MAGIC NEMWEB CURRENT API (HTTP/ZIP/CSV)     BOM Weather API (JSON)
# MAGIC         │                                       │
# MAGIC         │  nemweb_stream (custom datasource)    │  bom_weather.py
# MAGIC         ▼                                       ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │  Bronze                                                      │
# MAGIC │  ├── bronze_dispatch_stream  (5-min demand, generation)      │
# MAGIC │  ├── bronze_price_stream     (5-min spot prices)             │
# MAGIC │  └── bronze_bom_weather      (temperature, humidity, wind)   │
# MAGIC │                                                              │
# MAGIC │  Silver                                                      │
# MAGIC │  └── silver_demand_weather   (demand + price + weather)      │
# MAGIC │                                                              │
# MAGIC │  Gold                                                        │
# MAGIC │  ├── gold_demand_hourly      (hourly demand + weather)       │
# MAGIC │  └── gold_price_hourly       (hourly price stats)            │
# MAGIC └──────────────────────────────────────────────────────────────┘
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
# MAGIC ---
# MAGIC ## Part 2: ML on Live Energy Data (25 min)
# MAGIC
# MAGIC Now let's build a **load forecasting model** using the live data we just saw streaming in.
# MAGIC
# MAGIC **Goal:** Predict `TOTALDEMAND` (MW) for NSW using time features, temperature, and demand history.
# MAGIC
# MAGIC This is representative of what your team does every day - but tracked,
# MAGIC reproducible, and governed.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure MLflow
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/load_forecast_workshop"
mlflow.set_experiment(experiment_name)

print(f"MLflow experiment: {experiment_name}")
print(f"Model registry: Unity Catalog ({CATALOG}.{SCHEMA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Load the silver data
# MAGIC
# MAGIC We'll use NSW demand from the silver table (demand + price + weather already joined).

# COMMAND ----------

df_nsw = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .orderBy("settlement_date")
)

pdf = df_nsw.toPandas()
pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
pdf = pdf.set_index("settlement_date").sort_index()

print(f"NSW demand data: {len(pdf):,} rows")
print(f"Date range: {pdf.index.min()} to {pdf.index.max()}")
print(f"\nColumns: {list(pdf.columns)}")

display(pdf.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Fetch live BOM weather (demo of external data source)
# MAGIC
# MAGIC Let's also show the live BOM API call - this is how you'd fetch weather
# MAGIC data in a production pipeline or notebook.

# COMMAND ----------

from bom_weather import fetch_bom_observations, generate_sample_weather

try:
    weather = fetch_bom_observations(regions=["NSW1"], latest_only=True, as_pandas=True)
    print("Live BOM weather for NSW:")
    display(weather)
except Exception as e:
    print(f"BOM API unavailable ({e}), using sample data")
    weather = pd.DataFrame(generate_sample_weather())
    weather = weather[weather["region_id"] == "NSW1"]
    display(weather)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Feature engineering
# MAGIC
# MAGIC These are the features your team already builds - time-based, temperature,
# MAGIC lag features, and rolling statistics. The key addition here is **temperature**
# MAGIC from BOM - the #1 driver of electricity demand.

# COMMAND ----------

def create_load_forecast_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for load forecasting (predicting TOTALDEMAND).

    Temperature is the primary driver of electricity demand:
    - Hot days -> air conditioning -> high demand
    - Cold days -> heating -> high demand
    - Mild days -> low demand
    """
    out = df.copy()

    # -- Time features --
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["month"] = out.index.month
    out["is_weekend"] = out.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding (captures circular nature of time)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7)

    # -- Weather features (temperature is #1 driver) --
    if "air_temp_c" in out.columns:
        out["air_temp_c"] = out["air_temp_c"].ffill()
        out["apparent_temp_c"] = out["apparent_temp_c"].ffill()
        out["rel_humidity_pct"] = out["rel_humidity_pct"].ffill()
        out["wind_speed_kmh"] = out["wind_speed_kmh"].ffill()

    # -- Demand lag features --
    # 5-min intervals: lag_1 = 5min ago, lag_12 = 1hr ago, lag_288 = 1 day ago
    for lag in [1, 2, 3, 6, 12, 48, 288]:
        out[f"demand_lag_{lag}"] = out["total_demand_mw"].shift(lag)

    # -- Rolling statistics --
    out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()
    out["demand_rolling_1h_std"] = out["total_demand_mw"].rolling(12).std()
    out["demand_rolling_1d_mean"] = out["total_demand_mw"].rolling(288).mean()

    # -- Price signal (price-responsive demand) --
    out["rrp_lag_1"] = out["rrp"].shift(1)
    out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()

    # -- AEMO benchmark (their forecast vs actual) --
    if "demand_forecast_mw" in out.columns:
        out["aemo_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    return out.dropna()


pdf_features = create_load_forecast_features(pdf)

# Identify feature columns (everything except target and metadata)
exclude_cols = ["total_demand_mw", "region_id", "rop", "_processed_at"]
feature_cols = [c for c in pdf_features.columns if c not in exclude_cols]

print(f"Features ({len(feature_cols)}):")
for c in sorted(feature_cols):
    print(f"  - {c}")
print(f"\nRows after feature engineering: {len(pdf_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Train/test split
# MAGIC
# MAGIC For time series, we use a **temporal split** - train on the past, test on the future.
# MAGIC Never random splits for time series!

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

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
# MAGIC ### 2.5 Train XGBoost with MLflow tracking
# MAGIC
# MAGIC Every experiment is **automatically tracked** - no more screenshots or `model_final_FINAL_v2.pkl`.

# COMMAND ----------

import xgboost as xgb

with mlflow.start_run(run_name="xgboost_load_forecast"):
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
    }

    mlflow.log_params(params)
    mlflow.log_param("model_type", "xgboost")
    mlflow.log_param("target", "total_demand_mw")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = xgb.XGBRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae_mw": mean_absolute_error(y_test, y_pred),
        "rmse_mw": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape_pct": mean_absolute_percentage_error(y_test, y_pred) * 100,
    }
    mlflow.log_metrics(metrics)

    # Log model to MLflow (input_example auto-infers the signature for UC)
    mlflow.sklearn.log_model(model, "model", input_example=X_train.iloc[:5])

    # Log feature importance
    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    mlflow.log_table(importance_df, "feature_importance.json")

    best_run_id = mlflow.active_run().info.run_id

    print(f"XGBoost Load Forecast Results:")
    print(f"  MAE:  {metrics['mae_mw']:.1f} MW")
    print(f"  RMSE: {metrics['rmse_mw']:.1f} MW")
    print(f"  MAPE: {metrics['mape_pct']:.2f}%")
    print(f"\nTop features:")
    display(importance_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Register best model to Unity Catalog
# MAGIC
# MAGIC Instead of `model_final_FINAL_v2.pkl` on a shared drive, you get a **versioned registry
# MAGIC with lineage** back to the training data and code.

# COMMAND ----------

model_name = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"
model_uri = f"runs:/{best_run_id}/model"

registered_model = mlflow.register_model(model_uri, model_name)

print(f"Model registered: {model_name}")
print(f"Version: {registered_model.version}")

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()

client.update_registered_model(
    name=model_name,
    description=(
        "NSW Load Forecasting Model\n\n"
        "**Target:** 5-minute electricity demand (TOTALDEMAND) for NSW region\n\n"
        "**Key features:** Temperature (BOM), demand lags, time-of-day, rolling stats\n\n"
        "**Training data:** NEMWEB DISPATCHREGIONSUM + DISPATCHPRICE + BOM Weather\n\n"
        "**Owner:** Load Forecasting Team"
    ),
)

client.set_registered_model_alias(model_name, "champion", registered_model.version)
print(f"Model '{model_name}' version {registered_model.version} aliased as 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC **[Click the Experiments icon in the left sidebar to show the MLflow UI]**
# MAGIC
# MAGIC In the UI you can:
# MAGIC - Compare runs visually (parameter vs metric charts)
# MAGIC - Inspect feature importance artifacts
# MAGIC - See full reproducibility info (code version, environment, timestamps)
# MAGIC - Share results with teammates

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Predictions + Databricks App (10 min)
# MAGIC
# MAGIC Now let's score the latest data and write predictions to a Delta table
# MAGIC that feeds the forecast dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Score latest data with the champion model

# COMMAND ----------

# Load the champion model
champion_model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

# Score the test set (in production, this would be the latest streaming data)
predictions = champion_model.predict(X_test)

# Build predictions DataFrame
pred_df = pd.DataFrame({
    "settlement_date": X_test.index,
    "region_id": "NSW1",
    "actual_demand_mw": y_test.values,
    "predicted_demand_mw": predictions,
    "forecast_error_mw": predictions - y_test.values,
    "model_version": int(registered_model.version),
    "scored_at": datetime.now(),
})

print(f"Predictions: {len(pred_df):,} rows")
print(f"MAE: {pred_df['forecast_error_mw'].abs().mean():.1f} MW")
print(f"MAPE: {(pred_df['forecast_error_mw'].abs() / pred_df['actual_demand_mw']).mean() * 100:.2f}%")

display(pred_df.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Write predictions to Delta table

# COMMAND ----------

spark_pred_df = spark.createDataFrame(pred_df)

(
    spark_pred_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.demand_predictions")
)

print(f"Predictions written to {CATALOG}.{SCHEMA}.demand_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 The Databricks App
# MAGIC
# MAGIC The forecast dashboard reads from the `demand_predictions` table and shows:
# MAGIC - **Actual vs predicted demand** over time
# MAGIC - **Forecast error** by region
# MAGIC - **Model metrics** (MAE, RMSE, MAPE)
# MAGIC - **Auto-refresh** every 60 seconds as new data arrives
# MAGIC
# MAGIC **[Switch to the Databricks App to show the live dashboard]**
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │  NEM Load Forecast Dashboard                    [LIVE] 🔴   │
# MAGIC ├──────────────────────────────────────────────────────────────┤
# MAGIC │                                                              │
# MAGIC │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐              │
# MAGIC │  │ NSW1 │ │ VIC1 │ │ QLD1 │ │ SA1  │ │ TAS1 │              │
# MAGIC │  │8,200 │ │5,400 │ │6,100 │ │1,900 │ │1,100 │ MW actual   │
# MAGIC │  │8,150 │ │5,380 │ │6,050 │ │1,920 │ │1,090 │ MW predicted│
# MAGIC │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘              │
# MAGIC │                                                              │
# MAGIC │  [===== Actual vs Predicted Demand (24h) =====]              │
# MAGIC │                                                              │
# MAGIC │  Model: XGBoost v1 │ MAE: 45 MW │ MAPE: 0.6%              │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | What we covered | Takeaway |
# MAGIC |-----------------|----------|
# MAGIC | **Live streaming** | NEMWEB + BOM data flows in automatically via Lakeflow |
# MAGIC | **External data join** | BOM weather (temperature) joined with dispatch data |
# MAGIC | **Feature engineering** | Time, temperature, demand lags, rolling stats |
# MAGIC | **MLflow tracking** | Every experiment tracked automatically |
# MAGIC | **Model registry** | Versioned models in Unity Catalog with `@champion` alias |
# MAGIC | **Predictions** | Scored data written to Delta, displayed in Databricks App |
# MAGIC
# MAGIC ### Revisiting your pain points
# MAGIC
# MAGIC | Pain point | What we showed |
# MAGIC |------------|----------------|
# MAGIC | "Pull data from AEMO, BOM via scripts" | Streaming pipeline + BOM weather join |
# MAGIC | "Which model is in production?" | Unity Catalog Model Registry with `@champion` |
# MAGIC | "Silent failures" | Pipeline UI with data quality + alerting |
# MAGIC | "Comparing experiments" | MLflow tracking UI |
# MAGIC | "Scattered data" | Everything in Unity Catalog |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A (5 min)
# MAGIC
# MAGIC **Discussion questions:**
# MAGIC
# MAGIC 1. Which part would be most useful for your current load forecasting workflow?
# MAGIC 2. What other data sources would you want to integrate? (e.g., solar forecasts, holiday calendars)
# MAGIC 3. How does this compare to your current pipeline and model management?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Concrete next steps:**
# MAGIC - Try running one of your own forecasting scripts in a Databricks notebook
# MAGIC - Wrap it with `mlflow.start_run()` - just 3 lines of code to get tracking
# MAGIC - Explore the pipeline data in Unity Catalog
# MAGIC
# MAGIC **Resources:**
# MAGIC - MLflow docs: https://mlflow.org/docs/latest/index.html
# MAGIC - Lakeflow pipelines: Search "Lakeflow Declarative Pipelines" in Databricks docs
# MAGIC - This workshop: Available in the workshop repository
