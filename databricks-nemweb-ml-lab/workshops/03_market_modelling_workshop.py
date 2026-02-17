# Databricks notebook source
# MAGIC %md
# MAGIC # Market Modelling Workshop
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused (instructor drives, participants observe and ask questions)
# MAGIC
# MAGIC **Audience:** Market Modelling team - technical engineers who run Python scripts
# MAGIC for generation forecasts, battery optimisation, and outage modelling.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Section | Duration | Format |
# MAGIC |---------|----------|--------|
# MAGIC | **Pain points + platform intro** | 5 min | Discussion |
# MAGIC | **Live data streaming** | 15 min | Demo - real NEMWEB + BOM weather data streaming in |
# MAGIC | **ML: Price forecasting** | 25 min | Demo - feature engineering, XGBoost, MLflow tracking |
# MAGIC | **Predictions + Databricks App** | 10 min | Demo - score live data, show price forecast dashboard |
# MAGIC | **Q&A / next steps** | 5 min | Discussion |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Data:** This workshop uses **live AEMO NEMWEB data** (5-minute dispatch intervals)
# MAGIC and **BOM weather observations** - the same data your models consume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Pain Points (5 min)
# MAGIC
# MAGIC > **"What's the most frustrating part of your current modelling workflow?"**
# MAGIC
# MAGIC Common answers from market modelling teams:
# MAGIC
# MAGIC | Pain point | Sound familiar? |
# MAGIC |------------|----------------|
# MAGIC | "We run Python scripts that pull from AEMO and Plexos" | Fragile, manual, hard to debug |
# MAGIC | "Battery optimisation models run overnight with no monitoring" | Find out from traders when it fails |
# MAGIC | "Outage model versioning is a mess" | Which version is running? |
# MAGIC | "Comparing model runs means digging through old outputs" | No systematic tracking |
# MAGIC | "Getting generation forecasts to production is a multi-day process" | No CI/CD for models |
# MAGIC
# MAGIC ```
# MAGIC Your current state:
# MAGIC
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │ AEMO NEMWEB │     │   Plexos    │     │ Internal DB │
# MAGIC └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
# MAGIC        │                   │                   │
# MAGIC        ▼                   ▼                   ▼
# MAGIC    ┌───────────────────────────────────────────────┐
# MAGIC    │           Python Scripts (cron/manual)        │
# MAGIC    │  - Fragile pipelines  - No versioning         │
# MAGIC    │  - Manual scheduling  - No experiment tracking│
# MAGIC    └───────────────────────────────────────────────┘
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
# MAGIC joined with **BOM weather observations**. This is the same data your price
# MAGIC models need - demand, generation, interconnectors, and weather.

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
# MAGIC the NEMWEB CURRENT folder. New 5-minute dispatch files are picked up automatically.
# MAGIC
# MAGIC ```python
# MAGIC # Stream live NEM price data
# MAGIC spark.dataSource.register(NemwebStreamDataSource)
# MAGIC
# MAGIC df = (spark.readStream
# MAGIC       .format("nemweb_stream")
# MAGIC       .option("table", "DISPATCHPRICE")
# MAGIC       .option("regions", "NSW1,VIC1,QLD1,SA1,TAS1")
# MAGIC       .load())
# MAGIC ```
# MAGIC
# MAGIC **[Switch to Lakeflow UI to show the streaming pipeline DAG]**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Live price data (streaming from NEMWEB)

# COMMAND ----------

df_price = spark.table(f"{CATALOG}.{SCHEMA}.bronze_price_stream")

print(f"Live price data: {df_price.count():,} rows")
display(df_price.orderBy(df_price.columns[0], ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Dispatch data (demand, generation, interconnectors)

# COMMAND ----------

df_demand = spark.table(f"{CATALOG}.{SCHEMA}.bronze_dispatch_stream")

print(f"Live dispatch data: {df_demand.count():,} rows")
display(df_demand.orderBy(df_demand.columns[0], ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 BOM weather data
# MAGIC
# MAGIC Weather affects both demand (temperature -> heating/cooling) and supply
# MAGIC (wind speed -> wind generation, cloud cover -> solar output).

# COMMAND ----------

df_weather = spark.table(f"{CATALOG}.{SCHEMA}.bronze_bom_weather")

print(f"BOM weather observations: {df_weather.count():,} rows")
display(df_weather.orderBy("observation_time", ascending=False).limit(25))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Silver layer - ML-ready joined data
# MAGIC
# MAGIC The pipeline joins dispatch + price + weather for you. This is what your
# MAGIC price model will train on.

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
# MAGIC ### 1.6 The pipeline architecture
# MAGIC
# MAGIC ```
# MAGIC NEMWEB CURRENT API (HTTP/ZIP/CSV)     BOM Weather API (JSON)
# MAGIC         │                                       │
# MAGIC         │  nemweb_stream (custom datasource)    │  bom_weather.py
# MAGIC         ▼                                       ▼
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │  Bronze                                                      │
# MAGIC │  ├── bronze_dispatch_stream  (demand, generation, intercon.) │
# MAGIC │  ├── bronze_price_stream     (RRP, spot prices)              │
# MAGIC │  └── bronze_bom_weather      (temperature, wind, humidity)   │
# MAGIC │                                                              │
# MAGIC │  Silver                                                      │
# MAGIC │  └── silver_demand_weather   (all features joined)           │
# MAGIC │                                                              │
# MAGIC │  Gold                                                        │
# MAGIC │  ├── gold_demand_hourly      (hourly demand + weather)       │
# MAGIC │  └── gold_price_hourly       (hourly price stats)            │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: ML on Live Energy Data (25 min)
# MAGIC
# MAGIC Now let's build a **price forecasting model** using the live data.
# MAGIC
# MAGIC **Goal:** Predict `RRP` (Regional Reference Price, $/MWh) for NSW using
# MAGIC demand level, generation availability, interconnector flows, weather, and time features.
# MAGIC
# MAGIC Price forecasting is harder than demand forecasting - prices are spiky
# MAGIC and driven by the supply-demand margin. This is where it gets interesting.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure MLflow
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/market_modelling_workshop"
mlflow.set_experiment(experiment_name)

print(f"MLflow experiment: {experiment_name}")
print(f"Model registry: Unity Catalog ({CATALOG}.{SCHEMA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Load the silver data

# COMMAND ----------

df_nsw = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .orderBy("settlement_date")
)

pdf = df_nsw.toPandas()
pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
pdf = pdf.set_index("settlement_date").sort_index()

print(f"NSW market data: {len(pdf):,} rows")
print(f"Date range: {pdf.index.min()} to {pdf.index.max()}")
print(f"\nPrice summary:")
print(f"  Mean RRP:  ${pdf['rrp'].mean():.2f}/MWh")
print(f"  Max RRP:   ${pdf['rrp'].max():.2f}/MWh")
print(f"  Min RRP:   ${pdf['rrp'].min():.2f}/MWh")
print(f"  Std RRP:   ${pdf['rrp'].std():.2f}/MWh")

display(pdf.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Fetch live BOM weather (demo of external data source)

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
# MAGIC ### 2.3 Feature engineering for price forecasting
# MAGIC
# MAGIC Price forecasting uses different features than demand forecasting.
# MAGIC The key drivers of spot price are:
# MAGIC
# MAGIC 1. **Demand-supply margin** (available generation minus demand) - tighter margin = higher price
# MAGIC 2. **Demand level** - high demand pushes price up
# MAGIC 3. **Interconnector flows** - import/export between regions
# MAGIC 4. **Weather** - affects renewable output (wind, solar)
# MAGIC 5. **Price momentum** - recent price history

# COMMAND ----------

def create_price_forecast_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for price forecasting (predicting RRP).

    The key driver of NEM spot prices is the demand-supply margin.
    When available generation barely exceeds demand, prices spike
    as expensive peaking generators are dispatched.
    """
    out = df.copy()

    # -- Demand-supply margin (THE key price driver) --
    out["supply_margin_mw"] = out["available_generation_mw"] - out["total_demand_mw"]

    # -- Demand features (demand drives price) --
    out["demand_lag_1"] = out["total_demand_mw"].shift(1)
    out["demand_lag_12"] = out["total_demand_mw"].shift(12)
    out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()

    # AEMO demand forecast (what the market expects)
    if "demand_forecast_mw" in out.columns:
        out["demand_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    # -- Supply features --
    out["gen_lag_1"] = out["available_generation_mw"].shift(1)
    out["margin_lag_1"] = out["supply_margin_mw"].shift(1)
    out["margin_rolling_1h_mean"] = out["supply_margin_mw"].rolling(12).mean()
    out["margin_rolling_1h_min"] = out["supply_margin_mw"].rolling(12).min()

    # -- Interconnector features --
    out["interchange_lag_1"] = out["net_interchange_mw"].shift(1)
    out["interchange_rolling_1h"] = out["net_interchange_mw"].rolling(12).mean()

    # -- Price lag features (price momentum) --
    for lag in [1, 2, 3, 6, 12]:
        out[f"rrp_lag_{lag}"] = out["rrp"].shift(lag)
    out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()
    out["rrp_rolling_1h_std"] = out["rrp"].rolling(12).std()
    out["rrp_rolling_1h_max"] = out["rrp"].rolling(12).max()

    # -- Weather features (affects renewable output) --
    if "air_temp_c" in out.columns:
        out["air_temp_c"] = out["air_temp_c"].ffill()
        out["wind_speed_kmh"] = out["wind_speed_kmh"].ffill()

    # -- Time features --
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["is_weekend"] = out.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7)

    return out.dropna()


pdf_features = create_price_forecast_features(pdf)

# Identify feature columns (everything except target and metadata)
exclude_cols = ["rrp", "rop", "region_id", "_processed_at"]
feature_cols = [c for c in pdf_features.columns if c not in exclude_cols]

print(f"Features ({len(feature_cols)}):")
for c in sorted(feature_cols):
    print(f"  - {c}")
print(f"\nRows after feature engineering: {len(pdf_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Train/test split
# MAGIC
# MAGIC Temporal split (no random splits for time series).

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

X = pdf_features[feature_cols]
y = pdf_features["rrp"]

# Time-based split - last 20% for testing
split_idx = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Training set: {len(X_train):,} rows ({X_train.index.min()} to {X_train.index.max()})")
print(f"Test set:     {len(X_test):,} rows ({X_test.index.min()} to {X_test.index.max()})")
print(f"\nPrice range in test set: ${y_test.min():.2f} to ${y_test.max():.2f}/MWh")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Train XGBoost with MLflow tracking
# MAGIC
# MAGIC Price forecasting is harder than demand - prices are spiky and can jump
# MAGIC from $50 to $15,000 in a single interval. The model will capture the
# MAGIC normal patterns well but struggle with extreme events.

# COMMAND ----------

import xgboost as xgb

with mlflow.start_run(run_name="xgboost_price_forecast"):
    params = {
        "n_estimators": 300,
        "max_depth": 8,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
    }

    mlflow.log_params(params)
    mlflow.log_param("model_type", "xgboost")
    mlflow.log_param("target", "rrp")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = xgb.XGBRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae_dollars": mean_absolute_error(y_test, y_pred),
        "rmse_dollars": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape_pct": mean_absolute_percentage_error(y_test, y_pred) * 100,
    }
    mlflow.log_metrics(metrics)

    # input_example auto-infers the signature for UC
    mlflow.sklearn.log_model(model, "model", input_example=X_train.iloc[:5])

    # Log feature importance
    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    mlflow.log_table(importance_df, "feature_importance.json")

    best_run_id = mlflow.active_run().info.run_id

    print(f"XGBoost Price Forecast Results:")
    print(f"  MAE:  ${metrics['mae_dollars']:.2f}/MWh")
    print(f"  RMSE: ${metrics['rmse_dollars']:.2f}/MWh")
    print(f"  MAPE: {metrics['mape_pct']:.2f}%")
    print(f"\nTop features (note: supply margin is the key driver):")
    display(importance_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Discussion: Why price forecasting is hard
# MAGIC
# MAGIC | Challenge | NEM Reality |
# MAGIC |-----------|-------------|
# MAGIC | **Price spikes** | RRP can jump from $50 to $16,600/MWh (market cap) in one interval |
# MAGIC | **Negative prices** | -$1,000/MWh when renewables flood the market |
# MAGIC | **Regime changes** | Summer vs winter, drought vs flood, coal retirements |
# MAGIC | **Strategic bidding** | Generators bid strategically, not just on cost |
# MAGIC | **Interconnector congestion** | Regional price separation when lines are full |
# MAGIC
# MAGIC > **"What else would you add to this model?"** (fuel costs, outage schedules,
# MAGIC > renewable forecasts, bidding patterns, constraint information...)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Register model to Unity Catalog

# COMMAND ----------

model_name = f"{CATALOG}.{SCHEMA}.price_forecast_nsw"
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
        "NSW Price Forecasting Model\n\n"
        "**Target:** 5-minute spot price (RRP) for NSW region\n\n"
        "**Key features:** Supply margin, demand level, price lags, interconnector flows, weather\n\n"
        "**Training data:** NEMWEB DISPATCHREGIONSUM + DISPATCHPRICE + BOM Weather\n\n"
        "**Owner:** Market Modelling Team"
    ),
)

client.set_registered_model_alias(model_name, "champion", registered_model.version)
print(f"Model '{model_name}' version {registered_model.version} aliased as 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Predictions + Databricks App (10 min)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Score latest data with the champion model

# COMMAND ----------

champion_model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

predictions = champion_model.predict(X_test)

pred_df = pd.DataFrame({
    "settlement_date": X_test.index,
    "region_id": "NSW1",
    "actual_rrp": y_test.values,
    "predicted_rrp": predictions,
    "forecast_error_dollars": predictions - y_test.values,
    "model_version": int(registered_model.version),
    "scored_at": datetime.now(),
})

print(f"Predictions: {len(pred_df):,} rows")
print(f"MAE: ${pred_df['forecast_error_dollars'].abs().mean():.2f}/MWh")

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
    .saveAsTable(f"{CATALOG}.{SCHEMA}.price_predictions")
)

print(f"Predictions written to {CATALOG}.{SCHEMA}.price_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 The Databricks App
# MAGIC
# MAGIC The price forecast dashboard reads from the `price_predictions` table and shows:
# MAGIC - **Actual vs predicted RRP** over time
# MAGIC - **Forecast error** by region
# MAGIC - **Model metrics** (MAE, RMSE in $/MWh)
# MAGIC - **Auto-refresh** every 60 seconds
# MAGIC
# MAGIC **[Switch to the Databricks App - select the "Price Forecast" tab]**
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │  NEM Price Forecast Dashboard                   [LIVE] 🔴   │
# MAGIC ├──────────────────────────────────────────────────────────────┤
# MAGIC │                                                              │
# MAGIC │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐              │
# MAGIC │  │ NSW1 │ │ VIC1 │ │ QLD1 │ │ SA1  │ │ TAS1 │              │
# MAGIC │  │$72.50│ │$68.20│ │$85.10│ │$92.30│ │$55.40│ actual $/MWh│
# MAGIC │  │$70.80│ │$67.50│ │$83.40│ │$90.10│ │$54.20│ predicted   │
# MAGIC │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘              │
# MAGIC │                                                              │
# MAGIC │  [===== Actual vs Predicted RRP (24h) =====]                 │
# MAGIC │                                                              │
# MAGIC │  Model: XGBoost v1 │ MAE: $5.20/MWh │ Top: supply_margin   │
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
# MAGIC | **External data join** | BOM weather joined with dispatch + price data |
# MAGIC | **Price features** | Supply margin, demand, interconnectors, price lags |
# MAGIC | **MLflow tracking** | Every experiment tracked automatically |
# MAGIC | **Model registry** | Versioned models in Unity Catalog with `@champion` alias |
# MAGIC | **Predictions** | Scored data written to Delta, displayed in Databricks App |
# MAGIC
# MAGIC ### Revisiting your pain points
# MAGIC
# MAGIC | Pain point | What we showed |
# MAGIC |------------|----------------|
# MAGIC | "Run Python scripts pulling from AEMO and Plexos" | Streaming pipeline with automatic retry |
# MAGIC | "Battery/outage models run with no monitoring" | Pipeline UI + alerting + Lakehouse Monitoring |
# MAGIC | "Model versioning is a mess" | Unity Catalog Model Registry |
# MAGIC | "Comparing model runs is painful" | MLflow tracking UI |
# MAGIC | "Getting to production takes days" | Batch inference + Databricks App |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A (5 min)
# MAGIC
# MAGIC **Discussion questions:**
# MAGIC
# MAGIC 1. Which part would help your generation forecast / battery optimisation workflow most?
# MAGIC 2. What additional data sources would improve the price model? (fuel costs, outage schedules, FCAS)
# MAGIC 3. How would you integrate this with your existing Plexos workflow?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Concrete next steps:**
# MAGIC - Try running one of your Python modelling scripts in a Databricks notebook
# MAGIC - Wrap it with `mlflow.start_run()` - just 3 lines of code to get tracking
# MAGIC - Explore the pipeline data in Unity Catalog
# MAGIC
# MAGIC **Resources:**
# MAGIC - MLflow docs: https://mlflow.org/docs/latest/index.html
# MAGIC - Lakeflow pipelines: Search "Lakeflow Declarative Pipelines" in Databricks docs
# MAGIC - This workshop: Available in the workshop repository
