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
dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "ml_workshops", "Schema")
dbutils.widgets.text("history_days", "90", "History Days")
dbutils.widgets.text("experiment_path", "", "MLflow Experiment Path (optional)")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
try:
    HISTORY_DAYS = max(1, int(dbutils.widgets.get("history_days")))
except ValueError:
    HISTORY_DAYS = 90
EXPERIMENT_PATH = dbutils.widgets.get("experiment_path").strip()

print(f"Reading from: {CATALOG}.{SCHEMA}")
print(f"Training history window: last {HISTORY_DAYS} day(s)")
if EXPERIMENT_PATH:
    print(f"MLflow experiment override: {EXPERIMENT_PATH}")

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

# Column names may be uppercase (REGIONID) from NEMWEB or lowercase (region_id) from transforms
region_col = "REGIONID" if "REGIONID" in df_demand.columns else "region_id"
regions = [r[0] for r in df_demand.select(region_col).distinct().collect()]

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
# MAGIC ### 1.4 Rooftop solar PV - the duck curve
# MAGIC
# MAGIC Behind-the-meter rooftop solar **suppresses operational demand** by 5-10 GW nationally
# MAGIC at midday. Without accounting for this, load forecasts systematically overpredict
# MAGIC during daylight hours. AEMO publishes satellite-derived estimates every 30 minutes.

# COMMAND ----------

df_pv = spark.table(f"{CATALOG}.{SCHEMA}.bronze_rooftop_pv_stream")

print(f"Rooftop PV data: {df_pv.count():,} rows")
display(
    df_pv
    .filter("REGIONID = 'NSW1'")
    .orderBy("INTERVAL_DATETIME", ascending=False)
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussion:** At midday, rooftop solar can be generating 5+ GW across the NEM.
# MAGIC > That's 5 GW that doesn't show up in "operational demand" - but your grid-connected
# MAGIC > generators still need to ramp down. If your demand forecast doesn't account for
# MAGIC > rooftop PV, you'll systematically overforecast during daylight hours.
# MAGIC >
# MAGIC > This is the **duck curve** - demand dips at midday then surges at sunset when
# MAGIC > solar drops off and everyone turns on air conditioning.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Silver layer - ML-ready joined data
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
# MAGIC ### 1.6 The pipeline that does all this
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
# MAGIC │  ├── bronze_p5min_forecast      (AEMO 5-min pre-dispatch)  ★NEW │
# MAGIC │  ├── bronze_rooftop_pv_stream   (satellite solar estimates)★NEW │
# MAGIC │  ├── bronze_scada_stream        (unit-level MW output)     ★NEW │
# MAGIC │  ├── bronze_interconnector_stream (cross-region flows)     ★NEW │
# MAGIC │  └── bronze_bom_weather         (temperature, humidity, wind)    │
# MAGIC │                                                                  │
# MAGIC │  Silver                                                          │
# MAGIC │  ├── silver_demand_weather      (demand + price + weather)       │
# MAGIC │  ├── silver_forecast_vs_actual  (AEMO forecast accuracy)   ★NEW │
# MAGIC │  └── silver_supply_stack        (NEM-wide generation)      ★NEW │
# MAGIC │                                                                  │
# MAGIC │  Gold                                                            │
# MAGIC │  ├── gold_demand_hourly         (hourly demand + weather)        │
# MAGIC │  ├── gold_price_hourly          (hourly price stats)             │
# MAGIC │  ├── gold_forecast_accuracy     (AEMO P5MIN MAPE/bias)    ★NEW │
# MAGIC │  └── gold_interconnector_hourly (interconnector utilisation)★NEW│
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
current_user = spark.sql("SELECT current_user()").first()[0]
default_experiment = (
    f"/Users/{current_user}/load_forecast_workshop"
    if current_user and str(current_user).lower() != "none"
    else None
)

experiment_candidates = []
if EXPERIMENT_PATH:
    experiment_candidates.append(EXPERIMENT_PATH)
if default_experiment and default_experiment != EXPERIMENT_PATH:
    experiment_candidates.append(default_experiment)

configured_experiment = None
for candidate in experiment_candidates:
    try:
        mlflow.set_experiment(candidate)
        configured_experiment = candidate
        break
    except Exception as e:
        print(f"Could not set MLflow experiment '{candidate}': {e}")

if configured_experiment is None:
    print("Falling back to default job-linked MLflow experiment.")
else:
    print(f"MLflow experiment: {configured_experiment}")
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
    .filter(f"settlement_date >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS")
    # Avoid Arrow conversion issues in toPandas() for mixed/object-like timestamp columns.
    .drop("_processed_at", "weather_observation_time")
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

def _inline_weather_sample(region_id: str = "NSW1") -> pd.DataFrame:
    """Generate a minimal synthetic weather sample when module import fails."""
    now = pd.Timestamp.utcnow().floor("5min")
    return pd.DataFrame(
        {
            "observation_time": [now],
            "region_id": [region_id],
            "station_name": ["SYDNEY"],
            "air_temp_c": [24.0],
            "apparent_temp_c": [25.0],
            "rel_humidity_pct": [60.0],
            "wind_speed_kmh": [14.0],
            "rain_since_9am_mm": [0.0],
            "_processed_at": [now],
        }
    )


try:
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
except Exception as e:
    print(f"bom_weather import unavailable ({e}), using inline sample weather")
    weather = _inline_weather_sample("NSW1")
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
    # Keep only lags that can be computed with available history.
    candidate_lags = [1, 2, 3, 6, 12, 48, 288]
    usable_lags = [lag for lag in candidate_lags if lag < len(out)]
    for lag in usable_lags:
        out[f"demand_lag_{lag}"] = out["total_demand_mw"].shift(lag)

    # -- Rolling statistics --
    if len(out) >= 12:
        out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()
        out["demand_rolling_1h_std"] = out["total_demand_mw"].rolling(12).std()
    if len(out) >= 288:
        out["demand_rolling_1d_mean"] = out["total_demand_mw"].rolling(288).mean()

    # -- Price signal (price-responsive demand) --
    out["rrp_lag_1"] = out["rrp"].shift(1)
    if len(out) >= 12:
        out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()

    # -- AEMO benchmark (their forecast vs actual) --
    if "demand_forecast_mw" in out.columns:
        out["aemo_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    return out.dropna()


pdf_features = create_load_forecast_features(pdf)

if len(pdf_features) < 2:
    print(
        f"Only {len(pdf_features)} rows after full feature engineering; "
        "switching to a minimal feature set for small demo windows."
    )
    minimal = pdf.copy()
    minimal["hour"] = minimal.index.hour
    minimal["day_of_week"] = minimal.index.dayofweek
    minimal["is_weekend"] = minimal.index.dayofweek.isin([5, 6]).astype(int)
    minimal["demand_lag_1"] = minimal["total_demand_mw"].shift(1)
    minimal["rrp_lag_1"] = minimal["rrp"].shift(1)
    if "air_temp_c" in minimal.columns:
        minimal["air_temp_c"] = minimal["air_temp_c"].ffill().bfill()
    pdf_features = minimal.dropna(subset=["total_demand_mw", "demand_lag_1", "rrp_lag_1"])

# Identify feature columns (everything except target and metadata)
exclude_cols = ["total_demand_mw", "region_id", "rop", "_processed_at"]
feature_cols = [c for c in pdf_features.columns if c not in exclude_cols]
numeric_feature_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(pdf_features[c])]
dropped_non_numeric = sorted(set(feature_cols) - set(numeric_feature_cols))
feature_cols = numeric_feature_cols

if dropped_non_numeric:
    print("Dropping non-numeric features:")
    for c in dropped_non_numeric:
        print(f"  - {c}")
if not feature_cols:
    raise ValueError("No numeric feature columns available for model training.")

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
if len(X) < 2:
    raise ValueError(
        f"Not enough rows after feature engineering ({len(X)}). "
        "Let the streaming pipeline run longer, then rerun the workshop."
    )

split_idx = int(len(X) * 0.8)
split_idx = max(1, min(split_idx, len(X) - 1))
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

try:
    import xgboost as xgb

    model_name_for_run = "xgboost"
    model_params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
    }

    def build_model():
        return xgb.XGBRegressor(**model_params, random_state=42)

except Exception as e:
    from sklearn.ensemble import RandomForestRegressor

    print(f"xgboost unavailable ({e}); falling back to RandomForestRegressor")
    model_name_for_run = "random_forest_fallback"
    model_params = {
        "n_estimators": 300,
        "max_depth": 12,
        "min_samples_leaf": 2,
        "n_jobs": -1,
    }

    def build_model():
        return RandomForestRegressor(**model_params, random_state=42)

with mlflow.start_run(run_name="xgboost_load_forecast"):
    mlflow.log_params(model_params)
    mlflow.log_param("model_type", model_name_for_run)
    mlflow.log_param("target", "total_demand_mw")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = build_model()
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
model_load_uri = model_uri
registered_model_version = -1

from mlflow import MlflowClient
client = MlflowClient()

try:
    registered_model = mlflow.register_model(model_uri, model_name)
    registered_model_version = int(registered_model.version)
    print(f"Model registered: {model_name}")
    print(f"Version: {registered_model.version}")

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
    model_load_uri = f"models:/{model_name}@champion"
    print(f"Model '{model_name}' version {registered_model.version} aliased as 'champion'")
except Exception as e:
    print(f"Model registry step skipped ({e}). Using run model URI for scoring: {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 UC registered models: inspect versions and champion alias
# MAGIC
# MAGIC Before deployment, verify which version is serving as `@champion`.

# COMMAND ----------

print(f"Registered model: {model_name}")
try:
    for mv in client.search_model_versions(f"name = '{model_name}'"):
        raw_aliases = getattr(mv, "aliases", None)
        if raw_aliases is None:
            aliases = "-"
        elif isinstance(raw_aliases, str):
            aliases = raw_aliases
        else:
            try:
                alias_list = list(raw_aliases)
                aliases = ", ".join(alias_list) if alias_list else "-"
            except TypeError:
                aliases = str(raw_aliases)
        stage = mv.current_stage if getattr(mv, "current_stage", None) else "-"
        print(f"  v{mv.version} | aliases={aliases} | stage={stage} | run_id={mv.run_id}")

    champion = client.get_model_version_by_alias(model_name, "champion")
    print(f"\nChampion alias currently points to version {champion.version}")
except Exception as e:
    print(f"Model alias inspection skipped ({e}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8 Model serving for the champion model (MCP + SDK)
# MAGIC
# MAGIC This workshop uses batch scoring for simplicity, but this same UC model can be deployed
# MAGIC as a real-time endpoint for intraday forecast requests.
# MAGIC
# MAGIC **MCP-first workflow (recommended for demos):**
# MAGIC ```python
# MAGIC # Check endpoint state
# MAGIC get_serving_endpoint_status(name="load-forecast-nsw")
# MAGIC
# MAGIC # Query when READY
# MAGIC query_serving_endpoint(
# MAGIC   name="load-forecast-nsw",
# MAGIC   dataframe_records=[{"hour": 14, "day_of_week": 2, "month": 7, "...": "..."}]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Databricks SDK workflow (create/update endpoint from notebook):**
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="load-forecast-nsw",
# MAGIC     config=EndpointCoreConfigInput(
# MAGIC         served_entities=[
# MAGIC             ServedEntityInput(
# MAGIC                 name="load-forecast-nsw-champion",
# MAGIC                 entity_name=model_name,
# MAGIC                 entity_version=str(champion.version),
# MAGIC                 workload_size="Small",
# MAGIC                 scale_to_zero_enabled=True,
# MAGIC             )
# MAGIC         ]
# MAGIC     ),
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **[Click the Experiments icon in the left sidebar to show the MLflow UI]**
# MAGIC
# MAGIC In the UI you can:
# MAGIC - Compare runs visually (parameter vs metric charts)
# MAGIC - Inspect feature importance artifacts
# MAGIC - See full reproducibility info (code version, environment, timestamps)
# MAGIC - Share results with teammates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 UC functions for shared forecasting logic
# MAGIC
# MAGIC UC functions let you centralize business logic so notebooks, dashboards, and SQL users
# MAGIC all use the same definitions instead of copy-pasted logic.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.demand_regime(
  total_demand_mw DOUBLE,
  air_temp_c DOUBLE
)
RETURNS STRING
RETURN CASE
  WHEN total_demand_mw >= 9000 THEN 'extreme_peak'
  WHEN total_demand_mw >= 7500 THEN 'peak'
  WHEN air_temp_c >= 35 THEN 'weather_driven_risk'
  WHEN total_demand_mw <= 4500 THEN 'off_peak'
  ELSE 'normal'
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.forecast_error_band(
  actual_demand_mw DOUBLE,
  predicted_demand_mw DOUBLE
)
RETURNS STRING
RETURN CASE
  WHEN abs(predicted_demand_mw - actual_demand_mw) < 50 THEN 'excellent'
  WHEN abs(predicted_demand_mw - actual_demand_mw) < 150 THEN 'acceptable'
  WHEN abs(predicted_demand_mw - actual_demand_mw) < 300 THEN 'watch'
  ELSE 'intervene'
END
""")

print(f"Created UC functions in {CATALOG}.{SCHEMA}:")
print("  - demand_regime(total_demand_mw, air_temp_c)")
print("  - forecast_error_band(actual_demand_mw, predicted_demand_mw)")

# COMMAND ----------

# MAGIC %md
# MAGIC Example usage from SQL, dashboards, or notebooks:

# COMMAND ----------

silver_cols = set(spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather").columns)
if "air_temp_c" in silver_cols:
    temp_select_expr = "air_temp_c"
    temp_arg_expr = "air_temp_c"
else:
    temp_select_expr = "CAST(NULL AS DOUBLE) AS air_temp_c"
    temp_arg_expr = "CAST(NULL AS DOUBLE)"
    print("Column air_temp_c not found; using NULL placeholder in example query.")

display(
    spark.sql(f"""
    SELECT
      settlement_date,
      region_id,
      total_demand_mw,
      {temp_select_expr},
      {CATALOG}.{SCHEMA}.demand_regime(total_demand_mw, {temp_arg_expr}) AS demand_regime
    FROM {CATALOG}.{SCHEMA}.silver_demand_weather
    WHERE region_id = 'NSW1'
    ORDER BY settlement_date DESC
    LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.10 UC metric view for streaming demand KPIs
# MAGIC
# MAGIC A metric view gives analysts a governed semantic layer over your streaming tables.
# MAGIC It keeps KPI definitions consistent across AI/BI dashboards, Genie, and SQL notebooks.
# MAGIC
# MAGIC > Requires Databricks Runtime 17.2+ or a compatible SQL warehouse.

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.demand_ops_metric_view
    WITH METRICS
    LANGUAGE YAML
    AS $$
      version: 1.1
      comment: "Streaming demand KPIs for load forecasting operations"
      source: {CATALOG}.{SCHEMA}.silver_demand_weather
      dimensions:
        - name: event_hour
          expr: DATE_TRUNC('HOUR', settlement_date)
        - name: region_id
          expr: region_id
        - name: demand_regime
          expr: {CATALOG}.{SCHEMA}.demand_regime(total_demand_mw, air_temp_c)
      measures:
        - name: interval_count
          expr: COUNT(1)
        - name: avg_demand_mw
          expr: AVG(total_demand_mw)
        - name: peak_demand_mw
          expr: MAX(total_demand_mw)
        - name: avg_temp_c
          expr: AVG(air_temp_c)
    $$
    """)
    print(f"Created metric view: {CATALOG}.{SCHEMA}.demand_ops_metric_view")
except Exception as e:
    print("Metric view creation skipped.")
    print("Check runtime/warehouse support (metric views require DBR 17.2+).")
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Example metric view query (`MEASURE()` is required for measure columns):

# COMMAND ----------

try:
    display(
        spark.sql(f"""
        SELECT
          event_hour,
          region_id,
          demand_regime,
          measure(interval_count) AS interval_count,
          measure(avg_demand_mw) AS avg_demand_mw,
          measure(peak_demand_mw) AS peak_demand_mw,
          measure(avg_temp_c) AS avg_temp_c
        FROM {CATALOG}.{SCHEMA}.demand_ops_metric_view
        WHERE region_id = 'NSW1'
        GROUP BY ALL
        ORDER BY event_hour DESC
        LIMIT 24
        """)
    )
except Exception as e:
    print("Metric view query skipped.")
    print("The metric view may be unavailable on this runtime/warehouse.")
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.11 Can you beat AEMO? (P5MIN benchmark)
# MAGIC
# MAGIC AEMO's P5MIN pre-dispatch engine produces demand forecasts every 5 minutes.
# MAGIC Let's compare our XGBoost model against the official AEMO forecast.
# MAGIC
# MAGIC > This is the question your team cares about: **is your model adding value
# MAGIC > beyond what AEMO already publishes for free?**

# COMMAND ----------

# Load AEMO's forecast accuracy (pre-computed in the pipeline)
df_aemo = spark.table(f"{CATALOG}.{SCHEMA}.gold_forecast_accuracy")

# AEMO's demand forecast accuracy for NSW
pdf_aemo = (
    df_aemo
    .filter("region_id = 'NSW1'")
    .filter(f"hour >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS")
    .orderBy("hour", ascending=False)
    .toPandas()
)

if len(pdf_aemo) > 0:
    aemo_mae = pdf_aemo["demand_mae_mw"].mean()
    aemo_mape = pdf_aemo["demand_mape_pct"].mean()
    aemo_bias = pdf_aemo["demand_bias_mw"].mean()

    print("=" * 60)
    print("AEMO P5MIN Demand Forecast (official benchmark)")
    print(f"  MAE:  {aemo_mae:.1f} MW")
    print(f"  MAPE: {aemo_mape:.2f}%")
    print(f"  Bias: {aemo_bias:+.1f} MW {'(over-forecasts)' if aemo_bias > 0 else '(under-forecasts)'}")
    print()
    print("Your XGBoost Model")
    print(f"  MAE:  {metrics['mae_mw']:.1f} MW")
    print(f"  MAPE: {metrics['mape_pct']:.2f}%")
    print()

    if metrics["mae_mw"] < aemo_mae:
        print(f"  >>> You beat AEMO by {aemo_mae - metrics['mae_mw']:.1f} MW!")
    else:
        print(f"  >>> AEMO wins by {metrics['mae_mw'] - aemo_mae:.1f} MW")
        print(f"  >>> (But with more features + tuning, you can close the gap)")
    print("=" * 60)

    # Log AEMO comparison to MLflow
    with mlflow.start_run(run_id=best_run_id):
        mlflow.log_metrics({
            "aemo_benchmark_mae_mw": aemo_mae,
            "aemo_benchmark_mape_pct": aemo_mape,
            "beat_aemo_by_mw": aemo_mae - metrics["mae_mw"],
        })
else:
    print("No AEMO forecast data available yet (P5MIN pipeline may still be ingesting)")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussion:** AEMO's P5MIN uses a full-scale dispatch engine with constraint
# MAGIC > modelling and generator bids. If your statistical model can match or beat it,
# MAGIC > that's genuinely valuable - especially for specific hours where AEMO is weakest
# MAGIC > (e.g., the evening ramp when solar drops off).
# MAGIC >
# MAGIC > Check `gold_forecast_accuracy` for hourly breakdowns - you'll often find
# MAGIC > that AEMO's errors are largest during rapid transitions (sunrise, sunset,
# MAGIC > weather fronts).

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

# Load model (champion alias when available, otherwise the just-trained run model)
champion_model = mlflow.pyfunc.load_model(model_load_uri)

# Score the test set (in production, this would be the latest streaming data)
predictions = champion_model.predict(X_test)

# Build predictions DataFrame
pred_df = pd.DataFrame({
    "settlement_date": X_test.index,
    "region_id": "NSW1",
    "actual_demand_mw": y_test.values,
    "predicted_demand_mw": predictions,
    "forecast_error_mw": predictions - y_test.values,
    "model_version": registered_model_version,
    "scored_at": datetime.now(),
})

print(f"Predictions: {len(pred_df):,} rows")
print(f"MAE: {pred_df['forecast_error_mw'].abs().mean():.1f} MW")
print(
    "MAPE: "
    f"{(pred_df['forecast_error_mw'].abs() / pred_df['actual_demand_mw'].abs().clip(lower=0.01)).mean() * 100:.2f}%"
)

display(pred_df.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Write predictions to Delta table

# COMMAND ----------

spark_pred_df = spark.createDataFrame(pred_df)
target_table = f"{CATALOG}.{SCHEMA}.demand_predictions_workshop"

# Recreate as Delta each run to avoid failures when a non-Delta object already exists.
for drop_stmt in [
    f"DROP MATERIALIZED VIEW IF EXISTS {target_table}",
    f"DROP VIEW IF EXISTS {target_table}",
    f"DROP TABLE IF EXISTS {target_table}",
]:
    try:
        spark.sql(drop_stmt)
    except Exception:
        pass

(
    spark_pred_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Predictions written to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 The Databricks App
# MAGIC
# MAGIC This workshop writes to `demand_predictions_workshop` for isolation. The app reads the
# MAGIC pipeline-managed `demand_predictions` table and shows:
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
# MAGIC | **Rooftop solar PV** | The duck curve - why you must account for behind-the-meter solar |
# MAGIC | **Feature engineering** | Time, temperature, demand lags, rolling stats |
# MAGIC | **MLflow tracking** | Every experiment tracked automatically |
# MAGIC | **Model registry** | Versioned models in Unity Catalog with `@champion` alias |
# MAGIC | **UC functions** | Shared forecasting logic reused by SQL, dashboards, and jobs |
# MAGIC | **UC metric view** | Governed KPI layer on top of streaming demand data |
# MAGIC | **AEMO P5MIN benchmark** | Compare your model against AEMO's official forecast |
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
# MAGIC 2. What other data sources would you want to integrate? (holiday calendars, EV charging, battery schedules)
# MAGIC 3. Where does AEMO's P5MIN forecast break down? Can you target those hours?
# MAGIC 4. How does this compare to your current pipeline and model management?
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
