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
# MAGIC ### 1.5 Interconnector flows - when regions decouple
# MAGIC
# MAGIC Interconnector constraints are one of the biggest drivers of **regional price separation**.
# MAGIC When a line hits its limit (binding), the connected regions decouple and prices diverge.
# MAGIC
# MAGIC Key interconnectors:
# MAGIC - **N-Q-MNSP1**: NSW ↔ QLD
# MAGIC - **VIC1-NSW1**: VIC ↔ NSW (the main southern link)
# MAGIC - **V-SA**: VIC ↔ SA (Heywood - often binding)
# MAGIC - **T-V-MNSP1**: TAS ↔ VIC (Basslink undersea cable - when it trips, TAS prices go wild)

# COMMAND ----------

df_interconn = spark.table(f"{CATALOG}.{SCHEMA}.gold_interconnector_hourly")

print(f"Interconnector hourly data: {df_interconn.count():,} rows")
display(
    df_interconn
    .orderBy("hour", ascending=False)
    .limit(40)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussion:** When `avg_utilisation_pct` approaches 100%, the interconnector is
# MAGIC > binding and prices in the connected regions will diverge. A binding Heywood (V-SA)
# MAGIC > typically means SA prices spike relative to VIC. This is a **direct feature**
# MAGIC > for price forecasting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 SCADA supply stack - generator trips in real time
# MAGIC
# MAGIC DISPATCH_UNIT_SCADA shows the MW output of **every generating unit** in the NEM.
# MAGIC When a large generator trips (MW → 0), supply drops and prices spike - often
# MAGIC within a single 5-minute interval. This is the data that drives intra-day trading.

# COMMAND ----------

df_supply = spark.table(f"{CATALOG}.{SCHEMA}.silver_supply_stack")

print(f"Supply stack data: {df_supply.count():,} rows")
display(
    df_supply
    .orderBy("SETTLEMENTDATE", ascending=False)
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussion:** Watch `active_unit_count` vs `offline_unit_count`. A sudden drop
# MAGIC > in active units means a trip event. The `largest_unit_mw` column shows the
# MAGIC > single biggest generator - if that trips, it's a contingency event and
# MAGIC > prices respond immediately.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Silver layer - ML-ready joined data
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
# MAGIC ### 1.8 The pipeline architecture
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
# MAGIC │  ├── bronze_p5min_forecast      (AEMO 5-min pre-dispatch)  ★    │
# MAGIC │  ├── bronze_scada_stream        (unit-level MW output)     ★    │
# MAGIC │  ├── bronze_interconnector_stream (cross-region flows)     ★    │
# MAGIC │  ├── bronze_rooftop_pv_stream   (satellite solar estimates)★    │
# MAGIC │  └── bronze_bom_weather         (temperature, wind, humidity)    │
# MAGIC │                                                                  │
# MAGIC │  Silver                                                          │
# MAGIC │  ├── silver_demand_weather      (demand + price + weather)       │
# MAGIC │  ├── silver_forecast_vs_actual  (AEMO forecast accuracy)   ★    │
# MAGIC │  └── silver_supply_stack        (NEM-wide generation)      ★    │
# MAGIC │                                                                  │
# MAGIC │  Gold                                                            │
# MAGIC │  ├── gold_demand_hourly         (hourly demand + weather)        │
# MAGIC │  ├── gold_price_hourly          (hourly price stats)             │
# MAGIC │  ├── gold_forecast_accuracy     (AEMO P5MIN MAPE/bias)    ★    │
# MAGIC │  └── gold_interconnector_hourly (interconnector utilisation)★    │
# MAGIC └──────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ★ = Tables we added for the Load Forecasting and Market Modelling teams

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
current_user = spark.sql("SELECT current_user()").first()[0]
default_experiment = (
    f"/Users/{current_user}/market_modelling_workshop"
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
# MAGIC ### 2.3 Feature engineering for price forecasting
# MAGIC
# MAGIC Price forecasting uses different features than demand forecasting.
# MAGIC The key drivers of spot price are:
# MAGIC
# MAGIC 1. **Demand-supply margin** (available generation minus demand) - tighter margin = higher price
# MAGIC 2. **Demand level** - high demand pushes price up
# MAGIC 3. **Interconnector flows** - import/export between regions (binding = price separation)
# MAGIC 4. **Supply stack health** - generator trips cause immediate price spikes
# MAGIC 5. **Weather** - affects renewable output (wind, solar)
# MAGIC 6. **Price momentum** - recent price history (mean-reverting but with fat tails)

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
# MAGIC Temporal split (no random splits for time series).

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

X = pdf_features[feature_cols]
y = pdf_features["rrp"]

# Time-based split - last 20% for testing
if len(X) < 2:
    raise ValueError(
        "Not enough rows after feature engineering to create train/test split. "
        "Ensure the pipeline has ingested more history, then rerun."
    )

split_idx = int(len(X) * 0.8)
split_idx = max(1, min(split_idx, len(X) - 1))
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

try:
    import xgboost as xgb

    model_name_for_run = "xgboost"
    model_params = {
        "n_estimators": 300,
        "max_depth": 8,
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
        "n_estimators": 400,
        "max_depth": 14,
        "min_samples_leaf": 2,
        "n_jobs": -1,
    }

    def build_model():
        return RandomForestRegressor(**model_params, random_state=42)

with mlflow.start_run(run_name="xgboost_price_forecast"):
    mlflow.log_params(model_params)
    mlflow.log_param("model_type", model_name_for_run)
    mlflow.log_param("target", "rrp")
    mlflow.log_param("region", "NSW1")
    mlflow.log_param("feature_count", len(feature_cols))

    model = build_model()
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
# MAGIC ### 2.7 Can you beat AEMO? (P5MIN price benchmark)
# MAGIC
# MAGIC AEMO's P5MIN engine also produces **price forecasts**. Let's see how our
# MAGIC XGBoost compares to the official AEMO price forecast.
# MAGIC
# MAGIC > For market modellers, this is the ultimate question: **is your model generating
# MAGIC > alpha beyond what AEMO publishes?** If you can forecast price spikes better than
# MAGIC > P5MIN, that's direct value for bidding and battery dispatch.

# COMMAND ----------

df_aemo = spark.table(f"{CATALOG}.{SCHEMA}.gold_forecast_accuracy")

pdf_aemo = (
    df_aemo
    .filter("region_id = 'NSW1'")
    .filter(f"hour >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS")
    .orderBy("hour", ascending=False)
    .toPandas()
)

if len(pdf_aemo) > 0:
    aemo_price_mae = pdf_aemo["price_mae"].mean()
    aemo_price_bias = pdf_aemo["price_bias"].mean()

    print("=" * 60)
    print("AEMO P5MIN Price Forecast (official benchmark)")
    print(f"  MAE:  ${aemo_price_mae:.2f}/MWh")
    print(f"  Bias: ${aemo_price_bias:+.2f}/MWh {'(over-forecasts)' if aemo_price_bias > 0 else '(under-forecasts)'}")
    print()
    print("Your XGBoost Model")
    print(f"  MAE:  ${metrics['mae_dollars']:.2f}/MWh")
    print()

    if metrics["mae_dollars"] < aemo_price_mae:
        print(f"  >>> You beat AEMO by ${aemo_price_mae - metrics['mae_dollars']:.2f}/MWh!")
    else:
        print(f"  >>> AEMO wins by ${metrics['mae_dollars'] - aemo_price_mae:.2f}/MWh")
        print(f"  >>> (Try adding interconnector binding flags + supply stack features)")
    print("=" * 60)

    # Log AEMO comparison to MLflow
    with mlflow.start_run(run_id=best_run_id):
        mlflow.log_metrics({
            "aemo_benchmark_price_mae": aemo_price_mae,
            "beat_aemo_by_dollars": aemo_price_mae - metrics["mae_dollars"],
        })

    # Show hourly breakdown - where is AEMO weakest?
    print("\nAEMO forecast accuracy by hour (where are the opportunities?):")
    display(
        df_aemo
        .filter("region_id = 'NSW1'")
        .orderBy("hour")
        .select("hour", "price_mae", "price_bias", "demand_mae_mw", "interval_count")
    )
else:
    print("No AEMO forecast data available yet (P5MIN pipeline may still be ingesting)")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussion:** Look at the hourly breakdown above.
# MAGIC > AEMO's price forecast is typically weakest during:
# MAGIC > - **Evening ramp** (16:00-19:00) when solar drops and gas peakers start
# MAGIC > - **Overnight lows** (02:00-04:00) when negative pricing from wind occurs
# MAGIC > - **Weather transitions** when sudden cloud/wind changes shift renewable output
# MAGIC >
# MAGIC > These are the hours where your model can generate the most alpha.
# MAGIC > With interconnector binding signals and supply stack health metrics,
# MAGIC > you have an edge that P5MIN's deterministic dispatch engine doesn't capture.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8 Register model to Unity Catalog

# COMMAND ----------

model_name = f"{CATALOG}.{SCHEMA}.price_forecast_nsw"
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
            "NSW Price Forecasting Model\n\n"
            "**Target:** 5-minute spot price (RRP) for NSW region\n\n"
            "**Key features:** Supply margin, demand level, price lags, interconnector flows, weather\n\n"
            "**Training data:** NEMWEB DISPATCHREGIONSUM + DISPATCHPRICE + BOM Weather\n\n"
            "**Owner:** Market Modelling Team"
        ),
    )

    client.set_registered_model_alias(model_name, "champion", registered_model.version)
    model_load_uri = f"models:/{model_name}@champion"
    print(f"Model '{model_name}' version {registered_model.version} aliased as 'champion'")
except Exception as e:
    print(f"Model registry step skipped ({e}). Using run model URI for scoring: {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 UC registered models: inspect versions and champion alias
# MAGIC
# MAGIC Confirm model lineage and alias routing before any production deployment.

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
# MAGIC ### 2.10 Model serving for the champion model (MCP + SDK)
# MAGIC
# MAGIC For market modelling, real-time serving is useful for intraday repricing and
# MAGIC battery dispatch decisions. Keep endpoint deployment tied to the `@champion` alias.
# MAGIC
# MAGIC **MCP-first workflow (recommended for demos):**
# MAGIC ```python
# MAGIC # Check endpoint state
# MAGIC get_serving_endpoint_status(name="price-forecast-nsw")
# MAGIC
# MAGIC # Query when READY
# MAGIC query_serving_endpoint(
# MAGIC   name="price-forecast-nsw",
# MAGIC   dataframe_records=[{"hour": 18, "supply_margin_mw": 3500, "...": "..."}]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Databricks SDK workflow (create endpoint from notebook):**
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="price-forecast-nsw",
# MAGIC     config=EndpointCoreConfigInput(
# MAGIC         served_entities=[
# MAGIC             ServedEntityInput(
# MAGIC                 name="price-forecast-nsw-champion",
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
# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.11 UC functions for shared market logic
# MAGIC
# MAGIC UC functions are useful for governed market signals that should be consistent across
# MAGIC modelling notebooks, BI dashboards, and trader-facing SQL queries.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.price_spike_band(rrp DOUBLE)
RETURNS STRING
RETURN CASE
  WHEN rrp >= 1000 THEN 'critical_spike'
  WHEN rrp >= 300 THEN 'high_spike'
  WHEN rrp <= -100 THEN 'negative_price_event'
  ELSE 'normal'
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.market_stress_score(
  supply_margin_mw DOUBLE,
  net_interchange_mw DOUBLE,
  rrp DOUBLE
)
RETURNS DOUBLE
RETURN GREATEST(
  0.0,
  LEAST(
    100.0,
    (CASE WHEN supply_margin_mw <= 2000 THEN 50.0 ELSE 10.0 END) +
    (CASE WHEN abs(net_interchange_mw) >= 1000 THEN 20.0 ELSE 5.0 END) +
    (CASE WHEN rrp >= 300 THEN 30.0 WHEN rrp >= 150 THEN 15.0 ELSE 0.0 END)
  )
)
""")

print(f"Created UC functions in {CATALOG}.{SCHEMA}:")
print("  - price_spike_band(rrp)")
print("  - market_stress_score(supply_margin_mw, net_interchange_mw, rrp)")

# COMMAND ----------

# MAGIC %md
# MAGIC Example usage from SQL, dashboards, or notebooks:

# COMMAND ----------

silver_cols = set(spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather").columns)
if "supply_margin_mw" in silver_cols:
    margin_select_expr = "supply_margin_mw"
    margin_arg_expr = "supply_margin_mw"
else:
    margin_select_expr = "(available_generation_mw - total_demand_mw) AS supply_margin_mw"
    margin_arg_expr = "(available_generation_mw - total_demand_mw)"

if "net_interchange_mw" in silver_cols:
    interchange_select_expr = "net_interchange_mw"
    interchange_arg_expr = "net_interchange_mw"
else:
    interchange_select_expr = "CAST(0.0 AS DOUBLE) AS net_interchange_mw"
    interchange_arg_expr = "CAST(0.0 AS DOUBLE)"
    print("Column net_interchange_mw not found; using 0.0 placeholder in example query.")

display(
    spark.sql(f"""
    SELECT
      settlement_date,
      region_id,
      rrp,
      {margin_select_expr},
      {interchange_select_expr},
      {CATALOG}.{SCHEMA}.price_spike_band(rrp) AS price_spike_band,
      {CATALOG}.{SCHEMA}.market_stress_score(
        {margin_arg_expr},
        {interchange_arg_expr},
        rrp
      ) AS market_stress_score
    FROM {CATALOG}.{SCHEMA}.silver_demand_weather
    WHERE region_id = 'NSW1'
    ORDER BY settlement_date DESC
    LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.12 UC metric view for streaming market KPIs
# MAGIC
# MAGIC This creates a governed metric layer for intraday market monitoring so SQL users,
# MAGIC dashboards, and Genie all use the same market KPI definitions.
# MAGIC
# MAGIC > Requires Databricks Runtime 17.2+ or a compatible SQL warehouse.

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.price_ops_metric_view
    WITH METRICS
    LANGUAGE YAML
    AS $$
      version: 1.1
      comment: "Streaming market KPIs for price modelling operations"
      source: {CATALOG}.{SCHEMA}.silver_demand_weather
      dimensions:
        - name: event_hour
          expr: DATE_TRUNC('HOUR', settlement_date)
        - name: region_id
          expr: region_id
        - name: price_spike_band
          expr: {CATALOG}.{SCHEMA}.price_spike_band(rrp)
      measures:
        - name: interval_count
          expr: COUNT(1)
        - name: avg_rrp
          expr: AVG(rrp)
        - name: max_rrp
          expr: MAX(rrp)
        - name: avg_market_stress_score
          expr: AVG({CATALOG}.{SCHEMA}.market_stress_score((available_generation_mw - total_demand_mw), net_interchange_mw, rrp))
    $$
    """)
    print(f"Created metric view: {CATALOG}.{SCHEMA}.price_ops_metric_view")
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
          price_spike_band,
          measure(interval_count) AS interval_count,
          measure(avg_rrp) AS avg_rrp,
          measure(max_rrp) AS max_rrp,
          measure(avg_market_stress_score) AS avg_market_stress_score
        FROM {CATALOG}.{SCHEMA}.price_ops_metric_view
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
# MAGIC ---
# MAGIC ## Part 3: Predictions + Databricks App (10 min)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Score latest data with the champion model

# COMMAND ----------

champion_model = mlflow.pyfunc.load_model(model_load_uri)

predictions = champion_model.predict(X_test)

pred_df = pd.DataFrame({
    "settlement_date": X_test.index,
    "region_id": "NSW1",
    "actual_rrp": y_test.values,
    "predicted_rrp": predictions,
    "forecast_error_dollars": predictions - y_test.values,
    "model_version": registered_model_version,
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
target_table = f"{CATALOG}.{SCHEMA}.price_predictions_workshop"

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
# MAGIC This workshop writes to `price_predictions_workshop` for isolation. The app reads the
# MAGIC pipeline-managed `price_predictions` table and shows:
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
# MAGIC | **Interconnector flows** | See binding constraints and price separation in real time |
# MAGIC | **SCADA supply stack** | Generator trips visible before the price moves |
# MAGIC | **Price features** | Supply margin, demand, interconnectors, price lags |
# MAGIC | **MLflow tracking** | Every experiment tracked automatically |
# MAGIC | **Model registry** | Versioned models in Unity Catalog with `@champion` alias |
# MAGIC | **UC functions** | Shared market signals reused by SQL, dashboards, and jobs |
# MAGIC | **UC metric view** | Governed KPI layer on top of streaming market data |
# MAGIC | **AEMO P5MIN benchmark** | Compare your model against AEMO's official price forecast |
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
# MAGIC 2. The interconnector and SCADA data is live - how would you use it for intra-day trading?
# MAGIC 3. Where does AEMO's P5MIN price forecast break down? Can you target those hours for alpha?
# MAGIC 4. How would you integrate this with your existing Plexos workflow?
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
