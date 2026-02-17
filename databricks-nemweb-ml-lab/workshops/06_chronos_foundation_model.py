# Databricks notebook source
# MAGIC %md
# MAGIC # Appendix: Foundation Models for Energy Forecasting
# MAGIC
# MAGIC **Format:** Optional take-away notebook | **Duration:** 30 min self-paced
# MAGIC
# MAGIC **Audience:** ML engineers and data scientists who want to explore how pre-trained
# MAGIC time series foundation models compare to hand-crafted feature + XGBoost approaches.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Chronos?
# MAGIC
# MAGIC [Chronos](https://github.com/amazon-science/chronos-forecasting) is Amazon's family
# MAGIC of pre-trained time series forecasting models. Think of it as **"GPT for time series"**:
# MAGIC
# MAGIC | Model | Parameters | Speed | Key capability |
# MAGIC |-------|-----------|-------|----------------|
# MAGIC | **Chronos-Bolt** (Nov 2024) | 9M - 205M | Up to 250x faster | Zero-shot univariate, quantile forecasts |
# MAGIC | **Chronos-2** (Oct 2025) | 28M - 120M | Fast | Zero-shot **with covariates** (temperature, demand, etc.) |
# MAGIC | Chronos-T5 (original) | 8M - 710M | Slower | Zero-shot univariate, autoregressive sampling |
# MAGIC
# MAGIC ### Why this matters for energy forecasting
# MAGIC
# MAGIC - **Zero-shot**: No training required - just feed it your demand or price series
# MAGIC - **Probabilistic**: Produces quantile forecasts (P10, P50, P90) out of the box.
# MAGIC   Traders and battery operators care about uncertainty, not just point estimates
# MAGIC - **Covariates** (Chronos-2): Can incorporate temperature, demand, and other
# MAGIC   exogenous variables - exactly what energy forecasters need
# MAGIC - **No feature engineering**: Skip the 50 lines of lag/rolling/cyclical features.
# MAGIC   The model learns temporal patterns from its pre-training corpus
# MAGIC
# MAGIC ### The experiment
# MAGIC
# MAGIC We'll compare three approaches on the **same NEM data** from workshops 02 and 03:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  Approach 1: XGBoost + 30 hand-crafted features            │
# MAGIC │  (What you built in workshops 02/03)                       │
# MAGIC │  → Your domain knowledge baked into features               │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │  Approach 2: Chronos-Bolt zero-shot                        │
# MAGIC │  (No training, no features - just raw demand/price series) │
# MAGIC │  → How good is a pre-trained model out of the box?         │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │  Approach 3: Chronos-2 with covariates                     │
# MAGIC │  (Temperature + demand as exogenous features for price)    │
# MAGIC │  → Best of both: foundation model + domain signals         │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Install the `chronos-forecasting` package. This pulls in PyTorch and
# MAGIC HuggingFace Transformers under the hood.

# COMMAND ----------

# MAGIC %pip install chronos-forecasting --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Configuration
dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "ml_workshops", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Reading from: {CATALOG}.{SCHEMA}")

# COMMAND ----------

import pandas as pd
import numpy as np
import torch
import mlflow
from datetime import datetime, timedelta
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

# MLflow setup
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/chronos_foundation_model"
mlflow.set_experiment(experiment_name)

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {device}")
print(f"PyTorch: {torch.__version__}")
print(f"MLflow experiment: {experiment_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Load NEM data
# MAGIC
# MAGIC Same silver table used in workshops 02 and 03 - demand + price + weather
# MAGIC at 5-minute intervals.

# COMMAND ----------

df_nsw = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .drop("_processed_at")
    .orderBy("settlement_date")
)

pdf = df_nsw.toPandas()
pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
pdf = pdf.set_index("settlement_date").sort_index()

print(f"NSW data: {len(pdf):,} rows")
print(f"Date range: {pdf.index.min()} to {pdf.index.max()}")
print(f"Columns: {list(pdf.columns)}")

display(pdf[["total_demand_mw", "rrp", "air_temp_c"]].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train/test split
# MAGIC
# MAGIC Same temporal split as workshops 02/03: train on the first 80%, test on the last 20%.

# COMMAND ----------

split_idx = int(len(pdf) * 0.8)
pdf_train = pdf.iloc[:split_idx]
pdf_test = pdf.iloc[split_idx:]

# The forecast horizon is the length of the test set
prediction_length = len(pdf_test)

print(f"Training context: {len(pdf_train):,} rows ({pdf_train.index.min()} to {pdf_train.index.max()})")
print(f"Test (forecast):  {len(pdf_test):,} rows ({pdf_test.index.min()} to {pdf_test.index.max()})")
print(f"Prediction length: {prediction_length} intervals ({prediction_length * 5 / 60:.0f} hours)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Chronos-Bolt - zero-shot demand forecasting
# MAGIC
# MAGIC No training, no feature engineering. Just feed the model the raw demand
# MAGIC time series and ask it to forecast.
# MAGIC
# MAGIC We use `chronos-bolt-small` (48M params) - fast enough for CPU inference.

# COMMAND ----------

from chronos import BaseChronosPipeline

bolt_pipeline = BaseChronosPipeline.from_pretrained(
    "amazon/chronos-bolt-small",
    device_map=device,
    torch_dtype=torch.float32,
)

print("Chronos-Bolt-Small loaded (48M parameters)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Demand forecast (zero-shot)
# MAGIC
# MAGIC We pass the entire training history as context. Chronos-Bolt produces
# MAGIC quantile forecasts directly - no sampling needed.
# MAGIC
# MAGIC > **Note:** Chronos-Bolt has a maximum context length. For long series,
# MAGIC > it uses the most recent observations automatically.

# COMMAND ----------

# Prepare context: just the raw demand values as a tensor
demand_context = torch.tensor(pdf_train["total_demand_mw"].values, dtype=torch.float32)

# Cap prediction length for Chronos (max ~4096 steps for Bolt)
# For a demo, forecast the next 24 hours (288 x 5-min intervals)
demo_horizon = min(prediction_length, 288)

with mlflow.start_run(run_name="chronos_bolt_demand_zeroshot"):
    mlflow.log_params({
        "model_type": "chronos-bolt-small",
        "model_params": "48M",
        "target": "total_demand_mw",
        "region": "NSW1",
        "feature_count": 0,
        "approach": "zero-shot (no training, no features)",
        "context_length": len(demand_context),
        "prediction_length": demo_horizon,
    })

    # Generate quantile forecasts [batch, num_quantiles, prediction_length]
    quantiles, mean_forecast = bolt_pipeline.predict_quantiles(
        context=demand_context.unsqueeze(0),
        prediction_length=demo_horizon,
        quantile_levels=[0.1, 0.5, 0.9],
    )

    # Extract predictions
    p10 = quantiles[0, :, 0].numpy()
    p50 = quantiles[0, :, 1].numpy()  # median forecast
    p90 = quantiles[0, :, 2].numpy()

    # Compare against actuals
    y_actual = pdf_test["total_demand_mw"].values[:demo_horizon]
    y_pred = p50

    bolt_demand_metrics = {
        "mae_mw": mean_absolute_error(y_actual, y_pred),
        "rmse_mw": np.sqrt(mean_squared_error(y_actual, y_pred)),
        "mape_pct": mean_absolute_percentage_error(y_actual, y_pred) * 100,
    }
    mlflow.log_metrics(bolt_demand_metrics)

    bolt_demand_run_id = mlflow.active_run().info.run_id

print("Chronos-Bolt Demand Forecast (zero-shot, no features):")
print(f"  MAE:  {bolt_demand_metrics['mae_mw']:.1f} MW")
print(f"  RMSE: {bolt_demand_metrics['rmse_mw']:.1f} MW")
print(f"  MAPE: {bolt_demand_metrics['mape_pct']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Visualise: actual vs forecast with prediction intervals
# MAGIC
# MAGIC The shaded band shows the P10-P90 prediction interval - this is the
# MAGIC **probabilistic forecast** that Chronos gives you for free.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

fig, ax = plt.subplots(figsize=(14, 5))

forecast_index = pdf_test.index[:demo_horizon]

# Last 6 hours of context for visual continuity
context_tail = pdf_train["total_demand_mw"].iloc[-72:]
ax.plot(context_tail.index, context_tail.values, color="steelblue", label="Historical", linewidth=1)

# Actual
ax.plot(forecast_index, y_actual, color="black", label="Actual", linewidth=1.2)

# Chronos median forecast
ax.plot(forecast_index, p50, color="darkorange", label="Chronos-Bolt P50 (median)", linewidth=1.2)

# Prediction interval
ax.fill_between(forecast_index, p10, p90, alpha=0.25, color="darkorange", label="P10–P90 interval")

ax.set_ylabel("Demand (MW)")
ax.set_title("NSW Demand: Chronos-Bolt Zero-Shot Forecast vs Actual")
ax.legend(loc="upper right")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Price forecast (zero-shot)
# MAGIC
# MAGIC Price is harder than demand - it's spiky, mean-reverting, and has fat tails.
# MAGIC Let's see how Chronos handles it without any features.

# COMMAND ----------

price_context = torch.tensor(pdf_train["rrp"].values, dtype=torch.float32)

with mlflow.start_run(run_name="chronos_bolt_price_zeroshot"):
    mlflow.log_params({
        "model_type": "chronos-bolt-small",
        "model_params": "48M",
        "target": "rrp",
        "region": "NSW1",
        "feature_count": 0,
        "approach": "zero-shot (no training, no features)",
        "prediction_length": demo_horizon,
    })

    quantiles_price, _ = bolt_pipeline.predict_quantiles(
        context=price_context.unsqueeze(0),
        prediction_length=demo_horizon,
        quantile_levels=[0.1, 0.5, 0.9],
    )

    price_p10 = quantiles_price[0, :, 0].numpy()
    price_p50 = quantiles_price[0, :, 1].numpy()
    price_p90 = quantiles_price[0, :, 2].numpy()

    y_actual_price = pdf_test["rrp"].values[:demo_horizon]

    bolt_price_metrics = {
        "mae_dollars": mean_absolute_error(y_actual_price, price_p50),
        "rmse_dollars": np.sqrt(mean_squared_error(y_actual_price, price_p50)),
        "mape_pct": mean_absolute_percentage_error(y_actual_price, price_p50) * 100,
    }
    mlflow.log_metrics(bolt_price_metrics)

print("Chronos-Bolt Price Forecast (zero-shot, no features):")
print(f"  MAE:  ${bolt_price_metrics['mae_dollars']:.2f}/MWh")
print(f"  RMSE: ${bolt_price_metrics['rmse_dollars']:.2f}/MWh")
print(f"  MAPE: {bolt_price_metrics['mape_pct']:.2f}%")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 5))

forecast_index = pdf_test.index[:demo_horizon]
context_tail_price = pdf_train["rrp"].iloc[-72:]

ax.plot(context_tail_price.index, context_tail_price.values, color="steelblue", label="Historical", linewidth=1)
ax.plot(forecast_index, y_actual_price, color="black", label="Actual", linewidth=1.2)
ax.plot(forecast_index, price_p50, color="crimson", label="Chronos-Bolt P50", linewidth=1.2)
ax.fill_between(forecast_index, price_p10, price_p90, alpha=0.2, color="crimson", label="P10–P90 interval")

ax.set_ylabel("Price ($/MWh)")
ax.set_title("NSW Price: Chronos-Bolt Zero-Shot Forecast vs Actual")
ax.legend(loc="upper right")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Chronos-2 with covariates
# MAGIC
# MAGIC Chronos-2 (Oct 2025) supports **exogenous covariates** - you can pass
# MAGIC additional time series like temperature, demand, and interconnector flows
# MAGIC alongside the target. This gives the model domain-relevant context
# MAGIC without manual feature engineering.
# MAGIC
# MAGIC > This is the best of both worlds: a pre-trained foundation model that
# MAGIC > can leverage the same signals your hand-crafted features capture.

# COMMAND ----------

from chronos import Chronos2Pipeline

chronos2_pipeline = Chronos2Pipeline.from_pretrained(
    "amazon/chronos-2",
    device_map=device,
    torch_dtype=torch.float32,
)

print("Chronos-2 loaded (120M parameters)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Price forecast with temperature and demand as covariates
# MAGIC
# MAGIC Chronos-2 uses `predict_df` which takes a DataFrame with:
# MAGIC - A **target** column (what to forecast)
# MAGIC - Additional columns treated as **covariates**
# MAGIC - A **timestamp** column for temporal alignment

# COMMAND ----------

# Build the context DataFrame for Chronos-2
# Target: rrp (price) | Covariates: demand, temperature
context_df = pdf_train[["total_demand_mw", "rrp"]].copy()

# Forward-fill temperature (may have gaps from BOM)
if "air_temp_c" in pdf_train.columns:
    context_df["air_temp_c"] = pdf_train["air_temp_c"].ffill().bfill()

context_df = context_df.reset_index().rename(columns={"settlement_date": "timestamp"})
context_df["id"] = "NSW1"

# Build future covariates (known at forecast time in a real scenario,
# we use actuals here for the experiment)
future_df = pdf_test[["total_demand_mw"]].iloc[:demo_horizon].copy()
if "air_temp_c" in pdf_test.columns:
    future_df["air_temp_c"] = pdf_test["air_temp_c"].iloc[:demo_horizon].ffill().bfill()

future_df = future_df.reset_index().rename(columns={"settlement_date": "timestamp"})
future_df["id"] = "NSW1"

print(f"Context: {len(context_df):,} rows, columns: {list(context_df.columns)}")
print(f"Future covariates: {len(future_df):,} rows, columns: {list(future_df.columns)}")

# COMMAND ----------

with mlflow.start_run(run_name="chronos2_price_with_covariates"):
    covariate_cols = [c for c in context_df.columns if c not in ("timestamp", "id", "rrp")]
    mlflow.log_params({
        "model_type": "chronos-2",
        "model_params": "120M",
        "target": "rrp",
        "region": "NSW1",
        "approach": "zero-shot with covariates",
        "covariates": ", ".join(covariate_cols),
        "feature_count": len(covariate_cols),
        "prediction_length": demo_horizon,
    })

    pred_df = chronos2_pipeline.predict_df(
        context_df,
        future_df=future_df,
        prediction_length=demo_horizon,
        quantile_levels=[0.1, 0.5, 0.9],
        id_column="id",
        timestamp_column="timestamp",
        target="rrp",
    )

    # Extract predictions
    c2_p50 = pred_df["predictions"].values
    c2_p10 = pred_df["0.1"].values
    c2_p90 = pred_df["0.9"].values

    y_actual_price_c2 = pdf_test["rrp"].values[:demo_horizon]

    c2_price_metrics = {
        "mae_dollars": mean_absolute_error(y_actual_price_c2, c2_p50),
        "rmse_dollars": np.sqrt(mean_squared_error(y_actual_price_c2, c2_p50)),
        "mape_pct": mean_absolute_percentage_error(y_actual_price_c2, c2_p50) * 100,
    }
    mlflow.log_metrics(c2_price_metrics)

print("Chronos-2 Price Forecast (with covariates: demand + temperature):")
print(f"  MAE:  ${c2_price_metrics['mae_dollars']:.2f}/MWh")
print(f"  RMSE: ${c2_price_metrics['rmse_dollars']:.2f}/MWh")
print(f"  MAPE: {c2_price_metrics['mape_pct']:.2f}%")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 5))

forecast_index = pdf_test.index[:demo_horizon]
context_tail_price = pdf_train["rrp"].iloc[-72:]

ax.plot(context_tail_price.index, context_tail_price.values, color="steelblue", label="Historical", linewidth=1)
ax.plot(forecast_index, y_actual_price_c2, color="black", label="Actual", linewidth=1.2)
ax.plot(forecast_index, c2_p50, color="mediumseagreen", label="Chronos-2 P50 (with covariates)", linewidth=1.2)
ax.fill_between(forecast_index, c2_p10, c2_p90, alpha=0.2, color="mediumseagreen", label="P10–P90 interval")

ax.set_ylabel("Price ($/MWh)")
ax.set_title("NSW Price: Chronos-2 with Covariates (Demand + Temperature) vs Actual")
ax.legend(loc="upper right")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Head-to-head comparison
# MAGIC
# MAGIC Let's bring it all together. How does each approach stack up?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Demand forecasting comparison

# COMMAND ----------

print("=" * 70)
print("DEMAND FORECASTING COMPARISON (NSW, next 24h)")
print("=" * 70)
print()
print(f"{'Approach':<45} {'MAE (MW)':>10} {'MAPE (%)':>10}")
print("-" * 70)
print(f"{'Chronos-Bolt zero-shot (no features)':<45} {bolt_demand_metrics['mae_mw']:>10.1f} {bolt_demand_metrics['mape_pct']:>10.2f}")
print()
print("Compare with your XGBoost from Workshop 02:")
print("  → Open the MLflow experiment 'load_forecast_workshop' to see your results")
print("  → XGBoost uses ~30 hand-crafted features (lags, rolling stats, weather)")
print("  → Chronos uses zero features - just the raw demand series")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Price forecasting comparison

# COMMAND ----------

print("=" * 70)
print("PRICE FORECASTING COMPARISON (NSW, next 24h)")
print("=" * 70)
print()
print(f"{'Approach':<45} {'MAE ($/MWh)':>12} {'MAPE (%)':>10}")
print("-" * 70)
print(f"{'Chronos-Bolt zero-shot (no features)':<45} {bolt_price_metrics['mae_dollars']:>12.2f} {bolt_price_metrics['mape_pct']:>10.2f}")
print(f"{'Chronos-2 + covariates (demand, temp)':<45} {c2_price_metrics['mae_dollars']:>12.2f} {c2_price_metrics['mape_pct']:>10.2f}")
print()
print("Compare with your XGBoost from Workshop 03:")
print("  → Open the MLflow experiment 'market_modelling_workshop' to see your results")
print("  → XGBoost uses ~30 hand-crafted features (supply margin, price lags, weather)")
print("  → Chronos-Bolt uses zero features")
print("  → Chronos-2 uses 2 covariates (demand + temperature)")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Side-by-side forecast plot

# COMMAND ----------

fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

forecast_index = pdf_test.index[:demo_horizon]

# --- Demand ---
ax = axes[0]
ax.plot(forecast_index, pdf_test["total_demand_mw"].values[:demo_horizon],
        color="black", label="Actual", linewidth=1.2)
ax.plot(forecast_index, p50,
        color="darkorange", label="Chronos-Bolt (zero-shot)", linewidth=1)
ax.fill_between(forecast_index, p10, p90, alpha=0.15, color="darkorange")
ax.set_ylabel("Demand (MW)")
ax.set_title("NSW Demand Forecast Comparison")
ax.legend(loc="upper right")

# --- Price ---
ax = axes[1]
ax.plot(forecast_index, y_actual_price_c2,
        color="black", label="Actual", linewidth=1.2)
ax.plot(forecast_index, price_p50,
        color="crimson", label="Chronos-Bolt (zero-shot)", linewidth=1, alpha=0.7)
ax.fill_between(forecast_index, price_p10, price_p90, alpha=0.1, color="crimson")
ax.plot(forecast_index, c2_p50,
        color="mediumseagreen", label="Chronos-2 (+ covariates)", linewidth=1)
ax.fill_between(forecast_index, c2_p10, c2_p90, alpha=0.1, color="mediumseagreen")
ax.set_ylabel("Price ($/MWh)")
ax.set_title("NSW Price Forecast Comparison")
ax.legend(loc="upper right")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Log all results to MLflow for comparison
# MAGIC
# MAGIC Log a summary run so you can compare all approaches in the MLflow UI
# MAGIC alongside your workshop 02/03 results.

# COMMAND ----------

with mlflow.start_run(run_name="chronos_vs_xgboost_summary"):
    mlflow.log_params({
        "comparison_type": "foundation_model_vs_tabular",
        "region": "NSW1",
        "prediction_horizon_intervals": demo_horizon,
        "prediction_horizon_hours": demo_horizon * 5 / 60,
    })

    # Demand metrics
    mlflow.log_metrics({
        "bolt_demand_mae_mw": bolt_demand_metrics["mae_mw"],
        "bolt_demand_mape_pct": bolt_demand_metrics["mape_pct"],
    })

    # Price metrics
    mlflow.log_metrics({
        "bolt_price_mae_dollars": bolt_price_metrics["mae_dollars"],
        "bolt_price_mape_pct": bolt_price_metrics["mape_pct"],
        "chronos2_price_mae_dollars": c2_price_metrics["mae_dollars"],
        "chronos2_price_mape_pct": c2_price_metrics["mape_pct"],
    })

    # Log the comparison table
    comparison_data = pd.DataFrame([
        {"approach": "Chronos-Bolt (zero-shot)", "target": "demand", "mae": bolt_demand_metrics["mae_mw"], "mape_pct": bolt_demand_metrics["mape_pct"]},
        {"approach": "Chronos-Bolt (zero-shot)", "target": "price", "mae": bolt_price_metrics["mae_dollars"], "mape_pct": bolt_price_metrics["mape_pct"]},
        {"approach": "Chronos-2 (+ covariates)", "target": "price", "mae": c2_price_metrics["mae_dollars"], "mape_pct": c2_price_metrics["mape_pct"]},
    ])
    mlflow.log_table(comparison_data, "chronos_comparison.json")

print("Summary logged to MLflow - compare with workshop 02/03 experiments in the UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Discussion: When to use what
# MAGIC
# MAGIC | Approach | Strengths | Weaknesses | Best for |
# MAGIC |----------|-----------|------------|----------|
# MAGIC | **XGBoost + features** | Domain knowledge encoded, handles spikes, interpretable features | Manual feature engineering, training/serving skew risk | Production forecasts where you understand the drivers |
# MAGIC | **Chronos-Bolt** | Zero-shot, fast, probabilistic, no features needed | Univariate only, can't use weather/supply signals | Quick baselines, new time series, anomaly detection |
# MAGIC | **Chronos-2 + covariates** | Zero-shot with exogenous signals, probabilistic | Larger model, slower than Bolt, newer (less battle-tested) | When you have covariates but don't want to feature-engineer |
# MAGIC
# MAGIC ### Practical recommendations for energy teams
# MAGIC
# MAGIC 1. **Start with Chronos-Bolt as a baseline** for any new forecasting task.
# MAGIC    If a zero-shot model gets you within 10% of your current approach,
# MAGIC    that's a signal your features may not be adding much value.
# MAGIC
# MAGIC 2. **Use Chronos-2 with covariates** when you have known-future variables
# MAGIC    (e.g., weather forecasts, scheduled outages, holiday calendars).
# MAGIC    This avoids the feature engineering step while still leveraging domain data.
# MAGIC
# MAGIC 3. **Stick with XGBoost/LightGBM** when:
# MAGIC    - You need interpretable feature importance (regulatory/audit requirements)
# MAGIC    - Your hand-crafted features encode domain knowledge the model can't learn
# MAGIC      (e.g., interconnector binding constraints, generator bid patterns)
# MAGIC    - You need very fast inference (tree models are sub-millisecond)
# MAGIC
# MAGIC 4. **Ensemble** for production: use Chronos probabilistic forecasts as a feature
# MAGIC    in your XGBoost model - the prediction interval width is itself a useful signal.
# MAGIC
# MAGIC ### The real insight
# MAGIC
# MAGIC > Foundation models like Chronos are not here to replace your domain expertise.
# MAGIC > They're here to give you a **strong baseline for free** and to handle the
# MAGIC > temporal patterns so you can focus your feature engineering on the signals
# MAGIC > that truly require domain knowledge (bid stacks, constraint equations,
# MAGIC > outage schedules).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: Using Chronos predictions as features for XGBoost
# MAGIC
# MAGIC The ensemble approach - use Chronos P50 forecast and prediction interval
# MAGIC width as additional features in your XGBoost model.
# MAGIC
# MAGIC ```python
# MAGIC # Generate Chronos forecasts for the training period (rolling window)
# MAGIC # Then add them as features to XGBoost:
# MAGIC #
# MAGIC # pdf_features["chronos_p50"] = chronos_rolling_p50
# MAGIC # pdf_features["chronos_interval_width"] = chronos_p90 - chronos_p10
# MAGIC #
# MAGIC # This gives XGBoost the "model consensus" signal plus a volatility
# MAGIC # measure from the foundation model, while keeping your domain features.
# MAGIC ```
# MAGIC
# MAGIC This is left as an exercise - the key idea is that foundation models
# MAGIC and traditional ML are **complementary**, not competing.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resources
# MAGIC
# MAGIC - **Chronos GitHub**: [amazon-science/chronos-forecasting](https://github.com/amazon-science/chronos-forecasting)
# MAGIC - **Chronos-2 paper**: [arxiv.org/abs/2510.15821](https://arxiv.org/abs/2510.15821)
# MAGIC - **Chronos-Bolt blog**: [AWS ML Blog](https://aws.amazon.com/blogs/machine-learning/fast-and-accurate-zero-shot-forecasting-with-chronos-bolt-and-autogluon/)
# MAGIC - **HuggingFace models**: [amazon/chronos-bolt-small](https://huggingface.co/amazon/chronos-bolt-small), [amazon/chronos-2](https://huggingface.co/amazon/chronos-2)
# MAGIC - **MLflow docs**: [mlflow.org/docs/latest](https://mlflow.org/docs/latest/index.html)
