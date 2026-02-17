# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop 2: ML workflows and MLflow
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this workshop, you will understand:
# MAGIC 1. How MLflow tracks experiments, parameters, and metrics
# MAGIC 2. How to compare different model approaches systematically
# MAGIC 3. How Unity Catalog Model Registry provides governance and lineage
# MAGIC
# MAGIC ## Agenda
# MAGIC | Section | Duration | Content |
# MAGIC |---------|----------|---------|
# MAGIC | ML landscape | 10 min | Where does ML fit in Databricks? |
# MAGIC | Experiment tracking | 15 min | MLflow experiments, runs, parameters, metrics |
# MAGIC | Model registry | 15 min | Versioning, staging, production promotion |
# MAGIC | Live demo | 15 min | Train a load forecasting model end-to-end |
# MAGIC | Q&A | 5 min | Discussion |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your current ML workflow (sound familiar?)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  Jupyter        │     │  Shared Drive   │     │  "Which model   │
# MAGIC │  Notebook v3    │────▶│  model_final_   │────▶│   is in prod?"  │
# MAGIC │  (or was it v5?)│     │  FINAL_v2.pkl   │     │                 │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC        │                       │
# MAGIC        │  "What hyperparams    │  "Who trained this?"
# MAGIC        │   did I use?"         │  "When?"
# MAGIC        ▼                       ▼
# MAGIC    ¯\_(ツ)_/¯              ¯\_(ツ)_/¯
# MAGIC ```
# MAGIC
# MAGIC **Pain points:**
# MAGIC - No record of what hyperparameters produced which results
# MAGIC - Model files scattered across shared drives
# MAGIC - Unclear which version is "production"
# MAGIC - No lineage to training data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: ML landscape in Databricks (10 min)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                      Unity Catalog (Governance)                     │
# MAGIC │                                                                     │
# MAGIC │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐               │
# MAGIC │  │   Tables    │   │   Models    │   │  Volumes    │               │
# MAGIC │  │   (Data)    │   │ (ML Models) │   │  (Files)    │               │
# MAGIC │  └─────────────┘   └─────────────┘   └─────────────┘               │
# MAGIC │         │                │                   │                      │
# MAGIC │         │         ┌──────┴───────┐          │                      │
# MAGIC │         │         │   MLflow     │          │                      │
# MAGIC │         │         │  - Tracking  │          │                      │
# MAGIC │         │         │  - Registry  │          │                      │
# MAGIC │         │         │  - Serving   │          │                      │
# MAGIC │         │         └──────────────┘          │                      │
# MAGIC │         │                │                   │                      │
# MAGIC │         └────────────────┼───────────────────┘                      │
# MAGIC │                          │                                          │
# MAGIC │              Automatic Lineage Tracking                             │
# MAGIC │         (Which data trained which model?)                           │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key components
# MAGIC
# MAGIC | Component | Purpose | Your benefit |
# MAGIC |-----------|---------|--------------|
# MAGIC | **MLflow Tracking** | Log parameters, metrics, artifacts | Know exactly what produced each result |
# MAGIC | **Model Registry** | Version and stage models | Clear path from dev → staging → prod |
# MAGIC | **Feature Store** | Centralized features | Prevent training-serving skew |
# MAGIC | **Model Serving** | Deploy endpoints | Real-time predictions without infra work |
# MAGIC
# MAGIC ### MLflow is open source
# MAGIC
# MAGIC - No vendor lock-in
# MAGIC - Export models to any platform
# MAGIC - Large community and ecosystem
# MAGIC - Integrates with scikit-learn, XGBoost, PyTorch, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Experiment tracking demo (15 min)
# MAGIC
# MAGIC Let's train a **load forecasting model** using the NEMWEB data from Workshop 1.

# COMMAND ----------

# Setup
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure MLflow to use Unity Catalog
CATALOG = "workspace"
SCHEMA = "ml_workshops"

mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/load_forecasting_workshop"
mlflow.set_experiment(experiment_name)

print(f"MLflow experiment: {experiment_name}")
print(f"Model registry: Unity Catalog ({CATALOG}.{SCHEMA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.1: Prepare training data
# MAGIC
# MAGIC We'll use regional demand data and add time-based features (similar to what you'd add from BOM weather data).

# COMMAND ----------

# Load demand data (created by 00_workshop_data_setup.py)
TABLE_NAME = f"{CATALOG}.{SCHEMA}.nemweb_dispatch_regionsum"
df_demand = spark.table(TABLE_NAME)
print(f"Loading from: {TABLE_NAME}")

# Focus on NSW for this demo
df_nsw = (
    df_demand
    .filter("REGIONID = 'NSW1'")
    .select("SETTLEMENTDATE", "TOTALDEMAND")
    .orderBy("SETTLEMENTDATE")
)

# Convert to pandas for ML
pdf = df_nsw.toPandas()
pdf['SETTLEMENTDATE'] = pd.to_datetime(pdf['SETTLEMENTDATE'])
pdf = pdf.set_index('SETTLEMENTDATE')

print(f"Training data: {len(pdf):,} rows")
print(f"Date range: {pdf.index.min()} to {pdf.index.max()}")

display(pdf.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.2: Feature engineering
# MAGIC
# MAGIC Add time-based features that capture demand patterns:

# COMMAND ----------

def create_features(df):
    """Create time-based features for load forecasting."""
    df = df.copy()

    # Time features
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    df['is_weekend'] = df.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding (better for ML models)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # Lag features (previous demand values)
    for lag in [1, 2, 3, 6, 12, 24, 48]:  # 5-min intervals
        df[f'lag_{lag}'] = df['TOTALDEMAND'].shift(lag)

    # Rolling statistics
    df['rolling_mean_12'] = df['TOTALDEMAND'].rolling(12).mean()  # 1 hour
    df['rolling_std_12'] = df['TOTALDEMAND'].rolling(12).std()
    df['rolling_mean_288'] = df['TOTALDEMAND'].rolling(288).mean()  # 1 day

    return df.dropna()

# Create features
pdf_features = create_features(pdf)
print(f"Features created: {list(pdf_features.columns)}")
print(f"Rows after feature engineering: {len(pdf_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.3: Train with MLflow tracking
# MAGIC
# MAGIC This is where the magic happens - every experiment is automatically logged:

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import xgboost as xgb
import lightgbm as lgb

# Prepare data
feature_cols = [c for c in pdf_features.columns if c != 'TOTALDEMAND']
X = pdf_features[feature_cols]
y = pdf_features['TOTALDEMAND']

# Time-based split (important for time series!)
split_idx = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Training set: {len(X_train):,} rows")
print(f"Test set: {len(X_test):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model 1: XGBoost
# MAGIC
# MAGIC Notice how we log everything - parameters, metrics, and the model itself:

# COMMAND ----------

with mlflow.start_run(run_name="xgboost_baseline"):
    # Log parameters
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8
    }
    mlflow.log_params(params)

    # Train model
    model = xgb.XGBRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    # Predict and evaluate
    y_pred = model.predict(X_test)

    # Log metrics
    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100
    }
    mlflow.log_metrics(metrics)

    # Log model
    mlflow.sklearn.log_model(model, "model")

    # Log feature importance
    importance_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)

    mlflow.log_table(importance_df, "feature_importance.json")

    print(f"XGBoost Results:")
    print(f"  MAE: {metrics['mae']:.2f} MW")
    print(f"  RMSE: {metrics['rmse']:.2f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model 2: LightGBM

# COMMAND ----------

with mlflow.start_run(run_name="lightgbm_baseline"):
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "verbose": -1
    }
    mlflow.log_params(params)

    model = lgb.LGBMRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100
    }
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    print(f"LightGBM Results:")
    print(f"  MAE: {metrics['mae']:.2f} MW")
    print(f"  RMSE: {metrics['rmse']:.2f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model 3: Gradient Boosting (scikit-learn)

# COMMAND ----------

with mlflow.start_run(run_name="gradient_boosting_baseline"):
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8
    }
    mlflow.log_params(params)

    model = GradientBoostingRegressor(**params, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    metrics = {
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "mape": mean_absolute_percentage_error(y_test, y_pred) * 100
    }
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    print(f"Gradient Boosting Results:")
    print(f"  MAE: {metrics['mae']:.2f} MW")
    print(f"  RMSE: {metrics['rmse']:.2f} MW")
    print(f"  MAPE: {metrics['mape']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Compare experiments
# MAGIC
# MAGIC Now let's see the power of tracking - we can compare all our experiments:

# COMMAND ----------

# Query all runs in our experiment
runs_df = mlflow.search_runs(experiment_names=[experiment_name])

# Show comparison
comparison = runs_df[['run_id', 'tags.mlflow.runName', 'metrics.mae', 'metrics.rmse', 'metrics.mape', 'start_time']].copy()
comparison.columns = ['run_id', 'model', 'mae', 'rmse', 'mape', 'timestamp']
comparison = comparison.sort_values('mae')

display(comparison)

print("\nBest model by MAE:", comparison.iloc[0]['model'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### View in MLflow UI
# MAGIC
# MAGIC Click the **Experiments** icon in the left sidebar to see:
# MAGIC - All runs with parameters and metrics
# MAGIC - Charts comparing model performance
# MAGIC - Artifacts (model files, feature importance)
# MAGIC - Full reproducibility information

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Model Registry (15 min)
# MAGIC
# MAGIC Now let's register our best model to Unity Catalog for governance and deployment.

# COMMAND ----------

# Get the best run
best_run = comparison.iloc[0]
best_run_id = best_run['run_id']
best_model_name = best_run['model']

print(f"Best model: {best_model_name}")
print(f"Run ID: {best_run_id}")
print(f"MAE: {best_run['mae']:.2f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.1: Register model to Unity Catalog

# COMMAND ----------

# Register the best model
model_name = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"

# Register from the best run
model_uri = f"runs:/{best_run_id}/model"
registered_model = mlflow.register_model(model_uri, model_name)

print(f"Model registered: {model_name}")
print(f"Version: {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.2: Add model description and tags

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Update model description
client.update_registered_model(
    name=model_name,
    description="""
    NSW Load Forecasting Model

    **Purpose:** Predict 5-minute ahead electricity demand for NSW region

    **Features:** Time-based features (hour, day of week, month) + lag features

    **Training data:** NEMWEB DISPATCHREGIONSUM

    **Owner:** Load Forecasting Team
    """
)

# Add alias for production
client.set_registered_model_alias(model_name, "champion", registered_model.version)

print(f"Model '{model_name}' updated with description and 'champion' alias")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.3: View lineage
# MAGIC
# MAGIC Unity Catalog automatically tracks:
# MAGIC - Which data was used to train this model
# MAGIC - Which notebook/job created the model
# MAGIC - Model version history

# COMMAND ----------

# Show model versions
versions = client.search_model_versions(f"name='{model_name}'")
for v in versions:
    print(f"Version {v.version}:")
    print(f"  Status: {v.status}")
    print(f"  Created: {v.creation_timestamp}")
    print(f"  Run ID: {v.run_id}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Before and after
# MAGIC
# MAGIC | Aspect | Before (Notebooks + shared drives) | After (MLflow + Unity Catalog) |
# MAGIC |--------|-----------------------------------|-------------------------------|
# MAGIC | **Experiment tracking** | Screenshots, notes in docs | Automatic logging of all params/metrics |
# MAGIC | **Model comparison** | Manual spreadsheets | Query and compare programmatically |
# MAGIC | **Model storage** | `model_final_FINAL_v2.pkl` | Versioned registry with lineage |
# MAGIC | **Production model** | "Ask Dave, he knows" | Clear alias: `@champion` |
# MAGIC | **Reproducibility** | "What hyperparams did I use?" | Full run history with code |
# MAGIC | **Governance** | None | Unity Catalog access control |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Q&A and discussion
# MAGIC
# MAGIC **Questions to discuss:**
# MAGIC 1. What models do you currently train? (Prophet, ARIMA, neural nets?)
# MAGIC 2. How do you currently compare model versions?
# MAGIC 3. What features do you use beyond time-based? (weather, events, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next workshop: Model serving and monitoring
# MAGIC
# MAGIC In the next session, we'll:
# MAGIC - Deploy our load forecast model as a real-time endpoint
# MAGIC - Set up inference tables to capture predictions
# MAGIC - Configure Lakehouse Monitoring for drift detection
# MAGIC - Create alerts when model performance degrades
# MAGIC
# MAGIC **Homework (optional):** Think about how you currently monitor model performance in production.
