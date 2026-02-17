# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-on ML workshop
# MAGIC
# MAGIC **Duration:** 90 minutes | **Format:** Hands-on
# MAGIC
# MAGIC **Audience:** Technical teams who build and maintain forecasting systems, run Python scripts, manage data pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Section | Duration | Format |
# MAGIC |---------|----------|--------|
# MAGIC | **Pain points** | 10 min | Discussion |
# MAGIC | **Your code, unchanged** | 15 min | Hands-on |
# MAGIC | **Add experiment tracking** | 25 min | Hands-on |
# MAGIC | **Turn into scheduled pipeline** | 20 min | Hands-on |
# MAGIC | **What else is possible** | 10 min | Demo |
# MAGIC | **Q&A** | 10 min | Discussion |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Pain points (10 min)
# MAGIC
# MAGIC **For the team leads:**
# MAGIC
# MAGIC > "What's the most annoying part of your current workflow?"
# MAGIC
# MAGIC Common answers we hear:
# MAGIC - "We don't know which model version is in production"
# MAGIC - "When something fails overnight, we find out from users"
# MAGIC - "Comparing experiments means digging through old notebooks"
# MAGIC - "Data is scattered across multiple servers"
# MAGIC
# MAGIC **Write down 1-2 pain points here - we'll reference them throughout:**
# MAGIC
# MAGIC ```
# MAGIC Pain point 1: _______________________________________________
# MAGIC
# MAGIC Pain point 2: _______________________________________________
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Your code, unchanged (15 min)
# MAGIC
# MAGIC The point here: **your existing Python code just works**.
# MAGIC
# MAGIC No rewrites. No new frameworks to learn. Just run your script.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: A typical forecasting script
# MAGIC
# MAGIC This is the kind of script you might already have:

# COMMAND ----------

# Your typical Python script - nothing special here
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Load some data (we'll use sample data - you'd use your own)
# This could be from AEMO, your internal databases, wherever
np.random.seed(42)
n_samples = 1000

# Simulated demand data - in reality you'd load from your source
data = pd.DataFrame({
    'hour': np.tile(np.arange(24), n_samples // 24 + 1)[:n_samples],
    'day_of_week': np.random.randint(0, 7, n_samples),
    'temperature': np.random.normal(20, 5, n_samples),
    'demand_mw': np.random.normal(7000, 500, n_samples)
})

# Add some realistic patterns
data['demand_mw'] += data['hour'].apply(lambda h: 500 if 9 <= h <= 17 else 0)
data['demand_mw'] += data['temperature'].apply(lambda t: 100 * abs(t - 20))

print(f"Loaded {len(data)} rows")
data.head()

# COMMAND ----------

# Standard ML workflow - exactly what you'd do locally
X = data[['hour', 'day_of_week', 'temperature']]
y = data['demand_mw']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a model
model = GradientBoostingRegressor(n_estimators=100, max_depth=4, learning_rate=0.1)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"MAE: {mae:.2f} MW")
print(f"RMSE: {rmse:.2f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### That's it. Your script just runs.
# MAGIC
# MAGIC No changes needed. Standard Python, standard libraries.
# MAGIC
# MAGIC **But here's the problem:** Where did those results go? What hyperparameters did you use?
# MAGIC If you run this again next week with different settings, how do you compare?
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Add experiment tracking (25 min)
# MAGIC
# MAGIC **This is where the magic happens - and it's only 3 lines of code.**
# MAGIC
# MAGIC MLflow tracks:
# MAGIC - What parameters you used
# MAGIC - What metrics you got
# MAGIC - The actual model file
# MAGIC - When you ran it, who ran it

# COMMAND ----------

import mlflow

# Line 1: Set up where to log (one time setup)
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/my_forecasting_experiments")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your code with tracking - spot the difference

# COMMAND ----------

# Same code as before, but now tracked
import mlflow

# Define parameters (same as before, just named)
params = {
    "n_estimators": 100,
    "max_depth": 4,
    "learning_rate": 0.1
}

# Line 2: Start tracking this run
with mlflow.start_run(run_name="gradient_boosting_v1"):

    # Line 3: Log your parameters
    mlflow.log_params(params)

    # === Your existing code, unchanged ===
    model = GradientBoostingRegressor(**params)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    # =====================================

    # Log metrics (optional but useful)
    mlflow.log_metrics({"mae": mae, "rmse": rmse})

    # Save the model (optional but useful)
    mlflow.sklearn.log_model(model, "model")

    print(f"MAE: {mae:.2f} MW")
    print(f"RMSE: {rmse:.2f} MW")
    print(f"\n✓ Run logged to MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now let's try different parameters

# COMMAND ----------

# Try a different configuration
params_v2 = {
    "n_estimators": 200,
    "max_depth": 6,
    "learning_rate": 0.05
}

with mlflow.start_run(run_name="gradient_boosting_v2"):
    mlflow.log_params(params_v2)

    model = GradientBoostingRegressor(**params_v2)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    mlflow.log_metrics({"mae": mae, "rmse": rmse})
    mlflow.sklearn.log_model(model, "model")

    print(f"MAE: {mae:.2f} MW")
    print(f"RMSE: {rmse:.2f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try XGBoost (your code, different library)

# COMMAND ----------

import xgboost as xgb

params_xgb = {
    "n_estimators": 100,
    "max_depth": 4,
    "learning_rate": 0.1
}

with mlflow.start_run(run_name="xgboost_v1"):
    mlflow.log_params(params_xgb)
    mlflow.log_param("model_type", "xgboost")  # Add custom tag

    model = xgb.XGBRegressor(**params_xgb)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    mlflow.log_metrics({"mae": mae, "rmse": rmse})
    mlflow.sklearn.log_model(model, "model")

    print(f"MAE: {mae:.2f} MW")
    print(f"RMSE: {rmse:.2f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare all experiments
# MAGIC
# MAGIC Now the payoff - see all your runs in one place:

# COMMAND ----------

# Query your experiments programmatically
runs = mlflow.search_runs()
comparison = runs[['run_id', 'tags.mlflow.runName', 'params.n_estimators', 'params.max_depth', 'metrics.mae', 'metrics.rmse']].copy()
comparison.columns = ['run_id', 'name', 'n_estimators', 'max_depth', 'mae', 'rmse']
display(comparison.sort_values('mae'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 👆 Click "Experiments" in the left sidebar to see the UI
# MAGIC
# MAGIC You can:
# MAGIC - Compare runs visually
# MAGIC - See parameter vs metric charts
# MAGIC - Download models
# MAGIC - Share with teammates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hands-on exercise (5 min)
# MAGIC
# MAGIC **Try it yourself:**
# MAGIC 1. Change some parameters in one of the cells above
# MAGIC 2. Give it a new `run_name`
# MAGIC 3. Run it
# MAGIC 4. Check the Experiments UI - your new run should appear
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Turn into a scheduled pipeline (20 min)
# MAGIC
# MAGIC Now your script is tracked. But it still requires you to manually run it.
# MAGIC
# MAGIC What if you want:
# MAGIC - Daily retraining with new data
# MAGIC - Automatic alerting if it fails
# MAGIC - Email notification when done

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Schedule this notebook directly
# MAGIC
# MAGIC **Simplest approach - use the UI:**
# MAGIC
# MAGIC 1. Click **Schedule** button (top right)
# MAGIC 2. Set frequency (daily, hourly, etc.)
# MAGIC 3. Add email notifications
# MAGIC 4. Done
# MAGIC
# MAGIC That's it. Your notebook runs on schedule, logs to MLflow, and you get notified.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Workflow with dependencies
# MAGIC
# MAGIC For more complex pipelines (data refresh → train → evaluate → alert):
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml - defines your workflow
# MAGIC jobs:
# MAGIC   daily_forecast:
# MAGIC     tasks:
# MAGIC       - task_key: refresh_data
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./01_refresh_data.py
# MAGIC
# MAGIC       - task_key: train_model
# MAGIC         depends_on:
# MAGIC           - task_key: refresh_data
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./02_train_model.py
# MAGIC
# MAGIC       - task_key: evaluate
# MAGIC         depends_on:
# MAGIC           - task_key: train_model
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./03_evaluate.py
# MAGIC
# MAGIC     schedule:
# MAGIC       quartz_cron_expression: "0 0 6 * * ?"  # 6am daily
# MAGIC
# MAGIC     email_notifications:
# MAGIC       on_failure:
# MAGIC         - your-team@company.com
# MAGIC ```
# MAGIC
# MAGIC Deploy with: `databricks bundle deploy`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's create a simple scheduled job via UI
# MAGIC
# MAGIC **Live demo:**
# MAGIC 1. Go to **Workflows** in the sidebar
# MAGIC 2. Click **Create Job**
# MAGIC 3. Add this notebook as a task
# MAGIC 4. Set a schedule
# MAGIC 5. Add notification

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you get for free
# MAGIC
# MAGIC | Feature | Your cron jobs | Databricks Workflows |
# MAGIC |---------|---------------|---------------------|
# MAGIC | Scheduling | ✓ (manual setup) | ✓ (built-in) |
# MAGIC | Retries on failure | Manual | Automatic |
# MAGIC | Dependency management | Scripts/Airflow | Built-in |
# MAGIC | Notifications | Manual setup | Built-in |
# MAGIC | Run history | Logs (maybe) | Full history with UI |
# MAGIC | Compute management | Always running | Serverless (pay per use) |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: What else is possible (10 min demo)
# MAGIC
# MAGIC We won't do these hands-on today, but here's what's available when you need it:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Registry - "which model is in production?"
# MAGIC
# MAGIC Register your best model:

# COMMAND ----------

# Register the best model to Unity Catalog
CATALOG = "workspace"
SCHEMA = "ml_workshops"

# Get the best run from our experiments
best_run = mlflow.search_runs().sort_values('metrics.mae').iloc[0]
best_run_id = best_run['run_id']

# Register it (one line)
model_name = f"{CATALOG}.{SCHEMA}.demand_forecast_model"
try:
    mlflow.set_registry_uri("databricks-uc")
    registered = mlflow.register_model(f"runs:/{best_run_id}/model", model_name)
    print(f"✓ Model registered: {model_name} (version {registered.version})")
except Exception as e:
    print(f"(Skipped - requires Unity Catalog setup: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Serving - deploy as API endpoint
# MAGIC
# MAGIC ```python
# MAGIC # Deploy model as REST endpoint (one API call)
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="demand-forecast",
# MAGIC     config={
# MAGIC         "served_models": [{
# MAGIC             "model_name": "workspace.ml_workshops.demand_forecast_model",
# MAGIC             "model_version": "1",
# MAGIC             "workload_size": "Small"
# MAGIC         }]
# MAGIC     }
# MAGIC )
# MAGIC
# MAGIC # Then call it from anywhere:
# MAGIC # POST https://your-workspace.databricks.com/serving-endpoints/demand-forecast/invocations
# MAGIC # {"dataframe_records": [{"hour": 14, "day_of_week": 2, "temperature": 25}]}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lakehouse Monitoring - automatic drift detection
# MAGIC
# MAGIC ```python
# MAGIC # Monitor your inference table for drift
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.quality_monitors.create(
# MAGIC     table_name="workspace.ml_workshops.predictions",
# MAGIC     assets_dir="/monitoring/demand_forecast",
# MAGIC     schedule={"quartz_cron_expression": "0 0 * * * ?"},  # hourly
# MAGIC     baseline_table_name="workspace.ml_workshops.training_data"
# MAGIC )
# MAGIC
# MAGIC # Alerts you when:
# MAGIC # - Input data distribution changes
# MAGIC # - Prediction distribution shifts
# MAGIC # - Data quality issues appear
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog - one place for everything
# MAGIC
# MAGIC ```
# MAGIC workspace (catalog)
# MAGIC └── ml_workshops (schema)
# MAGIC     ├── Tables
# MAGIC     │   ├── demand_data
# MAGIC     │   ├── weather_features
# MAGIC     │   └── predictions
# MAGIC     ├── Models
# MAGIC     │   └── demand_forecast_model (v1, v2, v3...)
# MAGIC     └── Volumes
# MAGIC         └── raw_files/
# MAGIC ```
# MAGIC
# MAGIC - Access control: who can read/write what
# MAGIC - Lineage: which data trained which model
# MAGIC - Discovery: search across all assets
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | What we covered | Time | Takeaway |
# MAGIC |-----------------|------|----------|
# MAGIC | Your code runs unchanged | 15 min | No rewrites needed |
# MAGIC | MLflow tracking | 25 min | 3 lines to track experiments |
# MAGIC | Scheduled pipelines | 20 min | Click to schedule, auto-alerting |
# MAGIC | Advanced features | 10 min | Model registry, serving, monitoring available when needed |
# MAGIC
# MAGIC ### Revisiting your pain points
# MAGIC
# MAGIC | Pain point | What we showed |
# MAGIC |------------|----------------|
# MAGIC | "Which model is in prod?" | Model Registry with versioning |
# MAGIC | "Silent failures" | Workflow notifications |
# MAGIC | "Comparing experiments" | MLflow UI |
# MAGIC | "Scattered data" | Unity Catalog |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A (10 min)
# MAGIC
# MAGIC **Discussion questions:**
# MAGIC
# MAGIC 1. Which part would be most useful for your current workflow?
# MAGIC
# MAGIC 2. What would you want to try first?
# MAGIC
# MAGIC 3. What questions do you have about migrating existing pipelines?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC - **Try it:** Run one of your own scripts in a notebook
# MAGIC - **Add tracking:** Wrap it with `mlflow.start_run()`
# MAGIC - **Schedule it:** Click Schedule on your notebook
# MAGIC
# MAGIC **Resources:**
# MAGIC - MLflow docs: https://mlflow.org/docs/latest/index.html
# MAGIC - Databricks Workflows: Search "Databricks Workflows" in docs
# MAGIC - This notebook: Available in the workshop repository
