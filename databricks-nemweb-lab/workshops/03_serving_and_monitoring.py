# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop 3: Model serving and monitoring
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this workshop, you will understand:
# MAGIC 1. How to deploy models as real-time endpoints or batch jobs
# MAGIC 2. How inference tables automatically capture predictions for monitoring
# MAGIC 3. How Lakehouse Monitoring detects drift and data quality issues
# MAGIC 4. How to set up alerts when model performance degrades
# MAGIC
# MAGIC ## Agenda
# MAGIC | Section | Duration | Content |
# MAGIC |---------|----------|---------|
# MAGIC | Deployment options | 10 min | Batch vs real-time, when to use each |
# MAGIC | Model Serving | 15 min | Deploy endpoint, scaling, A/B testing |
# MAGIC | Inference tables | 15 min | Automatic logging of inputs/outputs |
# MAGIC | Lakehouse Monitoring | 15 min | Drift detection, alerting |
# MAGIC | Q&A | 5 min | Discussion |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your current production ML (sound familiar?)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
# MAGIC │   Cron Job      │     │   Predictions   │     │   Stakeholder   │
# MAGIC │   (runs daily)  │────▶│   in CSV/DB     │────▶│   "Model seems  │
# MAGIC │                 │     │                 │     │    off lately"  │
# MAGIC └─────────────────┘     └─────────────────┘     └─────────────────┘
# MAGIC        │                       │
# MAGIC        │  Silent failures      │  No drift detection
# MAGIC        │  No observability     │  Discovered by users
# MAGIC        ▼                       ▼
# MAGIC    ¯\_(ツ)_/¯              ¯\_(ツ)_/¯
# MAGIC ```
# MAGIC
# MAGIC **Pain points:**
# MAGIC - No visibility into prediction quality over time
# MAGIC - Drift discovered when stakeholders complain
# MAGIC - No automated alerting
# MAGIC - Manual investigation when things go wrong

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Deployment options (10 min)
# MAGIC
# MAGIC ### When to use each approach
# MAGIC
# MAGIC | Approach | Latency | Use case | Example |
# MAGIC |----------|---------|----------|---------|
# MAGIC | **Batch inference** | Minutes-hours | Scheduled predictions | Daily load forecast for next 24h |
# MAGIC | **Real-time endpoint** | Milliseconds | On-demand predictions | "What's the demand in 5 minutes?" |
# MAGIC | **Streaming** | Seconds | Continuous predictions | Update forecast every 5 min |
# MAGIC
# MAGIC ### For load forecasting teams
# MAGIC
# MAGIC Most load forecasting is **batch** - you generate forecasts for the next day/week ahead of time.
# MAGIC
# MAGIC But **real-time** is useful for:
# MAGIC - Responding to sudden weather changes
# MAGIC - Intraday forecast updates
# MAGIC - Integration with trading systems

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Model Serving demo (15 min)
# MAGIC
# MAGIC Let's deploy our load forecast model from Workshop 2 as a real-time endpoint.

# COMMAND ----------

# Setup
import mlflow
from mlflow import MlflowClient
import pandas as pd
import numpy as np
import requests
import json

CATALOG = "workspace"
SCHEMA = "ml_workshops"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"

client = MlflowClient()
mlflow.set_registry_uri("databricks-uc")

# Get the champion model
champion_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
print(f"Champion model: {MODEL_NAME} version {champion_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.1: Create a Model Serving endpoint
# MAGIC
# MAGIC This can be done via UI or API. Here's the API approach:

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

endpoint_name = "load-forecast-nsw-demo"

# Check if endpoint already exists
try:
    existing = w.serving_endpoints.get(endpoint_name)
    print(f"Endpoint '{endpoint_name}' already exists")
    endpoint_exists = True
except Exception:
    endpoint_exists = False
    print(f"Creating endpoint '{endpoint_name}'...")

# COMMAND ----------

# Create or update the endpoint
if not endpoint_exists:
    endpoint = w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=champion_version.version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,  # Cost optimization
                )
            ]
        )
    )
    print(f"Endpoint created: {endpoint.name}")
    print(f"State: {endpoint.state}")
else:
    # Update to latest champion version if needed
    endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"Endpoint ready: {endpoint.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.2: Query the endpoint
# MAGIC
# MAGIC Now we can get predictions in real-time:

# COMMAND ----------

# Prepare sample input (same features as training)
sample_input = {
    "dataframe_records": [
        {
            "hour": 14,
            "day_of_week": 2,
            "month": 7,
            "is_weekend": 0,
            "hour_sin": np.sin(2 * np.pi * 14 / 24),
            "hour_cos": np.cos(2 * np.pi * 14 / 24),
            "dow_sin": np.sin(2 * np.pi * 2 / 7),
            "dow_cos": np.cos(2 * np.pi * 2 / 7),
            "lag_1": 8500,
            "lag_2": 8450,
            "lag_3": 8400,
            "lag_6": 8200,
            "lag_12": 7800,
            "lag_24": 7500,
            "lag_48": 8100,
            "rolling_mean_12": 8300,
            "rolling_std_12": 150,
            "rolling_mean_288": 8000
        }
    ]
}

# Query the endpoint
response = w.serving_endpoints.query(
    name=endpoint_name,
    dataframe_records=sample_input["dataframe_records"]
)

print(f"Predicted demand: {response.predictions[0]:.2f} MW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.3: Endpoint monitoring
# MAGIC
# MAGIC The endpoint automatically tracks:
# MAGIC - Request latency
# MAGIC - Error rates
# MAGIC - Request volume
# MAGIC - Model version

# COMMAND ----------

# Get endpoint metrics
endpoint_info = w.serving_endpoints.get(endpoint_name)
print(f"Endpoint: {endpoint_info.name}")
print(f"State: {endpoint_info.state}")
print(f"Config:")
for entity in endpoint_info.config.served_entities:
    print(f"  Model: {entity.entity_name}")
    print(f"  Version: {entity.entity_version}")
    print(f"  Scale to zero: {entity.scale_to_zero_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Inference tables (15 min)
# MAGIC
# MAGIC The real power comes from **automatic logging** of all predictions.
# MAGIC
# MAGIC ### What gets logged automatically
# MAGIC
# MAGIC | Field | Description |
# MAGIC |-------|-------------|
# MAGIC | `request_id` | Unique identifier for each request |
# MAGIC | `timestamp` | When the prediction was made |
# MAGIC | `input` | All input features |
# MAGIC | `output` | Model prediction |
# MAGIC | `model_version` | Which model version made the prediction |
# MAGIC | `latency_ms` | How long the prediction took |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.1: Enable inference tables

# COMMAND ----------

# Inference tables are automatically enabled for endpoints
# They're stored in the Unity Catalog catalog associated with the endpoint

inference_table = f"{CATALOG}.{SCHEMA}.load_forecast_nsw_demo_inference_logs"

print(f"Inference logs table: {inference_table}")
print("\nThis table is automatically populated with every prediction request.")
print("You can query it to:")
print("  - Analyze prediction patterns")
print("  - Join with actual values for accuracy monitoring")
print("  - Detect anomalies in input features")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.2: Query inference logs
# MAGIC
# MAGIC Let's see what's being captured:

# COMMAND ----------

# Query inference logs (if available)
try:
    inference_df = spark.table(inference_table)
    print(f"Inference logs: {inference_df.count()} records")

    display(
        inference_df
        .select("request_id", "timestamp_ms", "status_code", "execution_time_ms")
        .orderBy("timestamp_ms", ascending=False)
        .limit(10)
    )
except Exception as e:
    print("Inference table not yet populated (endpoint just created)")
    print("After a few predictions, you'll see data here.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Lakehouse Monitoring (15 min)
# MAGIC
# MAGIC Now let's set up monitoring to detect when the model starts to degrade.
# MAGIC
# MAGIC ### What Lakehouse Monitoring tracks
# MAGIC
# MAGIC | Metric | Description | Why it matters |
# MAGIC |--------|-------------|----------------|
# MAGIC | **Data drift** | Input features changing distribution | Model may be seeing data it wasn't trained on |
# MAGIC | **Prediction drift** | Output distribution changing | Model behavior is changing |
# MAGIC | **Model quality** | Accuracy metrics over time | Performance degradation |
# MAGIC | **Data quality** | Nulls, schema changes | Pipeline issues |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.1: Create a monitoring table
# MAGIC
# MAGIC For monitoring, we need a table that joins predictions with actuals:

# COMMAND ----------

# Create a sample monitoring table
# In production, this would be populated by your inference pipeline

from pyspark.sql.functions import col, lit, current_timestamp, rand

# Sample data showing predictions vs actuals
monitoring_data = [
    ("2024-01-15 14:00:00", 8500.0, 8450.0, 50.0, 0.59),
    ("2024-01-15 14:05:00", 8520.0, 8480.0, 40.0, 0.47),
    ("2024-01-15 14:10:00", 8540.0, 8600.0, -60.0, 0.70),
    ("2024-01-15 14:15:00", 8560.0, 8550.0, 10.0, 0.12),
    ("2024-01-15 14:20:00", 8580.0, 8620.0, -40.0, 0.46),
    # Add some drift scenario
    ("2024-01-16 14:00:00", 8500.0, 9100.0, -600.0, 6.59),  # Actual way higher
    ("2024-01-16 14:05:00", 8520.0, 9150.0, -630.0, 6.89),
    ("2024-01-16 14:10:00", 8540.0, 9200.0, -660.0, 7.17),
]

monitoring_df = spark.createDataFrame(
    monitoring_data,
    ["timestamp", "predicted_demand", "actual_demand", "error", "mape_pct"]
)

# Save as table
monitoring_table = f"{CATALOG}.{SCHEMA}.load_forecast_monitoring"
monitoring_df.write.mode("overwrite").saveAsTable(monitoring_table)

print(f"Monitoring table created: {monitoring_table}")
display(monitoring_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.2: Create a Lakehouse Monitor
# MAGIC
# MAGIC This sets up automated drift detection:

# COMMAND ----------

from databricks.sdk.service.catalog import MonitorTimeSeries, MonitorTimeSeriesProfileType

# Create monitor configuration
monitor_name = f"{monitoring_table}_monitor"

try:
    # Create the monitor
    monitor = w.quality_monitors.create(
        table_name=monitoring_table,
        time_series=MonitorTimeSeries(
            timestamp_col="timestamp",
            granularities=["1 hour", "1 day"]
        ),
        assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/monitors",
        output_schema_name=f"{CATALOG}.{SCHEMA}"
    )
    print(f"Monitor created: {monitor_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Monitor already exists: {monitor_name}")
    else:
        print(f"Monitor creation requires Lakehouse Monitoring enabled")
        print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.3: Drift detection analysis
# MAGIC
# MAGIC Let's manually analyze what a drift scenario looks like:

# COMMAND ----------

from pyspark.sql.functions import avg, stddev, min, max, abs

# Analyze by day
daily_metrics = (
    spark.table(monitoring_table)
    .withColumn("date", col("timestamp").cast("date"))
    .groupBy("date")
    .agg(
        avg("error").alias("avg_error"),
        avg(abs(col("error"))).alias("mae"),
        avg("mape_pct").alias("avg_mape"),
        stddev("error").alias("error_stddev"),
        avg("actual_demand").alias("avg_actual"),
        avg("predicted_demand").alias("avg_predicted")
    )
    .orderBy("date")
)

display(daily_metrics)

print("\nNotice the jump in MAPE on 2024-01-16 - this is drift!")
print("Lakehouse Monitoring would automatically detect this and alert you.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4.4: Set up alerts
# MAGIC
# MAGIC Configure alerts to notify you when metrics exceed thresholds:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Example alert configuration
# MAGIC from databricks.sdk.service.sql import Alert, AlertCondition
# MAGIC
# MAGIC alert = w.alerts.create(
# MAGIC     name="Load Forecast MAPE Alert",
# MAGIC     query_id="<your_query_id>",
# MAGIC     condition=AlertCondition(
# MAGIC         op="GREATER_THAN",
# MAGIC         operand=AlertConditionOperand(
# MAGIC             column="avg_mape"
# MAGIC         ),
# MAGIC         threshold=AlertConditionThreshold(
# MAGIC             value="5.0"  # Alert if MAPE > 5%
# MAGIC         )
# MAGIC     ),
# MAGIC     notify_on_ok=True,  # Also notify when recovered
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Alert destinations:**
# MAGIC - Email
# MAGIC - Slack
# MAGIC - PagerDuty
# MAGIC - Webhooks (custom integrations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Production ML observability
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                    Production ML Pipeline                           │
# MAGIC │                                                                     │
# MAGIC │  ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐  │
# MAGIC │  │   Data    │───▶│   Model   │───▶│ Inference │───▶│  Monitor  │  │
# MAGIC │  │  Pipeline │    │  Serving  │    │  Tables   │    │  + Alert  │  │
# MAGIC │  └───────────┘    └───────────┘    └───────────┘    └───────────┘  │
# MAGIC │       │                │                │                │          │
# MAGIC │       │                │                │                │          │
# MAGIC │       ▼                ▼                ▼                ▼          │
# MAGIC │  ┌─────────────────────────────────────────────────────────────┐   │
# MAGIC │  │                Unity Catalog (Full Lineage)                 │   │
# MAGIC │  │    Data → Model → Predictions → Metrics → Actions          │   │
# MAGIC │  └─────────────────────────────────────────────────────────────┘   │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Before and after
# MAGIC
# MAGIC | Aspect | Before | After |
# MAGIC |--------|--------|-------|
# MAGIC | **Deployment** | Cron jobs, manual | Managed endpoints with auto-scaling |
# MAGIC | **Prediction logging** | Manual (if at all) | Automatic inference tables |
# MAGIC | **Drift detection** | Discovered by users | Automated monitoring |
# MAGIC | **Alerting** | None or manual checks | Configurable thresholds + notifications |
# MAGIC | **Investigation** | "What happened?" | Full lineage and history |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Q&A and discussion
# MAGIC
# MAGIC **Questions to discuss:**
# MAGIC 1. How do you currently monitor your forecasting models?
# MAGIC 2. What drift scenarios have you encountered? (weather events, COVID, etc.)
# MAGIC 3. What alerting integrations would be most useful? (email, Slack, PagerDuty?)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next workshop: Advanced ML and GenAI (optional)
# MAGIC
# MAGIC In the next session, we'll explore:
# MAGIC - **AutoML** - Automated model selection and hyperparameter tuning
# MAGIC - **Feature Store** - Centralized feature management with point-in-time lookups
# MAGIC - **GenAI capabilities** - How LLMs can augment your forecasting workflows
# MAGIC
# MAGIC **Or:** We can do a deep-dive on any topic from today's workshops based on your interest.
