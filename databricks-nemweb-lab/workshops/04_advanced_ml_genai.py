# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop 4: Advanced ML and GenAI (optional)
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this workshop, you will understand:
# MAGIC 1. How AutoML accelerates model development
# MAGIC 2. How Feature Store prevents training-serving skew
# MAGIC 3. What GenAI capabilities are available in Databricks
# MAGIC
# MAGIC ## Agenda
# MAGIC | Section | Duration | Content |
# MAGIC |---------|----------|---------|
# MAGIC | AutoML | 15 min | Automated model selection and tuning |
# MAGIC | Feature Store | 15 min | Centralized features with point-in-time |
# MAGIC | GenAI overview | 15 min | Foundation Model APIs, RAG patterns |
# MAGIC | Demo | 10 min | AutoML on demand forecasting |
# MAGIC | Q&A | 5 min | Discussion and next steps |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The ML productivity challenge
# MAGIC
# MAGIC ```
# MAGIC Time spent on ML projects:
# MAGIC
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │ Data prep & feature engineering          ████████████ 60%   │
# MAGIC │ Model selection & hyperparameter tuning  ██████ 25%         │
# MAGIC │ Deployment & monitoring                  ██ 10%             │
# MAGIC │ Actual business value                    █ 5%               │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC AutoML and Feature Store help you spend more time on business value.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: AutoML (15 min)
# MAGIC
# MAGIC ### What is AutoML?
# MAGIC
# MAGIC AutoML automatically:
# MAGIC - Tries multiple algorithms (XGBoost, LightGBM, sklearn, Prophet)
# MAGIC - Tunes hyperparameters
# MAGIC - Handles feature preprocessing
# MAGIC - Generates a leaderboard of best models
# MAGIC
# MAGIC ### When to use AutoML
# MAGIC
# MAGIC | Use case | AutoML | Manual |
# MAGIC |----------|--------|--------|
# MAGIC | Quick baseline | ✅ | |
# MAGIC | Exploring new problem | ✅ | |
# MAGIC | Deep domain expertise needed | | ✅ |
# MAGIC | Custom architectures | | ✅ |
# MAGIC | Time-constrained | ✅ | |

# COMMAND ----------

# Setup
import mlflow
from databricks import automl
import pandas as pd

CATALOG = "workspace"
SCHEMA = "ml_workshops"

print("AutoML is available via:")
print("  - UI: Create > AutoML Experiment")
print("  - API: databricks.automl module")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 1.1: Prepare data for AutoML

# COMMAND ----------

# Load demand data
df_demand = spark.table(f"{CATALOG}.{SCHEMA}.nemweb_dispatch_regionsum")

# Prepare for AutoML - it expects a table with target column
automl_df = (
    df_demand
    .filter("REGIONID = 'NSW1'")
    .select("SETTLEMENTDATE", "TOTALDEMAND")
    .withColumnRenamed("SETTLEMENTDATE", "ds")  # AutoML expects 'ds' for time series
    .withColumnRenamed("TOTALDEMAND", "y")      # AutoML expects 'y' for target
    .orderBy("ds")
)

# Save as table for AutoML
automl_table = f"{CATALOG}.{SCHEMA}.automl_demand_input"
automl_df.write.mode("overwrite").saveAsTable(automl_table)

print(f"AutoML input table: {automl_table}")
print(f"Rows: {automl_df.count():,}")

display(automl_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 1.2: Run AutoML for forecasting
# MAGIC
# MAGIC **Note:** This can take 10-30 minutes to run. For the demo, we'll show the configuration and review pre-computed results.

# COMMAND ----------

# AutoML configuration (commented out for demo - would take too long)
# Uncomment to run:

# summary = automl.forecast(
#     dataset=automl_table,
#     target_col="y",
#     time_col="ds",
#     frequency="5min",
#     horizon=288,  # 24 hours of 5-min intervals
#     timeout_minutes=30,
#     primary_metric="smape"
# )

print("AutoML forecast configuration:")
print(f"  Table: {automl_table}")
print(f"  Target: y (demand)")
print(f"  Time column: ds")
print(f"  Frequency: 5 minutes")
print(f"  Horizon: 288 periods (24 hours)")
print(f"  Timeout: 30 minutes")
print(f"  Primary metric: SMAPE")

print("\nAutoML will try:")
print("  - Prophet (with various seasonality configs)")
print("  - ARIMA")
print("  - XGBoost/LightGBM with lag features")
print("  - Ensemble methods")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 1.3: AutoML results
# MAGIC
# MAGIC After AutoML runs, you get:
# MAGIC - **Leaderboard** - All models ranked by metric
# MAGIC - **Best model** - Automatically registered to MLflow
# MAGIC - **Notebooks** - Generated code for each model (reproducibility!)
# MAGIC - **Feature importance** - What drives predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC AutoML Leaderboard (example):
# MAGIC
# MAGIC ┌─────────────────────────┬─────────┬──────────┬──────────┐
# MAGIC │ Model                   │ SMAPE   │ MAE      │ RMSE     │
# MAGIC ├─────────────────────────┼─────────┼──────────┼──────────┤
# MAGIC │ Prophet (tuned)         │ 2.34%   │ 185 MW   │ 245 MW   │
# MAGIC │ LightGBM                │ 2.51%   │ 198 MW   │ 262 MW   │
# MAGIC │ XGBoost                 │ 2.58%   │ 203 MW   │ 271 MW   │
# MAGIC │ ARIMA                   │ 3.12%   │ 245 MW   │ 318 MW   │
# MAGIC │ Prophet (default)       │ 3.45%   │ 271 MW   │ 352 MW   │
# MAGIC └─────────────────────────┴─────────┴──────────┴──────────┘
# MAGIC
# MAGIC Best model: Prophet (tuned) - automatically registered
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Feature Store (15 min)
# MAGIC
# MAGIC ### The training-serving skew problem
# MAGIC
# MAGIC ```
# MAGIC Training time:                          Serving time:
# MAGIC ┌─────────────────┐                    ┌─────────────────┐
# MAGIC │ Load data       │                    │ Get features    │
# MAGIC │ Engineer feats  │  DIFFERENT CODE!   │ (different SQL) │
# MAGIC │ Train model     │ ═══════════════════│ Make prediction │
# MAGIC └─────────────────┘                    └─────────────────┘
# MAGIC         │                                       │
# MAGIC         └───── Subtle bugs, silent failures ────┘
# MAGIC ```
# MAGIC
# MAGIC ### Feature Store solution
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Unity Catalog Feature Store              │
# MAGIC │                                                             │
# MAGIC │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐       │
# MAGIC │  │ Weather     │   │ Calendar    │   │ Lag         │       │
# MAGIC │  │ Features    │   │ Features    │   │ Features    │       │
# MAGIC │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘       │
# MAGIC │         │                 │                 │               │
# MAGIC │         └─────────────────┼─────────────────┘               │
# MAGIC │                           │                                 │
# MAGIC │                   ┌───────┴───────┐                         │
# MAGIC │                   │ Same features │                         │
# MAGIC │                   │ for training  │                         │
# MAGIC │                   │ AND serving   │                         │
# MAGIC │                   └───────────────┘                         │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.1: Create a feature table

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.functions import col, hour, dayofweek, month, to_date, avg, stddev

fe = FeatureEngineeringClient()

# Create time-based features
demand_features = (
    spark.table(f"{CATALOG}.{SCHEMA}.nemweb_dispatch_regionsum")
    .filter("REGIONID = 'NSW1'")
    .select(
        col("SETTLEMENTDATE").alias("timestamp"),
        col("TOTALDEMAND"),
        col("REGIONID")
    )
    .withColumn("hour", hour("timestamp"))
    .withColumn("day_of_week", dayofweek("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("date", to_date("timestamp"))
)

# Create a feature table
feature_table_name = f"{CATALOG}.{SCHEMA}.demand_features"

# Save as feature table
demand_features.write.mode("overwrite").saveAsTable(feature_table_name)

print(f"Feature table created: {feature_table_name}")
display(demand_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.2: Point-in-time lookups
# MAGIC
# MAGIC For time series, you need features **as of** a specific time (no future leakage):

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Point-in-time feature lookup
# MAGIC from databricks.feature_engineering import FeatureLookup
# MAGIC
# MAGIC training_set = fe.create_training_set(
# MAGIC     df=labels_df,  # Your target variable with timestamps
# MAGIC     feature_lookups=[
# MAGIC         FeatureLookup(
# MAGIC             table_name=feature_table_name,
# MAGIC             lookup_key=["timestamp"],
# MAGIC             timestamp_lookup_key="timestamp",  # Point-in-time!
# MAGIC             feature_names=["hour", "day_of_week", "rolling_avg"]
# MAGIC         )
# MAGIC     ],
# MAGIC     label="TOTALDEMAND"
# MAGIC )
# MAGIC
# MAGIC # Features automatically looked up with correct timestamps
# MAGIC training_df = training_set.load_df()
# MAGIC ```
# MAGIC
# MAGIC **Key benefit:** Same code for training and serving - no skew!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: GenAI overview (15 min)
# MAGIC
# MAGIC ### GenAI capabilities in Databricks
# MAGIC
# MAGIC | Capability | Description | Energy industry use case |
# MAGIC |------------|-------------|--------------------------|
# MAGIC | **Foundation Model APIs** | Hosted LLMs (GPT-4, Claude, Llama) | Natural language queries on data |
# MAGIC | **Vector Search** | Semantic search on documents | Search operational manuals, regulations |
# MAGIC | **AI Functions** | SQL-based AI | Classify, extract, summarize in SQL |
# MAGIC | **Mosaic AI Agents** | Build AI assistants | "Why did demand spike yesterday?" |
# MAGIC
# MAGIC ### How GenAI complements forecasting
# MAGIC
# MAGIC GenAI doesn't replace your forecasting models - it augments them:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │               Forecasting + GenAI                           │
# MAGIC │                                                             │
# MAGIC │  Traditional ML:          GenAI Enhancement:                │
# MAGIC │  ┌─────────────┐          ┌─────────────────────────────┐  │
# MAGIC │  │ Load        │          │ "Explain why forecast was   │  │
# MAGIC │  │ Forecast    │────────▶ │  wrong on Jan 16th"         │  │
# MAGIC │  │ Model       │          │                             │  │
# MAGIC │  └─────────────┘          │ "There was an unplanned     │  │
# MAGIC │                           │  outage at Liddell + heat   │  │
# MAGIC │                           │  wave in Sydney"            │  │
# MAGIC │                           └─────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.1: AI Functions in SQL

# COMMAND ----------

# AI Functions allow you to call LLMs directly from SQL
# Example: Classify the cause of demand spikes

# MAGIC %sql
# MAGIC -- Example AI Function usage (requires Foundation Model API access)
# MAGIC -- SELECT
# MAGIC --     timestamp,
# MAGIC --     demand,
# MAGIC --     ai_classify(
# MAGIC --         CONCAT('Demand spike of ', demand, ' MW on ', timestamp),
# MAGIC --         ARRAY('weather_event', 'outage', 'special_event', 'normal_variation')
# MAGIC --     ) as likely_cause
# MAGIC -- FROM demand_spikes

print("AI Functions enable:")
print("  - Classification: ai_classify()")
print("  - Extraction: ai_extract()")
print("  - Summarization: ai_summarize()")
print("  - Generation: ai_generate()")
print("\nAll callable directly from SQL!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.2: Foundation Model APIs

# COMMAND ----------

# Query a Foundation Model for analysis
from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# Example: Ask about energy demand patterns
response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": [
            {
                "role": "user",
                "content": """You are an energy analyst. Given that NSW electricity demand
                peaked at 12,500 MW yesterday (15% above normal), what are the most likely
                causes? Consider typical Australian conditions."""
            }
        ],
        "max_tokens": 200
    }
)

print("LLM Analysis:")
print(response.choices[0]["message"]["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.3: RAG for operational knowledge
# MAGIC
# MAGIC **Retrieval-Augmented Generation (RAG)** lets you query your own documents:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │ Operational │     │ Vector      │     │ LLM         │
# MAGIC │ Manuals     │────▶│ Search      │────▶│ Response    │
# MAGIC │ Procedures  │     │ (find       │     │ (grounded   │
# MAGIC │ Regulations │     │  relevant)  │     │  in docs)   │
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC
# MAGIC User: "What's the procedure for handling negative prices?"
# MAGIC
# MAGIC RAG: "According to your operational manual v2.3, section 4.2:
# MAGIC       When RRP goes negative, the following steps apply..."
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Advanced capabilities
# MAGIC
# MAGIC | Capability | What it does | Time savings |
# MAGIC |------------|--------------|--------------|
# MAGIC | **AutoML** | Automated model selection + tuning | Days → Hours |
# MAGIC | **Feature Store** | Centralized features, no skew | Eliminates debugging |
# MAGIC | **GenAI** | Natural language interface to data | Faster insights |
# MAGIC
# MAGIC ### When to use each
# MAGIC
# MAGIC - **AutoML:** Quick baselines, new problems, time-constrained projects
# MAGIC - **Feature Store:** Production systems, multiple models sharing features
# MAGIC - **GenAI:** Explanations, document search, natural language queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Workshop series wrap-up
# MAGIC
# MAGIC ### What we covered
# MAGIC
# MAGIC | Workshop | Key takeaway |
# MAGIC |----------|--------------|
# MAGIC | 1. Pipelines | Replace fragmented scripts with declarative pipelines |
# MAGIC | 2. MLflow | Track experiments, version models, full reproducibility |
# MAGIC | 3. Serving | Production deployment with monitoring and alerts |
# MAGIC | 4. Advanced | Accelerate with AutoML, Feature Store, GenAI |
# MAGIC
# MAGIC ### Your potential path forward
# MAGIC
# MAGIC ```
# MAGIC Phase 1: Pilot (1-2 months)
# MAGIC  └─▶ Migrate one pipeline to Lakeflow
# MAGIC  └─▶ Track one model with MLflow
# MAGIC
# MAGIC Phase 2: Expand (3-6 months)
# MAGIC  └─▶ Deploy model to serving endpoint
# MAGIC  └─▶ Set up monitoring and alerts
# MAGIC  └─▶ Migrate additional pipelines
# MAGIC
# MAGIC Phase 3: Optimize (6+ months)
# MAGIC  └─▶ Feature Store for shared features
# MAGIC  └─▶ AutoML for new use cases
# MAGIC  └─▶ GenAI for insights and documentation
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A and next steps
# MAGIC
# MAGIC **Questions to discuss:**
# MAGIC 1. Which capability would have the biggest impact for your team?
# MAGIC 2. What would a pilot project look like?
# MAGIC 3. What blockers or concerns do you have?
# MAGIC
# MAGIC **Possible follow-ups:**
# MAGIC - Deep-dive on any specific topic
# MAGIC - Hands-on workshop with your data
# MAGIC - Architecture review of your current systems
# MAGIC - Proof of concept planning
