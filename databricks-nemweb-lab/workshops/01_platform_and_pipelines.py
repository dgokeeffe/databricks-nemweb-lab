# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop 1: Platform foundations and data pipelines
# MAGIC
# MAGIC **Duration:** 60 minutes | **Format:** Demo-focused
# MAGIC
# MAGIC ## Learning objectives
# MAGIC By the end of this workshop, you will understand:
# MAGIC 1. How Databricks unifies data engineering, ML, and governance
# MAGIC 2. How to replace "pull from multiple servers" scripts with declarative pipelines
# MAGIC 3. Built-in data quality, lineage, and monitoring capabilities
# MAGIC
# MAGIC ## Agenda
# MAGIC | Section | Duration | Content |
# MAGIC |---------|----------|---------|
# MAGIC | Platform overview | 10 min | Unity Catalog, Serverless, Lakehouse architecture |
# MAGIC | Data ingestion | 20 min | Lakeflow Declarative Pipelines, custom data sources |
# MAGIC | Live demo | 20 min | Build a pipeline ingesting AEMO/NEMWEB energy data |
# MAGIC | Q&A | 10 min | Discussion and questions |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your current state (sound familiar?)
# MAGIC
# MAGIC ```
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
# MAGIC **Pain points:**
# MAGIC - Fragile scripts across multiple servers
# MAGIC - No visibility when something fails
# MAGIC - Data quality issues discovered by downstream users
# MAGIC - No governance or audit trail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Platform overview (10 min)
# MAGIC
# MAGIC ### The Databricks Lakehouse
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
# MAGIC ### Key concepts for your teams
# MAGIC
# MAGIC | Concept | What it means for you |
# MAGIC |---------|----------------------|
# MAGIC | **Unity Catalog** | Single place to find all data, models, and who can access what |
# MAGIC | **Serverless** | No more cluster sizing debates - just run your code |
# MAGIC | **Delta Lake** | Your data files with database-like guarantees (rollback, audit) |
# MAGIC | **Lakeflow** | Declare what you want, not how to build it |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Data ingestion demo (20 min)
# MAGIC
# MAGIC We'll demonstrate using real **AEMO NEMWEB data** - the same data your teams work with for load forecasting and market modelling.
# MAGIC
# MAGIC ### Data sources we'll use
# MAGIC
# MAGIC | Table | Description | Use case |
# MAGIC |-------|-------------|----------|
# MAGIC | `DISPATCHREGIONSUM` | 5-minute regional demand and generation | Load forecasting target variable |
# MAGIC | `DISPATCHPRICE` | 5-minute spot prices per region | Price forecasting, market models |
# MAGIC | `DISPATCH_UNIT_SCADA` | Real-time unit generation | Generator dispatch patterns |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: Configure environment

# COMMAND ----------

# Configuration - in production, these come from Unity Catalog
CATALOG = "workspace"
SCHEMA = "ml_workshops"

# Create schema if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Working in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.1: Traditional approach vs Databricks
# MAGIC
# MAGIC **Traditional script** (what you might have today):
# MAGIC ```python
# MAGIC # Your current approach - fragile, no monitoring
# MAGIC import requests
# MAGIC import pandas as pd
# MAGIC from zipfile import ZipFile
# MAGIC
# MAGIC url = "https://nemweb.com.au/REPORTS/ARCHIVE/..."
# MAGIC response = requests.get(url)  # No retry, no timeout
# MAGIC # ... lots of manual parsing code
# MAGIC # ... save to CSV somewhere
# MAGIC # ... hope nothing breaks overnight
# MAGIC ```
# MAGIC
# MAGIC **Databricks approach** - declarative, monitored, governed:

# COMMAND ----------

# First, let's see what data sources are available
# The custom NEMWEB data source handles all the complexity

import sys
import os

# Get the path to the src directory
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

# Import and register our custom data source
from nemweb_datasource_arrow import NemwebArrowDataSource

# Register the data source with Spark
spark.dataSource.register(NemwebArrowDataSource)

print("Custom data source registered: nemweb_arrow")
print("\nThis handles:")
print("  - HTTP requests with retry logic")
print("  - ZIP file extraction")
print("  - CSV parsing with schema validation")
print("  - Arrow conversion for Serverless compatibility")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.2: Reading the data - already loaded
# MAGIC
# MAGIC The data has been pre-loaded into a Delta table. In production, this would be
# MAGIC refreshed by a Lakeflow pipeline (shown in Part 3).

# COMMAND ----------

# Read regional dispatch data from the pre-loaded table
df_demand = spark.table(f"{CATALOG}.{SCHEMA}.nemweb_dispatch_regionsum")

# Show the data structure
print(f"Regional demand data: {df_demand.count():,} rows")
df_demand.printSchema()

# COMMAND ----------

# Preview the data - 5 NEM regions, 5-minute intervals
display(
    df_demand
    .select("SETTLEMENTDATE", "REGIONID", "TOTALDEMAND", "AVAILABLEGENERATION", "NETINTERCHANGE")
    .orderBy("SETTLEMENTDATE", ascending=False)
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2.3: Regional demand analysis
# MAGIC
# MAGIC Quick analysis showing the power of SQL on Delta tables:

# COMMAND ----------

# Regional demand summary
display(
    df_demand
    .groupBy("REGIONID")
    .agg(
        {"TOTALDEMAND": "avg", "TOTALDEMAND": "max", "TOTALDEMAND": "min"}
    )
    .orderBy("avg(TOTALDEMAND)", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Lakeflow Declarative Pipelines (20 min)
# MAGIC
# MAGIC Now let's see how this becomes a **production pipeline** with:
# MAGIC - Automatic scheduling
# MAGIC - Built-in monitoring
# MAGIC - Data quality checks
# MAGIC - Full lineage tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.1: Pipeline definition
# MAGIC
# MAGIC Instead of hundreds of lines of orchestration code, you declare what you want:
# MAGIC
# MAGIC ```python
# MAGIC # pipelines/nemweb_pipeline.py
# MAGIC import dlt  # Lakeflow (formerly Delta Live Tables)
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Bronze layer - raw NEMWEB dispatch data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_region", "REGIONID IS NOT NULL")
# MAGIC @dlt.expect_or_fail("valid_demand", "TOTALDEMAND >= 0")
# MAGIC def bronze_dispatch():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("nemweb_arrow")
# MAGIC         .option("table", "DISPATCHREGIONSUM")
# MAGIC         .load()
# MAGIC     )
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Silver layer - cleaned and enriched"
# MAGIC )
# MAGIC def silver_dispatch():
# MAGIC     return (
# MAGIC         dlt.read("bronze_dispatch")
# MAGIC         .withColumn("dispatch_hour", hour("SETTLEMENTDATE"))
# MAGIC         .withColumn("dispatch_date", to_date("SETTLEMENTDATE"))
# MAGIC     )
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Gold layer - aggregated for analytics"
# MAGIC )
# MAGIC def gold_daily_demand():
# MAGIC     return (
# MAGIC         dlt.read("silver_dispatch")
# MAGIC         .groupBy("dispatch_date", "REGIONID")
# MAGIC         .agg(
# MAGIC             avg("TOTALDEMAND").alias("avg_demand"),
# MAGIC             max("TOTALDEMAND").alias("peak_demand"),
# MAGIC             sum("TOTALDEMAND").alias("total_energy_mwh")
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **What you get for free:**
# MAGIC - Data quality expectations (`expect_or_drop`, `expect_or_fail`)
# MAGIC - Automatic dependency resolution
# MAGIC - Incremental processing (only new data)
# MAGIC - Full lineage in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.2: The actual pipeline code
# MAGIC
# MAGIC Here's the real pipeline from this repository (`pipelines/nemweb_pipeline.py`).
# MAGIC
# MAGIC **Key points:**
# MAGIC - Uses modern **Spark Declarative Pipelines** (`from pyspark import pipelines as dp`)
# MAGIC - **Medallion architecture**: Bronze → Silver → Gold
# MAGIC - **Data quality expectations**: Invalid rows automatically dropped
# MAGIC - **Declarative**: You define *what*, the platform handles *how*
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import col, current_timestamp, expr, avg, max, min
# MAGIC
# MAGIC # =============================================================================
# MAGIC # Bronze Layer - Raw Ingestion with Metadata
# MAGIC # =============================================================================
# MAGIC @dp.table(
# MAGIC     name="nemweb_bronze",
# MAGIC     comment="Raw NEMWEB dispatch region data with ingestion metadata"
# MAGIC )
# MAGIC def nemweb_bronze():
# MAGIC     return (
# MAGIC         spark.read.table("workspace.nemweb_lab.nemweb_dispatch_regionsum")
# MAGIC         .withColumn("_ingested_at", current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC # =============================================================================
# MAGIC # Silver Layer - Cleansed Data with Quality Checks
# MAGIC # =============================================================================
# MAGIC @dp.table(name="nemweb_silver", comment="Cleansed NEMWEB data")
# MAGIC @dp.expect_or_drop("valid_region", "region_id IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')")
# MAGIC @dp.expect_or_drop("valid_demand", "total_demand_mw > 0 AND total_demand_mw < 20000")
# MAGIC def nemweb_silver():
# MAGIC     return (
# MAGIC         spark.read.table("nemweb_bronze")  # Read from bronze
# MAGIC         .select(
# MAGIC             col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
# MAGIC             col("REGIONID").alias("region_id"),
# MAGIC             col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
# MAGIC             col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
# MAGIC             col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
# MAGIC         )
# MAGIC         .withColumn("demand_generation_ratio",
# MAGIC                     col("total_demand_mw") / col("available_generation_mw"))
# MAGIC     )
# MAGIC
# MAGIC # =============================================================================
# MAGIC # Gold Layer - Business Aggregations
# MAGIC # =============================================================================
# MAGIC @dp.materialized_view(name="nemweb_gold_daily", comment="Daily regional summary")
# MAGIC def nemweb_gold_daily():
# MAGIC     return (
# MAGIC         spark.read.table("nemweb_silver")
# MAGIC         .withColumn("date", expr("date(settlement_date)"))
# MAGIC         .groupBy("region_id", "date")
# MAGIC         .agg(
# MAGIC             avg("total_demand_mw").alias("avg_demand_mw"),
# MAGIC             max("total_demand_mw").alias("peak_demand_mw"),
# MAGIC             min("total_demand_mw").alias("min_demand_mw"),
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key SDP concepts
# MAGIC
# MAGIC | Decorator | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `@dp.table()` | Creates a streaming table (supports incremental updates) |
# MAGIC | `@dp.materialized_view()` | Creates a materialized view (recomputed on refresh) |
# MAGIC | `@dp.expect_or_drop()` | Data quality - drop rows that fail the check |
# MAGIC | `@dp.expect_or_fail()` | Data quality - fail the pipeline if check fails |

# COMMAND ----------

# The pipeline is already deployed to this workspace
# Let's look at the output tables it creates
print("Pipeline output tables (in nemweb_lab schema):")
print("  - nemweb_bronze: Raw data with ingestion timestamp")
print("  - nemweb_silver: Cleansed data with quality checks")
print("  - nemweb_gold_hourly: Hourly aggregations")
print("  - nemweb_gold_daily: Daily aggregations")
print()
print("Navigate to: Workflows > Delta Live Tables to see the pipeline DAG")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.3: Data quality expectations in action
# MAGIC
# MAGIC Instead of discovering bad data from angry stakeholders, catch it at ingestion:

# COMMAND ----------

from pyspark.sql.functions import col, when, count

# Simulate data quality checks you'd put in a pipeline
quality_checks = (
    df_demand
    .agg(
        count(when(col("REGIONID").isNull(), 1)).alias("null_regions"),
        count(when(col("TOTALDEMAND") < 0, 1)).alias("negative_demand"),
        count(when(col("SETTLEMENTDATE").isNull(), 1)).alias("null_timestamps"),
        count("*").alias("total_rows")
    )
)

display(quality_checks)

print("\nIn Lakeflow, you declare these as expectations:")
print("  @dlt.expect_or_drop('valid_region', 'REGIONID IS NOT NULL')")
print("  @dlt.expect_or_fail('positive_demand', 'TOTALDEMAND >= 0')")
print("\nBad rows are automatically quarantined or the pipeline fails fast.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3.4: Lineage - know where your data comes from
# MAGIC
# MAGIC Unity Catalog automatically tracks data lineage:
# MAGIC
# MAGIC ```
# MAGIC NEMWEB API → Bronze Table → Silver Table → Gold Table → Dashboard
# MAGIC                  ↓              ↓             ↓
# MAGIC              ML Model      Forecast       Plexos Input
# MAGIC ```
# MAGIC
# MAGIC Let's see this in the UI...

# COMMAND ----------

# Query lineage information (if available)
# This shows which tables feed into which
try:
    lineage = spark.sql(f"""
        SELECT
            source_table_full_name,
            target_table_full_name,
            source_type,
            target_type
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{CATALOG}.{SCHEMA}.%'
        LIMIT 20
    """)
    display(lineage)
except Exception as e:
    print("Lineage query requires system tables access.")
    print("In the UI: Navigate to a table → Lineage tab to see upstream/downstream dependencies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Monitoring and observability
# MAGIC
# MAGIC ### What happens when something breaks?
# MAGIC
# MAGIC **Your current state:** Silent failure, discovered when someone asks "where's the data?"
# MAGIC
# MAGIC **Databricks approach:**
# MAGIC - Pipeline run history with success/failure status
# MAGIC - Data quality metrics over time
# MAGIC - Alerting integration (email, Slack, PagerDuty)
# MAGIC - System tables for operational queries

# COMMAND ----------

# Query pipeline run history (if available)
try:
    runs = spark.sql("""
        SELECT
            pipeline_name,
            update_id,
            state,
            creation_time,
            TIMESTAMPDIFF(MINUTE, creation_time, COALESCE(end_time, current_timestamp())) as duration_minutes
        FROM system.lakeflow.pipeline_event_log
        ORDER BY creation_time DESC
        LIMIT 10
    """)
    display(runs)
except Exception as e:
    print("System tables access required for operational queries.")
    print("Alternative: Use the Workflows UI to see run history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Before and after
# MAGIC
# MAGIC | Aspect | Before (Scripts on servers) | After (Databricks) |
# MAGIC |--------|---------------------------|-------------------|
# MAGIC | **Scheduling** | Cron jobs, manual intervention | Workflows with dependencies |
# MAGIC | **Failure handling** | Silent failures, manual recovery | Automatic alerts, retry logic |
# MAGIC | **Data quality** | Discovered by users | Enforced at ingestion |
# MAGIC | **Lineage** | None / spreadsheets | Automatic in Unity Catalog |
# MAGIC | **Monitoring** | Custom dashboards (maybe) | Built-in system tables |
# MAGIC | **Governance** | File permissions | Role-based access control |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Q&A and discussion
# MAGIC
# MAGIC **Questions to discuss:**
# MAGIC 1. Which of your current pipelines would benefit most from this approach?
# MAGIC 2. What data quality issues do you encounter regularly?
# MAGIC 3. How do you currently handle NEMWEB API rate limits and failures?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next workshop: ML workflows and MLflow
# MAGIC
# MAGIC In the next session, we'll take the data we just ingested and:
# MAGIC - Train load forecasting models (Prophet, XGBoost, LightGBM)
# MAGIC - Track experiments with MLflow
# MAGIC - Compare model performance
# MAGIC - Register the best model to Unity Catalog
# MAGIC
# MAGIC **Homework (optional):** Think about what features (weather, calendar, etc.) you currently use in your forecasting models.
