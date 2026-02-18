# Databricks notebook source
# MAGIC %md
# MAGIC # NEMWEB Prediction Pipeline
# MAGIC
# MAGIC Loads registered UC champion models and scores silver data to produce
# MAGIC `demand_predictions` and `price_predictions` tables.
# MAGIC
# MAGIC This runs as a **notebook job** (not a Lakeflow Declarative Pipeline) because
# MAGIC model scoring requires `toPandas()` and `mlflow.pyfunc.load_model()`, which
# MAGIC use `DataFrame.collect` - not supported inside `@dp.materialized_view`.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Streaming pipeline has run (creates `silver_demand_weather`)
# MAGIC - At least one workshop has trained and registered a model with `@champion` alias

# COMMAND ----------

dbutils.widgets.text("catalog", "daveok")
dbutils.widgets.text("schema", "ml_workshops")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Prediction pipeline config: catalog={CATALOG}, schema={SCHEMA}")

# Drop existing tables in case they were created as MATERIALIZED_VIEWs by the old Lakeflow pipeline.
# saveAsTable cannot overwrite a MATERIALIZED_VIEW, so we must DROP first.
for tbl in ["demand_predictions", "price_predictions"]:
    spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

import sys, os
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

# Import ml_features from src/prediction_pipeline/transformations/
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src/prediction_pipeline/transformations")

from ml_features import (
    create_load_forecast_features, LOAD_FORECAST_EXCLUDE_COLS,
    create_price_forecast_features, PRICE_FORECAST_EXCLUDE_COLS,
)

# COMMAND ----------

DEMAND_PREDICTIONS_SCHEMA = StructType([
    StructField("settlement_date", TimestampType(), False),
    StructField("region_id", StringType(), False),
    StructField("actual_demand_mw", DoubleType(), True),
    StructField("predicted_demand_mw", DoubleType(), True),
    StructField("forecast_error_mw", DoubleType(), True),
    StructField("model_version", IntegerType(), True),
    StructField("scored_at", TimestampType(), True),
])

PRICE_PREDICTIONS_SCHEMA = StructType([
    StructField("settlement_date", TimestampType(), False),
    StructField("region_id", StringType(), False),
    StructField("actual_rrp", DoubleType(), True),
    StructField("predicted_rrp", DoubleType(), True),
    StructField("forecast_error_dollars", DoubleType(), True),
    StructField("model_version", IntegerType(), True),
    StructField("scored_at", TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read silver data

# COMMAND ----------

pdf = (
    spark.read.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .orderBy("settlement_date")
    .toPandas()
)

print(f"Silver data: {len(pdf)} rows")

if pdf.empty:
    print("No silver data available - writing empty prediction tables")
    spark.createDataFrame([], DEMAND_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.demand_predictions")
    spark.createDataFrame([], PRICE_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.price_predictions")
    dbutils.notebook.exit("No silver data")

pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
pdf = pdf.set_index("settlement_date").sort_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand predictions

# COMMAND ----------

demand_model_name = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"

try:
    demand_model = mlflow.pyfunc.load_model(f"models:/{demand_model_name}@champion")
    print(f"Loaded demand model: {demand_model_name}@champion")
except Exception as e:
    demand_model = None
    print(f"Demand model not found: {e}")

if demand_model is not None:
    try:
        client = mlflow.MlflowClient()
        version_info = client.get_model_version_by_alias(demand_model_name, "champion")
        demand_version = int(version_info.version)
    except Exception:
        demand_version = 0

    # Use dropna=False to avoid losing all rows when columns have NaN
    # (e.g. demand_forecast_mw or weather columns may be sparse)
    pdf_features = create_load_forecast_features(pdf.copy(), dropna=False)
    feature_cols = [c for c in pdf_features.columns if c not in LOAD_FORECAST_EXCLUDE_COLS]

    # Align columns and dtypes with model's expected schema
    try:
        model_input_schema = demand_model.metadata.get_input_schema()
        if model_input_schema is not None:
            expected_inputs = model_input_schema.inputs
            expected_cols = [col_spec.name for col_spec in expected_inputs]
            for col_spec in expected_inputs:
                col_name = col_spec.name
                if col_name not in pdf_features.columns:
                    pdf_features[col_name] = 0.0
                dtype_name = str(col_spec.type).lower()
                if "integer" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("int32")
                elif "long" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("int64")
                elif "double" in dtype_name or "float" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("float64")
            feature_cols = expected_cols
    except Exception:
        pass

    X = pdf_features[feature_cols].fillna(0)
    predictions = demand_model.predict(X)
    scored_at = datetime.now()

    demand_pred_df = pd.DataFrame({
        "settlement_date": X.index,
        "region_id": "NSW1",
        "actual_demand_mw": pdf_features["total_demand_mw"].values,
        "predicted_demand_mw": predictions.astype(float),
        "forecast_error_mw": (predictions - pdf_features["total_demand_mw"].values).astype(float),
        "model_version": demand_version,
        "scored_at": scored_at,
    })

    print(f"Demand: scored {len(demand_pred_df):,} rows with model v{demand_version}")
    print(f"Demand MAE: {demand_pred_df['forecast_error_mw'].abs().mean():.1f} MW")

    spark.createDataFrame(demand_pred_df, schema=DEMAND_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.demand_predictions")
else:
    print("Skipping demand predictions - no champion model")
    spark.createDataFrame([], DEMAND_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.demand_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price predictions

# COMMAND ----------

price_model_name = f"{CATALOG}.{SCHEMA}.price_forecast_nsw"

try:
    price_model = mlflow.pyfunc.load_model(f"models:/{price_model_name}@champion")
    print(f"Loaded price model: {price_model_name}@champion")
except Exception as e:
    price_model = None
    print(f"Price model not found: {e}")

if price_model is not None:
    try:
        client = mlflow.MlflowClient()
        version_info = client.get_model_version_by_alias(price_model_name, "champion")
        price_version = int(version_info.version)
    except Exception:
        price_version = 0

    pdf_features = create_price_forecast_features(pdf.copy(), dropna=False)
    feature_cols = [c for c in pdf_features.columns if c not in PRICE_FORECAST_EXCLUDE_COLS]

    # Align columns and dtypes with model's expected schema
    try:
        model_input_schema = price_model.metadata.get_input_schema()
        if model_input_schema is not None:
            expected_inputs = model_input_schema.inputs
            expected_cols = [col_spec.name for col_spec in expected_inputs]
            for col_spec in expected_inputs:
                col_name = col_spec.name
                if col_name not in pdf_features.columns:
                    pdf_features[col_name] = 0.0
                dtype_name = str(col_spec.type).lower()
                if "integer" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("int32")
                elif "long" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("int64")
                elif "double" in dtype_name or "float" in dtype_name:
                    pdf_features[col_name] = pdf_features[col_name].fillna(0).astype("float64")
            feature_cols = expected_cols
    except Exception:
        pass

    X = pdf_features[feature_cols].fillna(0)
    predictions = price_model.predict(X)
    scored_at = datetime.now()

    price_pred_df = pd.DataFrame({
        "settlement_date": X.index,
        "region_id": "NSW1",
        "actual_rrp": pdf_features["rrp"].values,
        "predicted_rrp": predictions.astype(float),
        "forecast_error_dollars": (predictions - pdf_features["rrp"].values).astype(float),
        "model_version": price_version,
        "scored_at": scored_at,
    })

    print(f"Price: scored {len(price_pred_df):,} rows with model v{price_version}")
    print(f"Price MAE: ${price_pred_df['forecast_error_dollars'].abs().mean():.2f}/MWh")

    spark.createDataFrame(price_pred_df, schema=PRICE_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.price_predictions")
else:
    print("Skipping price predictions - no champion model")
    spark.createDataFrame([], PRICE_PREDICTIONS_SCHEMA).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.price_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

demand_count = spark.read.table(f"{CATALOG}.{SCHEMA}.demand_predictions").count()
price_count = spark.read.table(f"{CATALOG}.{SCHEMA}.price_predictions").count()
print(f"demand_predictions: {demand_count:,} rows")
print(f"price_predictions: {price_count:,} rows")
