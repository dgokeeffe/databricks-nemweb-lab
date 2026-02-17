# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store workshop - declarative feature engineering
# MAGIC
# MAGIC **Duration:** 45 minutes | **Format:** Demo-focused (instructor drives, participants observe)
# MAGIC
# MAGIC **Audience:** ML engineers and data scientists who want to see how Databricks Feature Store
# MAGIC handles feature engineering declaratively - no manual window functions, no point-in-time
# MAGIC join bugs, and full lineage from raw data to model predictions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Section | Duration | Format |
# MAGIC |---------|----------|--------|
# MAGIC | **Why Feature Store?** | 5 min | Discussion |
# MAGIC | **Define features declaratively** | 15 min | Demo - time-windowed aggregations on live NEM data |
# MAGIC | **Train with point-in-time correctness** | 15 min | Demo - training set, XGBoost, MLflow with lineage |
# MAGIC | **Batch scoring + materialization** | 10 min | Demo - score_batch, offline store, scheduling |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Use case: NEM price spike prediction
# MAGIC
# MAGIC Predict whether the next dispatch interval will be a **price spike** (RRP > $300/MWh).
# MAGIC Price spikes are rare but expensive - traders and battery operators need advance warning.
# MAGIC
# MAGIC The Feature Store handles the hard parts:
# MAGIC - **Point-in-time correctness** - no future data leaks into training
# MAGIC - **Declarative definitions** - say *what* you want, not *how* to compute it
# MAGIC - **Lineage** - every model knows exactly which features it was trained on
# MAGIC - **Reuse** - same feature definitions for training and serving

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Setup

# COMMAND ----------

# Dependencies (databricks-feature-engineering>=0.14.0, xgboost) are installed
# via the job environment spec. For interactive use, uncomment the line below:
# %pip install databricks-feature-engineering>=0.14.0 xgboost

# COMMAND ----------

# Configuration - uses widget parameters from bundle job, with defaults for interactive use
dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "ml_workshops", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# Feature Store objects live in a dedicated schema
FS_SCHEMA = f"{SCHEMA}"

print(f"Source data: {CATALOG}.{SCHEMA}")
print(f"Feature Store schema: {CATALOG}.{FS_SCHEMA}")

# COMMAND ----------

# Verify source data exists
row_count = spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather").count()
print(f"silver_demand_weather: {row_count:,} rows")
assert row_count > 0, "No data in silver_demand_weather - run the streaming pipeline first"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Why Feature Store? (5 min)
# MAGIC
# MAGIC ### The problem with manual feature engineering
# MAGIC
# MAGIC ```
# MAGIC Without Feature Store:
# MAGIC
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  Training notebook:                                         │
# MAGIC │    pdf["demand_rolling_1h"] = pdf["demand"].rolling(12).mean()  │
# MAGIC │    pdf["price_avg_6h"] = ...                                │
# MAGIC │    # 50 lines of pandas window functions                    │
# MAGIC │    # Hope the serving code matches exactly...               │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  Serving pipeline:                                          │
# MAGIC │    # Copy-paste from training notebook                      │
# MAGIC │    # Different developer, 3 months later                    │
# MAGIC │    # "Did training use a 1h or 2h window?"                  │
# MAGIC │    # "Was it rolling mean or rolling median?"               │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### With Feature Store
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │  Define once:                                               │
# MAGIC │    fe.create_feature("price_avg_1h", source=dispatch_data,  │
# MAGIC │        inputs=["rrp"], function=Avg(),                      │
# MAGIC │        time_window=ContinuousWindow(timedelta(hours=1)))    │
# MAGIC │                                                             │
# MAGIC │  Training:  fe.create_training_set(labels, features)        │
# MAGIC │  Serving:   fe.score_batch(model_uri, new_data)             │
# MAGIC │                                                             │
# MAGIC │  Same features, same computation, guaranteed.               │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Define features declaratively (15 min)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    DeltaTableSource,
    ContinuousWindow,
    TumblingWindow,
    SlidingWindow,
    Sum, Avg, Count, Min, Max, StddevPop,
    OfflineStoreConfig,
)
from datetime import timedelta

fe = FeatureEngineeringClient()

print("FeatureEngineeringClient initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Define the data source
# MAGIC
# MAGIC The `DeltaTableSource` tells the Feature Store:
# MAGIC - **Where** the data lives (Unity Catalog table)
# MAGIC - **Entity columns** - the join keys (region_id)
# MAGIC - **Timeseries column** - for point-in-time windowing (settlement_date)

# COMMAND ----------

# Our silver table has dispatch + price + weather joined at 5-min intervals
dispatch_source = DeltaTableSource(
    catalog_name=CATALOG,
    schema_name=SCHEMA,
    table_name="silver_demand_weather",
    entity_columns=["region_id"],
    timeseries_column="settlement_date",
)

print(f"Data source: {CATALOG}.{SCHEMA}.silver_demand_weather")
print(f"Entity columns: ['region_id']")
print(f"Timeseries column: settlement_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Demand features
# MAGIC
# MAGIC Time-windowed aggregations over total demand. The `ContinuousWindow` looks back
# MAGIC from each evaluation timestamp - exactly what you need for point-in-time correctness.

# COMMAND ----------

demand_features = [
    # Recent demand context (last hour = 12 x 5-min intervals)
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="demand_avg_1h",
        description="Average demand (MW) over the last hour",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="demand_max_1h",
        description="Peak demand (MW) over the last hour",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Max(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
    # Daily demand context
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="demand_avg_24h",
        description="Average demand (MW) over the last 24 hours",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=24)),
    ),
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="demand_max_24h",
        description="Peak demand (MW) in the last 24 hours",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Max(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=24)),
    ),
]

print(f"Defined {len(demand_features)} demand features")
for f in demand_features:
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Price features
# MAGIC
# MAGIC Price history is critical for spike prediction. Recent volatility (stddev)
# MAGIC and price momentum (recent avg vs historical avg) are strong signals.

# COMMAND ----------

price_features = [
    # Recent price context
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="price_avg_1h",
        description="Average RRP ($/MWh) over the last hour",
        source=dispatch_source,
        inputs=["rrp"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="price_max_1h",
        description="Maximum RRP ($/MWh) over the last hour",
        source=dispatch_source,
        inputs=["rrp"],
        function=Max(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
    # Daily price context
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="price_avg_24h",
        description="Average RRP ($/MWh) over the last 24 hours",
        source=dispatch_source,
        inputs=["rrp"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=24)),
    ),
    # Price volatility - high volatility often precedes spikes
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="price_stddev_6h",
        description="Price volatility (stddev of RRP) over the last 6 hours",
        source=dispatch_source,
        inputs=["rrp"],
        function=StddevPop(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=6)),
    ),
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="price_max_24h",
        description="Maximum RRP ($/MWh) over the last 24 hours",
        source=dispatch_source,
        inputs=["rrp"],
        function=Max(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=24)),
    ),
]

print(f"Defined {len(price_features)} price features")
for f in price_features:
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Supply margin features
# MAGIC
# MAGIC The **supply-demand margin** (available generation minus demand) is THE key
# MAGIC driver of price spikes. When it's tight, expensive peaking generators get
# MAGIC dispatched and prices spike.

# COMMAND ----------

supply_features = [
    # Available generation context
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="available_gen_avg_1h",
        description="Average available generation (MW) over the last hour",
        source=dispatch_source,
        inputs=["available_generation_mw"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
    # Minimum available generation - signals tight supply
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="available_gen_min_24h",
        description="Minimum available generation (MW) in last 24h - tight supply signal",
        source=dispatch_source,
        inputs=["available_generation_mw"],
        function=Min(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=24)),
    ),
    # Interconnector flow context
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="interchange_avg_1h",
        description="Average net interchange (MW) over the last hour",
        source=dispatch_source,
        inputs=["net_interchange_mw"],
        function=Avg(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
]

print(f"Defined {len(supply_features)} supply features")
for f in supply_features:
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Trend features
# MAGIC
# MAGIC Compare recent demand to historical demand using **offset windows**.
# MAGIC If current demand is much higher than the same period a week ago, that's a signal.

# COMMAND ----------

trend_features = [
    # Hourly demand pattern (tumbling - non-overlapping hourly buckets)
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="demand_avg_hourly",
        description="Average demand per hour (tumbling window)",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Avg(),
        time_window=TumblingWindow(window_duration=timedelta(hours=1)),
    ),
    # Interval count per hour - data completeness check
    fe.create_feature(
        catalog_name=CATALOG,
        schema_name=FS_SCHEMA,
        name="interval_count_1h",
        description="Number of 5-min intervals in the last hour (expect 12)",
        source=dispatch_source,
        inputs=["total_demand_mw"],
        function=Count(),
        time_window=ContinuousWindow(window_duration=timedelta(hours=1)),
    ),
]

print(f"Defined {len(trend_features)} trend features")
for f in trend_features:
    print(f"  - {f.name}")

# COMMAND ----------

# Collect all features
all_features = demand_features + price_features + supply_features + trend_features

print(f"\nTotal features defined: {len(all_features)}")
print("\nFeature summary:")
for i, f in enumerate(all_features, 1):
    print(f"  {i:2d}. {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Train with point-in-time correctness (15 min)
# MAGIC
# MAGIC Now we create a **training set** that joins our features with labeled data.
# MAGIC The Feature Store guarantees that features are computed using only data
# MAGIC available BEFORE each label's timestamp - no future data leakage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create labeled dataset
# MAGIC
# MAGIC Our target: **is_price_spike** (1 if RRP > $300/MWh, 0 otherwise).
# MAGIC
# MAGIC We build the labels from the source data, keeping only the columns the
# MAGIC Feature Store needs for joining (entity + timeseries) plus the target.

# COMMAND ----------

from pyspark.sql.functions import col, when

# Build labels from the source table
# The label column (is_price_spike) does NOT exist in the source table,
# which is a Feature Store requirement
labels_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .select(
        "region_id",
        "settlement_date",
        when(col("rrp") > 300, 1).otherwise(0).alias("is_price_spike"),
    )
)

total_labels = labels_df.count()
spike_count = labels_df.filter("is_price_spike = 1").count()

print(f"Labels: {total_labels:,} rows")
print(f"Price spikes (RRP > $300): {spike_count:,} ({spike_count/total_labels*100:.1f}%)")
print(f"Non-spikes: {total_labels - spike_count:,} ({(total_labels - spike_count)/total_labels*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Create training set
# MAGIC
# MAGIC This is where the magic happens. The Feature Store:
# MAGIC 1. Takes our labeled DataFrame (entity keys + timestamps + target)
# MAGIC 2. Computes ALL features using only data available before each timestamp
# MAGIC 3. Returns a joined DataFrame ready for training
# MAGIC
# MAGIC **No manual window functions. No point-in-time join bugs.**

# COMMAND ----------

training_set = fe.create_training_set(
    df=labels_df,
    features=all_features,
    label="is_price_spike",
    exclude_columns=["region_id", "settlement_date"],
)

training_df = training_set.load_df()

print(f"Training set columns: {training_df.columns}")
print(f"Training set rows: {training_df.count():,}")

display(training_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Train XGBoost classifier
# MAGIC
# MAGIC Convert to pandas and train an XGBoost classifier for price spike detection.

# COMMAND ----------

import mlflow
import mlflow.xgboost
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline as SKPipeline

# Configure MLflow
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/feature_store_workshop"
mlflow.set_experiment(experiment_name)

# Drop _processed_at if present to avoid Arrow conversion error in toPandas()
if "_processed_at" in training_df.columns:
    training_df = training_df.drop("_processed_at")

# Convert to pandas
pdf = training_df.toPandas()

# Separate features and target
X = pdf.drop(columns=["is_price_spike"])
y = pdf["is_price_spike"]

# Train/test split (stratified to handle class imbalance)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {len(X_train):,} rows (spikes: {y_train.sum():,})")
print(f"Test set:     {len(X_test):,} rows (spikes: {y_test.sum():,})")
print(f"\nFeatures ({len(X.columns)}):")
for c in X.columns:
    print(f"  - {c}")

# COMMAND ----------

# Handle any nulls from window boundaries and train
model = SKPipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("classifier", xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1),
        random_state=42,
        use_label_encoder=False,
        eval_metric="logloss",
    )),
])

model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("Classification Report:")
print(classification_report(y_test, y_pred, target_names=["Normal", "Price Spike"]))

roc_auc = roc_auc_score(y_test, y_proba) if y_test.nunique() > 1 else 0.0
print(f"ROC-AUC: {roc_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Feature importance
# MAGIC
# MAGIC Which features matter most for predicting price spikes?

# COMMAND ----------

# Extract feature importance from the XGBoost classifier inside the pipeline
xgb_model = model.named_steps["classifier"]
importance_df = pd.DataFrame({
    "feature": X.columns,
    "importance": xgb_model.feature_importances_,
}).sort_values("importance", ascending=False)

print("Top features for price spike prediction:")
display(importance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Log model with feature lineage
# MAGIC
# MAGIC When we log the model using `fe.log_model()` instead of `mlflow.log_model()`,
# MAGIC Unity Catalog records the **lineage** between the model and its features.
# MAGIC
# MAGIC This means:
# MAGIC - Anyone can see which features a model was trained on
# MAGIC - `score_batch` automatically computes the right features at inference time
# MAGIC - Feature changes are tracked and auditable

# COMMAND ----------

model_name = f"{CATALOG}.{SCHEMA}.price_spike_classifier_nsw"

with mlflow.start_run(run_name="xgboost_price_spike_fs") as run:
    mlflow.log_params({
        "model_type": "xgboost",
        "target": "is_price_spike",
        "region": "NSW1",
        "feature_count": len(X.columns),
        "spike_threshold_dollars": 300,
        "n_features_demand": len(demand_features),
        "n_features_price": len(price_features),
        "n_features_supply": len(supply_features),
        "n_features_trend": len(trend_features),
    })

    mlflow.log_metrics({
        "roc_auc": roc_auc,
        "spike_precision": float(classification_report(y_test, y_pred, output_dict=True).get("1", {}).get("precision", 0)),
        "spike_recall": float(classification_report(y_test, y_pred, output_dict=True).get("1", {}).get("recall", 0)),
    })

    # Log with feature lineage - this is the key difference from mlflow.log_model()
    fe.log_model(
        model=model,
        artifact_path="price_spike_model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=model_name,
    )

    best_run_id = run.info.run_id

print(f"Model logged with feature lineage: {model_name}")
print(f"Run ID: {best_run_id}")

# COMMAND ----------

# Set champion alias
from mlflow import MlflowClient
client = MlflowClient()

# Get the latest version
versions = client.search_model_versions(f"name='{model_name}'")
latest_version = max(v.version for v in versions)

client.set_registered_model_alias(model_name, "champion", latest_version)
print(f"Model '{model_name}' version {latest_version} aliased as 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC **[Check Unity Catalog > Models to see the feature lineage graph]**
# MAGIC
# MAGIC The model page shows:
# MAGIC - Which features were used in training
# MAGIC - Which source tables the features came from
# MAGIC - Full data lineage from raw data to predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Batch scoring + materialization (10 min)
# MAGIC
# MAGIC ### 4.1 Score new data with `score_batch`
# MAGIC
# MAGIC The Feature Store **automatically computes features** at inference time.
# MAGIC You just provide a DataFrame with entity keys and timestamps - no feature
# MAGIC engineering code needed.

# COMMAND ----------

# Simulate new data arriving - just entity keys and timestamps
# In production this would be the latest streaming data
inference_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
    .filter("region_id = 'NSW1'")
    .select("region_id", "settlement_date")
    .orderBy(col("settlement_date").desc())
    .limit(500)
)

print(f"Inference data: {inference_df.count()} rows (latest 500 intervals)")

# COMMAND ----------

# score_batch automatically computes all features and applies the model
model_uri = f"models:/{model_name}@champion"
predictions_df = fe.score_batch(model_uri=model_uri, df=inference_df)

print(f"Predictions: {predictions_df.count()} rows")
print(f"Columns: {predictions_df.columns}")

display(
    predictions_df
    .orderBy("settlement_date", ascending=False)
    .limit(20)
)

# COMMAND ----------

# Summary of predictions
pred_summary = predictions_df.groupBy("prediction").count().collect()
for row in pred_summary:
    label = "Price Spike" if row["prediction"] == 1 else "Normal"
    print(f"  {label}: {row['count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Materialize features (optional)
# MAGIC
# MAGIC For production, you can **materialize** features to an offline store (Delta table)
# MAGIC with automatic scheduling. This pre-computes features so training sets load faster
# MAGIC and features stay fresh.
# MAGIC
# MAGIC ```python
# MAGIC # Uncomment to materialize features with hourly refresh
# MAGIC fe.materialize_features(
# MAGIC     features=all_features,
# MAGIC     offline_config=OfflineStoreConfig(
# MAGIC         catalog_name=CATALOG,
# MAGIC         schema_name=FS_SCHEMA,
# MAGIC         table_name_prefix="nem_energy_features"
# MAGIC     ),
# MAGIC     pipeline_state="ACTIVE",
# MAGIC     cron_schedule="0 0 * * * ?"  # Hourly
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This creates a Lakeflow pipeline that keeps features up to date automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | What we covered | Takeaway |
# MAGIC |-----------------|----------|
# MAGIC | **Declarative features** | Define *what* you want (avg demand over 1h), not *how* to compute it |
# MAGIC | **Point-in-time correctness** | No future data leaks - built into the API |
# MAGIC | **Feature lineage** | Unity Catalog tracks which features every model used |
# MAGIC | **Batch scoring** | `score_batch` auto-computes features at inference - no duplicate code |
# MAGIC | **Materialization** | Pre-compute and schedule features for production |
# MAGIC
# MAGIC ### Feature Store vs manual features
# MAGIC
# MAGIC | | Manual (workshops 02/03) | Feature Store (this workshop) |
# MAGIC |---|---|---|
# MAGIC | **Define features** | Inline pandas code | `fe.create_feature()` - declarative |
# MAGIC | **Point-in-time** | Your responsibility | Guaranteed by the API |
# MAGIC | **Training/serving skew** | Copy-paste risk | Same definition used everywhere |
# MAGIC | **Lineage** | None | Full lineage in Unity Catalog |
# MAGIC | **Reuse** | Copy code between notebooks | Share feature objects across teams |
# MAGIC | **Production** | Manual scheduling | `materialize_features()` with cron |
# MAGIC
# MAGIC ### When to use which approach
# MAGIC
# MAGIC - **Manual features** (workshops 02/03): Quick iteration, custom transformations,
# MAGIC   non-aggregation features (cyclical encoding, interaction terms)
# MAGIC - **Feature Store**: Production features, cross-team sharing, audit requirements,
# MAGIC   time-windowed aggregations, serving consistency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. **Add more features**: Weather features, interconnector patterns, seasonal offsets
# MAGIC 2. **Materialize for production**: Schedule hourly feature refresh
# MAGIC 3. **Online serving**: Serve features for real-time prediction endpoints
# MAGIC 4. **Cross-team sharing**: Load forecasting team reuses demand features from market modelling
# MAGIC
# MAGIC **Resources:**
# MAGIC - [Declarative feature engineering docs](https://docs.databricks.com/aws/en/machine-learning/feature-store/declarative-apis)
# MAGIC - [Feature Store overview](https://docs.databricks.com/aws/en/machine-learning/feature-store/concepts)
# MAGIC - MLflow docs: https://mlflow.org/docs/latest/index.html
