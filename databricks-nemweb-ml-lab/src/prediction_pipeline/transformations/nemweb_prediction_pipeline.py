from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

CATALOG = spark.conf.get("pipeline.catalog", "daveok")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

print(f"Prediction pipeline config: catalog={CATALOG}, schema={SCHEMA}")

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

@dp.materialized_view(
    name="demand_predictions",
    comment="NSW demand predictions from champion load_forecast_nsw model"
)
def demand_predictions():
    """Load the champion demand model, score silver data, return predictions.

    If the model doesn't exist yet (first run before workshop), returns
    an empty DataFrame with the correct schema.
    """
    import mlflow
    import pandas as pd
    from datetime import datetime
    from ml_features import create_load_forecast_features, LOAD_FORECAST_EXCLUDE_COLS

    model_name = f"{CATALOG}.{SCHEMA}.load_forecast_nsw"

    try:
        model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
    except Exception as e:
        print(f"Model {model_name}@champion not found ({e}), returning empty table")
        return spark.createDataFrame([], DEMAND_PREDICTIONS_SCHEMA)

    # Get model version from alias
    try:
        client = mlflow.MlflowClient()
        version_info = client.get_model_version_by_alias(model_name, "champion")
        model_version = int(version_info.version)
    except Exception:
        model_version = 0

    # Read silver data for NSW
    pdf = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
        .filter("region_id = 'NSW1'")
        .orderBy("settlement_date")
        .toPandas()
    )

    if pdf.empty:
        print("No silver data available, returning empty table")
        return spark.createDataFrame([], DEMAND_PREDICTIONS_SCHEMA)

    pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
    pdf = pdf.set_index("settlement_date").sort_index()

    # Feature engineering
    pdf_features = create_load_forecast_features(pdf)
    if pdf_features.empty:
        print("No rows after feature engineering, returning empty table")
        return spark.createDataFrame([], DEMAND_PREDICTIONS_SCHEMA)

    feature_cols = [c for c in pdf_features.columns if c not in LOAD_FORECAST_EXCLUDE_COLS]

    # Align columns and dtypes with model's expected schema.
    # MLflow signature enforcement is strict (for example int64 != int32).
    try:
        model_input_schema = model.metadata.get_input_schema()
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
                    pdf_features[col_name] = pdf_features[col_name].astype("float64")
            feature_cols = expected_cols
    except Exception:
        pass  # Fall back to using all non-excluded columns

    X = pdf_features[feature_cols]

    # Predict
    predictions = model.predict(X)

    # Build output
    scored_at = datetime.now()
    pred_df = pd.DataFrame({
        "settlement_date": X.index,
        "region_id": "NSW1",
        "actual_demand_mw": pdf_features["total_demand_mw"].values,
        "predicted_demand_mw": predictions.astype(float),
        "forecast_error_mw": (predictions - pdf_features["total_demand_mw"].values).astype(float),
        "model_version": model_version,
        "scored_at": scored_at,
    })

    print(f"Scored {len(pred_df):,} rows with model v{model_version}")
    print(f"MAE: {pred_df['forecast_error_mw'].abs().mean():.1f} MW")

    return spark.createDataFrame(pred_df, schema=DEMAND_PREDICTIONS_SCHEMA)

@dp.materialized_view(
    name="price_predictions",
    comment="NSW price predictions from champion price_forecast_nsw model"
)
def price_predictions():
    """Load the champion price model, score silver data, return predictions.

    If the model doesn't exist yet (first run before workshop), returns
    an empty DataFrame with the correct schema.
    """
    import mlflow
    import pandas as pd
    from datetime import datetime
    from ml_features import create_price_forecast_features, PRICE_FORECAST_EXCLUDE_COLS

    model_name = f"{CATALOG}.{SCHEMA}.price_forecast_nsw"

    try:
        model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
    except Exception as e:
        print(f"Model {model_name}@champion not found ({e}), returning empty table")
        return spark.createDataFrame([], PRICE_PREDICTIONS_SCHEMA)

    # Get model version from alias
    try:
        client = mlflow.MlflowClient()
        version_info = client.get_model_version_by_alias(model_name, "champion")
        model_version = int(version_info.version)
    except Exception:
        model_version = 0

    # Read silver data for NSW
    pdf = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_demand_weather")
        .filter("region_id = 'NSW1'")
        .orderBy("settlement_date")
        .toPandas()
    )

    if pdf.empty:
        print("No silver data available, returning empty table")
        return spark.createDataFrame([], PRICE_PREDICTIONS_SCHEMA)

    pdf["settlement_date"] = pd.to_datetime(pdf["settlement_date"])
    pdf = pdf.set_index("settlement_date").sort_index()

    # Feature engineering
    pdf_features = create_price_forecast_features(pdf)
    if pdf_features.empty:
        print("No rows after feature engineering, returning empty table")
        return spark.createDataFrame([], PRICE_PREDICTIONS_SCHEMA)

    feature_cols = [c for c in pdf_features.columns if c not in PRICE_FORECAST_EXCLUDE_COLS]

    # Align columns and dtypes with model's expected schema.
    # MLflow signature enforcement is strict (for example int64 != int32).
    try:
        model_input_schema = model.metadata.get_input_schema()
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
                    pdf_features[col_name] = pdf_features[col_name].astype("float64")
            feature_cols = expected_cols
    except Exception:
        pass  # Fall back to using all non-excluded columns

    X = pdf_features[feature_cols]

    # Predict
    predictions = model.predict(X)

    # Build output
    scored_at = datetime.now()
    pred_df = pd.DataFrame({
        "settlement_date": X.index,
        "region_id": "NSW1",
        "actual_rrp": pdf_features["rrp"].values,
        "predicted_rrp": predictions.astype(float),
        "forecast_error_dollars": (predictions - pdf_features["rrp"].values).astype(float),
        "model_version": model_version,
        "scored_at": scored_at,
    })

    print(f"Scored {len(pred_df):,} rows with model v{model_version}")
    print(f"MAE: ${pred_df['forecast_error_dollars'].abs().mean():.2f}/MWh")

    return spark.createDataFrame(pred_df, schema=PRICE_PREDICTIONS_SCHEMA)
