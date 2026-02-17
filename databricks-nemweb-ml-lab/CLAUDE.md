# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See the parent `../CLAUDE.md` for broader project context (NEMWEB data source, supported tables, bundle engine, SVLS 4 details).

## What this is

ML workshop lab for Australian energy market (AEMO NEMWEB) data - load forecasting and market modelling. Follows the [Lakeflow Pipelines Python project structure](https://github.com/databricks/bundle-examples/tree/main/lakeflow_pipelines_python). This is a **standalone bundle** - deploy from this directory, not the repo root.

## Commands

```bash
# Run tests (no Spark required)
uv run python -m pytest tests/ -v
uv run python -m pytest tests/test_ml_features.py -v -k "test_cyclical"  # single test

# Deploy bundle (always from this directory)
databricks bundle deploy --var="environment=dev" --profile daveok

# First deploy on fresh clone (no existing state)
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t dev --profile daveok

# Validate
databricks bundle validate --var="environment=dev" --profile daveok

# Run jobs (streaming must run first to populate tables)
databricks bundle run streaming_pipeline_job --target dev --var="environment=dev" --profile daveok
databricks bundle run load_forecast_workshop --target dev --var="environment=dev" --profile daveok
databricks bundle run market_modelling_workshop --target dev --var="environment=dev" --profile daveok
databricks bundle run prediction_pipeline_job --target dev --var="environment=dev" --profile daveok

# App
databricks bundle run nem_forecast --target dev --var="environment=dev" --profile daveok

# Clean up
databricks bundle destroy --target dev --var="environment=dev" --profile daveok
```

## Architecture

```
NEMWEB + BOM APIs
    |
    |  streaming_pipeline (continuous, custom data source)
    v
Bronze/Silver/Gold streaming tables (14 tables)
    |
    |  workshops (interactive notebooks)
    v
UC Models with @champion alias
    |
    |  prediction_pipeline (batch, reads champion models)
    v
demand_predictions / price_predictions tables
    |
    |  nem_forecast (Dash app)
    v
Dashboard with live/demo mode
```

### Three pipelines

| Pipeline | Trigger | Source | Purpose |
|----------|---------|--------|---------|
| `streaming_pipeline` | Continuous | Custom NEMWEB + BOM data sources | 14 bronze/silver/gold tables via `readStream` |
| `ml_pipeline` | Batch (full refresh) | Auto Loader from UC Volumes CSVs | Bronze/silver/gold for offline ML training |
| `prediction_pipeline` | Batch | Champion models from UC Model Registry | Scores `demand_predictions` and `price_predictions` |

### Key code patterns

**Pipeline files** (`src/`) are raw Python (not notebooks). They use Lakeflow Declarative Pipelines API:
```python
from pyspark import pipelines as dp
from databricks.sdk.runtime import spark, dbutils

CATALOG = spark.conf.get("pipeline.catalog", "daveok")
SCHEMA = spark.conf.get("pipeline.schema", "ml_workshops")

@dp.table(name="my_table", comment="...")
@dp.expect_or_drop("valid_demand", "TOTALDEMAND > 0")
def my_table():
    return spark.readStream.format("nemweb_stream").option("table", "DISPATCHREGIONSUM").load()
```

Use `@dp.table` for streaming, `@dp.materialized_view` for batch/aggregation.

**Workshop files** (`workshops/`) are Databricks notebooks with `# COMMAND ----------` markers. They accept widget parameters:
```python
dbutils.widgets.text("catalog", "daveok")
CATALOG = dbutils.widgets.get("catalog")
```

**Glob-based loading**: Pipeline YAMLs use `include: ../src/streaming_pipeline/**`. File `register_datasource.py` loads before `transformations/` (alphabetical order) - this ensures the custom data source is registered before any streaming reads.

**Champion model pattern** (prediction pipeline):
```python
model = mlflow.pyfunc.load_model(f"models:/{CATALOG}.{SCHEMA}.{model_name}@champion")
```
Returns empty DataFrame with correct schema if model doesn't exist yet (graceful degradation).

**Workshop isolation**: Workshops write to `*_workshop` shadow tables; the prediction pipeline writes to production tables `demand_predictions` / `price_predictions`. This keeps the app stable during demos.

### Feature engineering

`src/prediction_pipeline/transformations/ml_features.py` is the single source of truth for feature engineering. Two functions: `create_load_forecast_features()` and `create_price_forecast_features()`. Both take a pandas DataFrame with DatetimeIndex. Features include cyclical time encoding (sin/cos), adaptive lag features (only computes lags feasible for available history), and rolling statistics.

Workshops 02 and 03 duplicate these functions inline for readability, but tests and the prediction pipeline import from `ml_features.py`.

### App

`app/app.py` is a Plotly Dash app with 4 tabs. Connects to SQL warehouse via `databricks-sql-connector`. Falls back to synthetic sample data if warehouse is unavailable (DEMO mode vs LIVE mode). Shows data provenance badge: MODEL (green), AEMO_FALLBACK (amber), or SAMPLE (red).

## Testing

Tests in `tests/` validate feature engineering only (no Spark dependency). Fixtures in `conftest.py` provide 576 rows of synthetic 5-min NSW1 dispatch data with realistic diurnal patterns. Tests use `sys.path.insert` to import `ml_features` directly from `src/prediction_pipeline/transformations/`.

## Bundle variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `catalog` | `daveok` | Unity Catalog catalog |
| `schema` | `ml_workshops` | Schema for all tables and models |
| `environment` | `dev` | Target environment |
| `warehouse_id` | `e4082fdb7ea19a15` | SQL warehouse for the Dash app |

## Workshop run order

1. `streaming_pipeline_job` - populates bronze/silver/gold tables (run first, takes a few minutes)
2. `load_forecast_workshop` or `market_modelling_workshop` - trains models, registers with `@champion` alias
3. `prediction_pipeline_job` - scores champion models into prediction tables
4. `nem_forecast` app - reads prediction tables

For offline/batch training path: `ml_workshop_data_setup` (downloads CSVs + runs ML pipeline) then `ml_workshop`.

## Failure recovery

- **Prediction table conflicts**: `DROP TABLE {catalog}.{schema}.demand_predictions` then re-run prediction pipeline
- **MLflow experiment path**: Workshops try job-supplied path first, fall back to `/Users/{current_user}/...`
- **Pipeline update conflicts**: Full refresh the pipeline from Databricks UI
- **App shows DEMO mode**: Verify warehouse ID and that prediction tables exist with data
