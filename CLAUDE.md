# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Three main components plus workshops:

1. **nemweb-data-source/** - Custom PySpark Data Source for AEMO NEMWEB data (packaged as wheel)
2. **nemweb-standalone/** - Pure Python utility for fetching NEMWEB data (no Spark required, stdlib only)
3. **databricks-nemweb-lab/** - 40-minute hands-on lab for data engineers (exercises, solutions, pipelines)
4. **databricks-nemweb-lab/workshops/** - 4-module ML workshop series for Load Forecasting and Market Modelling teams
5. **databricks-nemweb-analyst-lab/** - 60-minute demo-only workshop for trading analytics teams (Genie, AI/BI, Power BI)

Bundle config (`databricks.yml`) lives at repository root.

## Common Commands

```bash
# Install data source package (use uv, not pip)
cd nemweb-data-source && uv pip install -e .

# Run data source tests
cd nemweb-data-source && uv run python -m pytest tests/ -v
uv run python -m pytest tests/test_nemweb_utils.py -v          # single file

# Run standalone utility tests
cd nemweb-standalone && uv run python -m pytest test_nemweb.py -v

# Databricks Asset Bundles
databricks bundle validate --var="environment=dev"
databricks bundle deploy --var="environment=dev"
databricks bundle run nemweb_lab_workflow --target dev --var="environment=dev"
databricks bundle run nemweb_lab_solutions --target dev --var="environment=dev"  # instructor
databricks bundle run ml_workshops --target dev --var="environment=dev"          # ML workshop series
databricks bundle run workshop_data_setup --target dev --var="environment=dev"   # Load NEMWEB data for ML workshops
databricks bundle destroy --target dev --var="environment=dev"

# Dashboard app (local development)
cd databricks-nemweb-lab/app && gunicorn app:server -b 0.0.0.0:8050 --workers 2
python -m pytest databricks-nemweb-lab/app/test_app.py -v

# Validate pipeline locally
cd databricks-nemweb-lab/pipelines && spark-pipelines dry-run
```

## Architecture

```
NEMWEB API (HTTP/ZIP/CSV) → Custom Datasource → Lakeflow Pipeline (Bronze/Silver/Gold) → Delta Table → Dash App
```

### Core Components

| File | Purpose |
|------|---------|
| `nemweb-data-source/nemweb_datasource_arrow.py` | Custom PySpark datasource (Python Data Source API). Three modes: Volume, Auto-download, HTTP. Uses PyArrow for Serverless compatibility. |
| `nemweb-data-source/nemweb_datasource_stream.py` | Streaming variant of the datasource |
| `nemweb-data-source/nemweb_utils.py` | HTTP fetch with retry (`MAX_RETRIES=3`, `REQUEST_TIMEOUT=30s`), CSV parsing, schema definitions |
| `nemweb-data-source/nemweb_ingest.py` | Parallel downloads to UC Volumes, CURRENT/ARCHIVE URL handling |
| `nemweb-data-source/nemweb_sink.py` | `PriceAlertWriter` (threshold alerts), `MetricsSink` (observability) |
| `nemweb-data-source/nemweb_local.py` | DuckDB-based local development (no Spark required) |
| `nemweb-data-source/nemweb_dispatch.py` | Dispatch table operations and transformations |
| `nemweb-standalone/nemweb.py` | Standalone Python loader - stdlib only, no Spark required |
| `databricks-nemweb-lab/pipelines/nemweb_pipeline.py` | Declarative pipeline with `@dp.table()` decorators from `pyspark.pipelines` |
| `databricks-nemweb-lab/app/app.py` | Dash dashboard: 5 NEM regions, 30s refresh, price thresholds ($100 warn, $300 alert, $1000 critical) |

### Key Directories

- `nemweb-data-source/` - Custom PySpark Data Source (packaged as `nemweb_datasource` wheel)
- `nemweb-standalone/` - Pure Python utility for quick data access (no dependencies)
- `databricks-nemweb-lab/pipelines/` - Lakeflow pipeline definitions
- `databricks-nemweb-lab/exercises/` - Lab exercises (00-04)
- `databricks-nemweb-lab/solutions/` - Reference solutions (instructors)
- `databricks-nemweb-lab/workshops/` - ML workshop modules (00-04: data setup, platform, MLflow, serving, GenAI)
- `databricks-nemweb-lab/app/` - Databricks App dashboard
- `databricks-nemweb-lab/artifacts/` - Built wheel copied here for bundle deployment

## NEMWEB Data Source

- Base URL: `https://www.nemweb.com.au/REPORTS/CURRENT/`
- Data format: CSV within ZIP archives
- Rate-limited API - always use exponential backoff retry

### Supported Tables

| Table | Source Folder | Granularity | Description |
|-------|---------------|-------------|-------------|
| `DISPATCHREGIONSUM` | DispatchIS_Reports | 5-min | Regional demand, generation, interchange |
| `DISPATCHPRICE` | DispatchIS_Reports | 5-min | Regional spot prices (RRP) |
| `TRADINGPRICE` | TradingIS_Reports | 30-min | Trading period prices |
| `DISPATCH_UNIT_SCADA` | Dispatch_SCADA | 5-min | Real-time unit generation (per DUID) |
| `ROOFTOP_PV_ACTUAL` | ROOFTOP_PV/ACTUAL | 5-min | Rooftop solar generation estimates |
| `DISPATCH_REGION` | Dispatch_Reports | 5-min | Comprehensive dispatch with FCAS prices |
| `DISPATCH_INTERCONNECTOR` | Dispatch_Reports | 5-min | Interconnector dispatch details |
| `DISPATCH_INTERCONNECTOR_TRADING` | Dispatch_Reports | 5-min | Metered interconnector flows |

### Usage

```python
spark.dataSource.register(NemwebArrowDataSource)

# Regional dispatch data
df = spark.read.format("nemweb_arrow").option("table", "DISPATCHREGIONSUM").load()

# Unit-level generation (all power stations)
df = spark.read.format("nemweb_arrow").option("table", "DISPATCH_UNIT_SCADA").load()

# 5-minute price forecasts
df = spark.read.format("nemweb_arrow").option("table", "P5MIN_REGIONSOLUTION").load()
```

## ML Workshops

Four-module series for Load Forecasting and Market Modelling teams:

| Module | File | Topics |
|--------|------|--------|
| 00 | `workshops/00_workshop_data_setup.py` | Load NEMWEB data for ML training |
| 01 | `workshops/01_platform_and_pipelines.py` | Databricks platform, Unity Catalog, Lakeflow |
| 02 | `workshops/02_ml_workflows_mlflow.py` | MLflow tracking, experiments, model registry |
| 03 | `workshops/03_serving_and_monitoring.py` | Model serving endpoints, monitoring |
| 04 | `workshops/04_advanced_ml_genai.py` | Feature Store, AutoML, GenAI integration |

Workshops use `ml_workshops` schema and require the `nemweb_datasource` wheel plus ML libraries (xgboost, lightgbm, prophet).

## Development Guidelines

### Runtime Requirements
- Serverless (Environment Version 4) or DBR 15.4+ required
- Spark 4.0+ required - `spark.dataSource.register()` not available earlier
- All HTTP calls must implement retry logic with backoff

### Serverless Arrow Compatibility
The custom datasource uses a `_to_python_scalar()` function to ensure all values are pure Python types (`datetime.datetime`, not `pandas.Timestamp` or `numpy.datetime64`). This prevents Arrow fast path assertion errors on Serverless.

### Databricks Notebook Imports
For importing from nemweb-data-source in notebooks:
```python
import sys, os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/nemweb-data-source")

from nemweb_datasource_arrow import NemwebArrowDataSource
```

Import spark and dbutils from `databricks.sdk.runtime`:
```python
from databricks.sdk.runtime import spark, dbutils
```

### Standalone Utility
For quick data access without Spark (e.g., in ML notebooks):
```python
# Copy nemweb-standalone/nemweb.py or add to path
from nemweb import fetch, list_tables, NEM_REGIONS

data = fetch("DISPATCHPRICE", hours=1)  # Returns list[dict]
df = fetch("DISPATCHPRICE", hours=1, as_pandas=True)  # Returns DataFrame
```

### Lab Exercise Modifications
- Keep exercises achievable within 40-minute window
- Use clear TODO markers for participant tasks
- Provide hints but don't give away solutions
- Ensure solutions in `solutions/` match exercise structure exactly

### Testing
- Mock all external HTTP calls (no live NEMWEB API dependency)
- Use pytest fixtures from `conftest.py` for reusable test data
- Tests must pass without Spark cluster

### Local Development
- `nemweb_local.py` with DuckDB for quick iteration (no Spark required)
- Full DataSource API testing requires Databricks Serverless or DBR 15.4+ cluster

## CI/CD

GitHub Actions workflow (`.github/workflows/databricks-ci.yml`):
- Validates and deploys bundles on PR and main branch pushes
- Runs `demo_workflow` job and cleans up after (**Note**: `demo_workflow` not in current bundle - may need updating)
- Requires `DEPLOY_NOTEBOOK_TOKEN` secret
- Target workspace: `e2-demo-field-eng.cloud.databricks.com`

## Bundle Configuration

Variables in `databricks.yml`:
- `catalog` (default: workspace)
- `schema` (default: nemweb_lab)
- `environment` (default: dev)

Wheel artifact built from `nemweb-data-source/` with: `uv build --wheel --out-dir dist`

Package name: `nemweb-datasource` (current version in `nemweb-data-source/pyproject.toml`)
