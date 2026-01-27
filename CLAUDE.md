# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Two separate labs in one repository:

1. **databricks-nemweb-lab/** - 40-minute hands-on lab for data engineers (custom PySpark data sources, Lakeflow pipelines, cluster optimization)
2. **databricks-nemweb-analyst-lab/** - 60-minute demo-only workshop (Genie, AI/BI, Power BI integration)

Most development occurs in `databricks-nemweb-lab/`.

## Common Commands

```bash
# Install dependencies (use uv, not pip)
uv pip install -r requirements.txt
cd databricks-nemweb-lab/src && uv pip install -e .

# Run tests
cd databricks-nemweb-lab && python -m pytest src/tests/ -v
python -m pytest databricks-nemweb-lab/src/tests/test_nemweb_utils.py -v          # single file
python -m pytest databricks-nemweb-lab/src/tests/test_nemweb_datasource.py::test_name -v  # single test

# Databricks Asset Bundles
databricks bundle validate --var="environment=dev"
databricks bundle deploy --var="environment=dev"
databricks bundle run nemweb_lab_workflow --target dev --var="environment=dev"
databricks bundle run nemweb_lab_solutions --target dev --var="environment=dev"  # instructor
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
| `src/nemweb_datasource_arrow.py` | Custom PySpark datasource (Python Data Source API). Three modes: Volume, Auto-download, HTTP. Uses PyArrow for Serverless compatibility. |
| `src/nemweb_utils.py` | HTTP fetch with retry (`MAX_RETRIES=3`, `REQUEST_TIMEOUT=30s`), CSV parsing, schema definitions |
| `src/nemweb_ingest.py` | Parallel downloads to UC Volumes, CURRENT/ARCHIVE URL handling |
| `src/nemweb_sink.py` | `PriceAlertWriter` (threshold alerts), `MetricsSink` (observability) |
| `pipelines/nemweb_pipeline.py` | Declarative pipeline with `@dp.table()` decorators from `pyspark.pipelines` |
| `app/app.py` | Dash dashboard: 5 NEM regions, 30s refresh, price thresholds ($100 warn, $300 alert, $1000 critical) |

### Key Directories

- `databricks-nemweb-lab/src/` - Core library (packaged as `nemweb_datasource`)
- `databricks-nemweb-lab/pipelines/` - Lakeflow pipeline definitions
- `databricks-nemweb-lab/exercises/` - Lab exercises (00-04)
- `databricks-nemweb-lab/solutions/` - Reference solutions (instructors)
- `databricks-nemweb-lab/app/` - Databricks App dashboard
- `databricks-nemweb-lab/docs/` - Lab documentation

## NEMWEB Data Source

- Base URL: `https://www.nemweb.com.au/REPORTS/CURRENT/`
- Primary table: DISPATCHREGIONSUM
- 5-minute granularity, 5 NEM regions (NSW1, QLD1, SA1, VIC1, TAS1)
- Data format: CSV within ZIP archives
- Rate-limited API - always use exponential backoff retry

## Development Guidelines

### Runtime Requirements
- Serverless (Environment Version 4) or DBR 15.4+ required
- Spark 4.0+ required - `spark.dataSource.register()` not available earlier
- All HTTP calls must implement retry logic with backoff

### Serverless Arrow Compatibility
The custom datasource uses a `_to_python_scalar()` function to ensure all values are pure Python types (`datetime.datetime`, not `pandas.Timestamp` or `numpy.datetime64`). This prevents Arrow fast path assertion errors on Serverless.

### Databricks Notebook Imports
For importing from local src folder in notebooks:
```python
import sys, os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = str(os.path.dirname(os.path.dirname(notebook_path)))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from nemweb_datasource_arrow import NemwebArrowDataSource
```

Import spark and dbutils from `databricks.sdk.runtime`:
```python
from databricks.sdk.runtime import spark, dbutils
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
- Runs `demo_workflow` job and cleans up after
- Requires `DEPLOY_NOTEBOOK_TOKEN` secret

## Bundle Configuration

Variables in `databricks.yml`:
- `catalog` (default: workspace)
- `schema` (default: nemweb_lab)
- `environment` (default: dev)

Wheel artifact built with: `uv build --wheel --out-dir dist`
