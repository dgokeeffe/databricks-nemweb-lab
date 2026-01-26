# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains two separate labs:

**1. databricks-nemweb-lab/** - 40-minute hands-on lab for experienced Databricks data engineers:
- Custom PySpark data sources using Python Data Source API (DBR 15.4+/Spark 4.0)
- Cluster right-sizing based on Spark UI metrics and DBU cost analysis
- Lakeflow Declarative Pipelines with bronze/silver/gold architecture
- Real-time dashboard development with Databricks Apps

**2. databricks-nemweb-anaylst-lab/** - 60-minute demo-only workshop for AGL Trading Analytics:
- Genie (natural language queries) and AI/BI Assistant demonstrations
- SQL metric views for semantic layer
- Power BI integration examples
- Excel/Power BI workflow mapping to Databricks

Most code and development work occurs in `databricks-nemweb-lab/`.

## Common Commands

### Setup and Installation
```bash
# Use Databricks virtual environment if available
# The virtual env path is in: $DATABRICKS_VIRTUAL_ENV

# Install root dependencies
pip install -r requirements.txt

# Install the nemweb_datasource package (for local development)
cd databricks-nemweb-lab/src && pip install -e .

# Or build as wheel
cd databricks-nemweb-lab/src && python -m build
```

### Running Tests
```bash
# Run all tests from project directory
cd databricks-nemweb-lab && python -m pytest src/tests/ -v

# Run single test file
python -m pytest databricks-nemweb-lab/src/tests/test_nemweb_utils.py -v

# Run specific test
python -m pytest databricks-nemweb-lab/src/tests/test_nemweb_datasource.py::test_name -v
```

### Lab Exercises
| Exercise | Description |
|----------|-------------|
| 00 - Setup | Validate environment, pre-load NEMWEB data |
| 01 - Custom Source | Build a PySpark data source for NEMWEB |
| 02 - Lakeflow Pipeline | Integrate into bronze/silver/gold layers |
| 03a - Cluster Sizing | Analyze Spark UI metrics for right-sizing |
| 03b - Optimization | Compare liquid clustering vs. partitioning |
| 04 - Fault Tolerance | (Extension) Add retry logic and streaming recovery |

### Databricks Asset Bundles
```bash
# Validate bundle configuration
databricks bundle validate --var="environment=dev"

# Deploy to Databricks
databricks bundle deploy --var="environment=dev"

# Run lab workflow
databricks bundle run nemweb_lab_workflow --target dev --var="environment=dev"

# Run solutions workflow (instructor)
databricks bundle run nemweb_lab_solutions --target dev --var="environment=dev"

# Run pipeline refresh job
databricks bundle run nemweb_pipeline_job --target dev --var="environment=dev"

# Cleanup deployment (interactive)
./scripts/cleanup.sh

# Or cleanup non-interactively
databricks bundle destroy --target dev --var="environment=dev"
```

### Databricks App (Dashboard)
```bash
# Deploy price dashboard app
databricks apps create nemweb-prices
databricks apps deploy nemweb-prices --source-code-path ./databricks-nemweb-lab/app

# Local development (from app directory)
cd databricks-nemweb-lab/app && pip install -r requirements.txt
gunicorn app:server -b 0.0.0.0:8050 --workers 2

# Run app tests
python -m pytest databricks-nemweb-lab/app/test_app.py -v
```

## Architecture

### Data Flow
```
NEMWEB API (HTTP/ZIP/CSV) → Custom Datasource → Lakeflow Pipeline (DLT)
  → Delta Table → Databricks App Dashboard
```

### Core Components

**src/nemweb_datasource.py** - Custom Spark datasource implementing Python Data Source API:
- `NemwebDataSource` - Main datasource registration class
- `NemwebDataSourceReader` - Manages partition planning and fetching
- `NemwebStreamReader` - Streaming reader with checkpoint recovery
- `NemwebPartition` - Represents data partitions for parallel reading

**src/nemweb_utils.py** - HTTP utilities with exponential backoff retry logic:
- HTTP fetch with `MAX_RETRIES=3`, `REQUEST_TIMEOUT=30s`
- CSV parsing with type conversion
- Schema definitions for NEMWEB tables

**src/nemweb_ingest.py** - File download utilities:
- Parallel file downloads to Unity Catalog Volumes
- Manages CURRENT and ARCHIVE URL structures
- Table-specific configurations (DISPATCHREGIONSUM, DISPATCHPRICE, etc.)

**src/nemweb_sink.py** - Custom sinks:
- `PriceAlertWriter` - Triggers alerts when prices exceed thresholds
- `MetricsDataSource` - Publishes metrics to observability endpoints

**src/pipeline/nemweb_pipeline.py** - Spark Declarative Pipeline definitions:
- Bronze layer: Raw NEMWEB ingestion with ingestion timestamp
- Silver layer: Data cleansing, type casting, and quality expectations
- Gold layer: Hourly aggregations
- Uses `@dp.table()` decorators from `pyspark.pipelines`

**app/app.py** - Dash dashboard with:
- Live price cards for 5 NEM regions (NSW1, QLD1, SA1, VIC1, TAS1)
- Auto-refresh every 30 seconds
- Price thresholds: Warning $100, Alert $300, Critical $1000/MWh

### Directory Structure
- `databricks-nemweb-lab/src/` - Core Python library (datasource, utils, sinks, pipeline)
  - Packaged as `nemweb_datasource` (see `setup.py` and `pyproject.toml`)
  - Installable via `pip install -e .` for local development
- `databricks-nemweb-lab/notebooks/` - Lab exercises (00-04)
- `databricks-nemweb-lab/solutions/` - Reference solutions (for instructors)
- `databricks-nemweb-lab/app/` - Databricks App dashboard
- `databricks-nemweb-lab/config/` - Cluster and pipeline JSON templates
- `databricks-nemweb-lab/docs/` - Lab documentation and guides
- `databricks-nemweb-lab/data/` - Static data files (NEM station registry, DUID mappings)
- `databricks-nemweb-anaylst-lab/` - Separate analyst/demo lab (SQL-focused)
- `scripts/` - Helper scripts (cleanup.sh)
- `.github/workflows/` - CI/CD workflows (databricks-ci.yml)

## NEMWEB Data Source

- Base URL: `https://www.nemweb.com.au/REPORTS/CURRENT/`
- Primary table: DISPATCHREGIONSUM (dispatch region summary)
- 5-minute granularity, 5 NEM regions (NSW1, QLD1, SA1, VIC1, TAS1)
- Data format: CSV within ZIP archives
- Rate-limited API - use exponential backoff retry

## Development Guidelines

### Version Control
- Commit and push changes regularly as you work through tasks
- Create meaningful commit messages that describe the changes made

### Lab Exercise Modifications
- Keep exercises achievable within 40-minute window
- Use clear TODO markers for participant tasks
- Provide hints but don't give away solutions
- Ensure solutions in `solutions/` match exercise structure exactly
- Make sure to import spark and dbutils from databricks.sdk.runtime

### Testing
- Mock all external HTTP calls (no live NEMWEB API dependency)
- Use pytest fixtures from `conftest.py` for reusable test data
- Tests must pass without Spark cluster (unit tests with mocks)

### Constraints
- Serverless (Environment Version 4) or DBR 15.4+ required for Python Data Source API
- Spark 4.0+ required - `spark.dataSource.register()` not available in earlier versions
- All HTTP calls must implement retry logic with backoff

### Local Development
- Use `nemweb_local.py` with DuckDB for quick iteration (no Spark required)
- Use `local_spark_iceberg.py` for Spark + Iceberg testing (limited - no DataSource API)
- Full DataSource API testing requires Databricks Serverless or DBR 15.4+ cluster
- When working in Databricks environment, use the virtual env at `$DATABRICKS_VIRTUAL_ENV`

## CI/CD

The repository includes GitHub Actions workflow (`.github/workflows/databricks-ci.yml`):
- Validates and deploys bundles on PR and main branch pushes
- Creates/reuses serverless SQL warehouse for testing
- Runs `demo_workflow` job and cleans up after
- Requires `DEPLOY_NOTEBOOK_TOKEN` secret configured in GitHub

## Package Management

The `src/` directory is configured as a Python package:
- `setup.py` - Package metadata for pip install
- `pyproject.toml` - Modern Python packaging with UV lock file
- `uv.lock` - Dependency lock file (managed by uv tool)
- Both package and standalone imports supported in notebooks
