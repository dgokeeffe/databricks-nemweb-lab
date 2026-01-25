# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Educational 40-minute hands-on lab for experienced Databricks data engineers covering:
1. Custom PySpark data sources using Python Data Source API (DBR 15.4+/Spark 4.0)
2. Cluster right-sizing based on Spark UI metrics and DBU cost analysis
3. Real-time dashboard development with Databricks Apps

The main project code lives in the `databricks-nemweb-lab/` subdirectory.

## Common Commands

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

# Cleanup deployment
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
- `databricks-nemweb-lab/notebooks/` - Lab exercises (00-04)
- `databricks-nemweb-lab/solutions/` - Reference solutions (for instructors)
- `databricks-nemweb-lab/app/` - Databricks App dashboard
- `databricks-nemweb-lab/config/` - Cluster and pipeline JSON templates
- `databricks-nemweb-lab/docs/` - Lab documentation and guides

## NEMWEB Data Source

- Base URL: `https://www.nemweb.com.au/REPORTS/CURRENT/`
- Primary table: DISPATCHREGIONSUM (dispatch region summary)
- 5-minute granularity, 5 NEM regions (NSW1, QLD1, SA1, VIC1, TAS1)
- Data format: CSV within ZIP archives
- Rate-limited API - use exponential backoff retry

## Development Guidelines

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
