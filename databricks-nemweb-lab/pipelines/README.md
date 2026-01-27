# NEMWEB Production Pipelines

This folder contains production-ready Lakeflow Spark Declarative Pipeline definitions, isolated from exercise notebooks and transformation code.

## Contents

- **`nemweb_pipeline.py`** - Main pipeline definition with bronze/silver/gold medallion architecture
- **`spark-pipeline.yaml`** - Spark Declarative Pipeline configuration for local testing

## Usage

### Local Testing

```bash
cd pipelines
spark-pipelines dry-run    # Validate pipeline definitions
spark-pipelines run        # Run pipeline locally
```

### Databricks Deployment

The pipeline is deployed via Databricks Asset Bundles:

```bash
# Deploy pipeline
databricks bundle deploy --var="environment=dev"

# Run pipeline refresh job
databricks bundle run nemweb_pipeline_job --target dev --var="environment=dev"
```

## Pipeline Architecture

- **Bronze Layer** (`nemweb_bronze`): Raw NEMWEB data ingestion
- **Silver Layer** (`nemweb_silver`): Cleansed data with quality checks
- **Gold Layer** (`nemweb_gold_hourly`, `nemweb_gold_daily`): Aggregated metrics

## Notes

- This folder is separate from `notebooks/` which contains lab exercises
- The pipeline uses the `nemweb_datasource` package installed from `src/`
- Configuration is managed via `databricks.yml` in the repository root
