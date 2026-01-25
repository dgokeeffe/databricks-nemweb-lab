# Databricks NEMWEB Lab

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)
[![Python Data Source API](https://img.shields.io/badge/Python_Data_Source-DBR_15.4+-9B59B6?style=for-the-badge)](https://docs.databricks.com/en/pyspark/datasources.html)

A 40-minute hands-on lab for experienced Databricks data engineers covering custom PySpark data sources, Lakeflow pipelines, and performance optimization using Australian electricity market (NEMWEB) data.

## Overview

This lab teaches you to build production-grade data pipelines by:
1. Creating a **custom PySpark data source** using the Python Data Source API (DBR 15.4+/Spark 4.0)
2. Integrating with **Lakeflow Declarative Pipelines** for bronze/silver/gold architecture
3. Comparing **liquid clustering vs. partitioning** for time-series optimization
4. Building a **real-time dashboard** with Databricks Apps

### Data Source: AEMO NEMWEB

The Australian Energy Market Operator (AEMO) publishes real-time electricity market data via NEMWEB:
- 5-minute dispatch intervals
- 5 NEM regions: NSW1, QLD1, SA1, VIC1, TAS1
- Data includes demand, generation, pricing, and interconnector flows

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- DBR 15.4+ (Spark 4.0 required for Python Data Source API)
- Serverless compute recommended

## Quick Start

### Option 1: Deploy via Databricks UI

1. Clone this repository into your Databricks Workspace
2. Open the Asset Bundle Editor
3. Click "Deploy" to create the workflow
4. Navigate to Deployments and click "Run"

### Option 2: Deploy via CLI

```bash
# Validate configuration
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy --target dev

# Run the lab workflow
databricks bundle run nemweb_lab_workflow --target dev
```

## Lab Exercises

| Exercise | Description | Duration |
|----------|-------------|----------|
| **00 - Setup** | Validate environment, pre-load NEMWEB data | 5 min |
| **01 - Custom Source** | Build a PySpark data source for NEMWEB API | 15 min |
| **02 - Lakeflow Pipeline** | Create bronze/silver pipeline with expectations | 10 min |
| **03a - Cluster Sizing** | Analyze Spark UI metrics for right-sizing | 5 min |
| **03b - Optimization** | Compare liquid clustering vs. partitioning | 5 min |

## Project Structure

```
databricks-nemweb-lab/
├── databricks.yml              # Asset bundle configuration
├── databricks-nemweb-lab/
│   ├── src/                    # Core Python library
│   │   ├── nemweb_datasource.py   # Custom PySpark data source
│   │   ├── nemweb_utils.py        # HTTP utilities with retry logic
│   │   ├── nemweb_sink.py         # Custom sinks (alerts, metrics)
│   │   └── pipeline/              # Lakeflow pipeline definitions
│   ├── notebooks/              # Lab exercises (participants)
│   ├── solutions/              # Reference solutions (instructors)
│   ├── app/                    # Databricks App dashboard
│   └── docs/                   # Lab documentation
├── requirements.txt            # Python dependencies
└── env.example                 # Environment variable template
```

## Configuration

Copy `env.example` to `.env` and configure:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi_your_token_here
NEMWEB_CATALOG=main
NEMWEB_SCHEMA=nemweb_lab
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
cd databricks-nemweb-lab && python -m pytest src/tests/ -v

# Run dashboard locally
cd databricks-nemweb-lab/app && gunicorn app:server -b 0.0.0.0:8050
```

## Contributing

1. Clone locally: `git clone <repo-url>`
2. Test changes with Databricks CLI
3. Submit pull request with review

## Third-Party Licenses

| Package | License | Copyright |
|---------|---------|-----------|
| databricks-sdk | Apache 2.0 | Databricks, Inc. |
| pyspark | Apache 2.0 | Apache Software Foundation |
| dash | MIT | Plotly, Inc. |

---

&copy; 2025 Databricks, Inc. All rights reserved. Subject to [Databricks License](https://databricks.com/db-license-source).
