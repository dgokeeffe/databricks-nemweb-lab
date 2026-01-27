# Databricks NEMWEB Lab: Custom Data Sources & Query Optimization

[![DBR 15.4+](https://img.shields.io/badge/DBR-15.4%2B-orange)](https://docs.databricks.com/release-notes/runtime/15.4.html)
[![Spark 4.0](https://img.shields.io/badge/Spark-4.0-blue)](https://spark.apache.org/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-green)](https://www.python.org/)

A 40-minute hands-on lab for experienced Databricks data engineers covering:

1. **Custom PySpark Data Sources** - Building production-ready data sources using Python Data Source API
2. **Lakeflow Pipeline Integration** - Integrating custom sources into declarative pipelines
3. **Query Optimization** - Comparing liquid clustering vs. generated columns for time-series data

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   AEMO NEMWEB   │────▶│  Custom Data     │────▶│   Lakeflow      │
│   (HTTP/CSV)    │     │  Source (PySpark)│     │   Pipeline      │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                        ┌─────────────────────────────────┼─────────────────────────────────┐
                        │                                 ▼                                 │
                        │  ┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐   │
                        │  │   Bronze    │───▶│     Silver      │───▶│      Gold       │   │
                        │  │  (Raw Data) │    │ (Quality Rules) │    │ (Aggregations)  │   │
                        │  └─────────────┘    └─────────────────┘    └────────┬────────┘   │
                        │                      Unity Catalog                  │            │
                        └─────────────────────────────────────────────────────┼────────────┘
                                                                              │
                                                                              ▼
                                                                    ┌─────────────────┐
                                                                    │  Databricks App │
                                                                    │  (Dash Dashboard)│
                                                                    └─────────────────┘
```

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Databricks Runtime | 15.4 LTS+ | Required for Python Data Source API |
| Unity Catalog | Enabled | For managed tables and governance |
| Python | 3.10+ | Included in DBR 15.4+ |
| Databricks CLI | 0.200+ | For Asset Bundle deployment |

## Quick Start

### Option 1: Databricks Asset Bundles (Recommended)

```bash
# Clone the repository
git clone https://github.com/your-org/databricks-nemweb-lab.git
cd databricks-nemweb-lab

# Configure authentication
databricks auth login --host https://your-workspace.cloud.databricks.com

# Validate and deploy
databricks bundle validate
databricks bundle deploy --target dev

# Run the lab workflow
databricks bundle run nemweb_lab_workflow --target dev
```

### Option 2: Manual Import

1. Clone this repository to your Databricks workspace via Repos
2. Create a cluster with DBR 15.4+ and Unity Catalog access
3. Open `notebooks/00_setup_and_validation.py` to verify environment
4. Follow `docs/lab_instructions.md` for the exercises

## Lab Exercises

| Exercise | Duration | Description |
|----------|----------|-------------|
| 00 - Setup | 5 min | Validate environment, pre-load NEMWEB data |
| 01 - Custom Source | 15 min | Build a PySpark data source for NEMWEB |
| 02 - Lakeflow Pipeline | 10 min | Integrate into bronze/silver/gold layers |
| 03a - Cluster Sizing | 5 min | Analyze Spark UI metrics for right-sizing |
| 03b - Optimization | 5 min | Compare liquid clustering vs. partitioning |
| 04 - Fault Tolerance | Extension | Add retry logic and streaming recovery |

## Repository Structure

```
databricks-nemweb-lab/
├── src/                        # Core library
│   ├── nemweb_datasource.py    # Python Data Source API implementation
│   ├── nemweb_utils.py         # HTTP utilities with retry logic
│   ├── nemweb_sink.py          # Custom sinks for alerts/metrics
│   └── pipeline/               # Lakeflow pipeline definitions
├── notebooks/                  # Lab exercises
├── solutions/                  # Reference implementations
├── app/                        # Databricks App (Dash dashboard)
├── config/                     # Cluster and pipeline templates
└── docs/                       # Documentation and guides
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NEMWEB_DATA_SOURCE` | Data source mode (`sample` or `delta`) | `sample` |
| `NEMWEB_DELTA_TABLE` | Unity Catalog table for live data | - |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse for app queries | - |

### Spark Configuration

```python
# Recommended settings for NEMWEB workloads
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
```

## NEMWEB Data Source

[AEMO NEMWEB](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb) provides real-time Australian electricity market data:

| Field | Description |
|-------|-------------|
| `SETTLEMENTDATE` | Trading interval timestamp (5-min granularity) |
| `REGIONID` | NEM region: NSW1, QLD1, SA1, VIC1, TAS1 |
| `TOTALDEMAND` | Regional demand in MW |
| `RRP` | Regional reference price in $/MWh |
| `AVAILABLEGENERATION` | Available generation capacity in MW |

**API Endpoint:** `https://www.nemweb.com.au/REPORTS/CURRENT/`

## Databricks App

Deploy the real-time price dashboard:

```bash
# Using Databricks CLI
databricks apps create nemweb-prices
databricks apps deploy nemweb-prices --source-code-path ./app

# Or via Asset Bundles (if configured in databricks.yml)
databricks bundle deploy --target prod
```

**Features:**
- Live price cards for all 5 NEM regions
- Price spike alerts (>$300/MWh warning, >$1000/MWh critical)
- Demand trends and distribution charts
- Auto-refresh every 30 seconds

## Documentation

- [Lab Instructions](docs/lab_instructions.md) - Step-by-step exercise guide
- [Spark UI Metrics Guide](docs/spark_ui_metrics_guide.md) - Interpreting performance metrics
- [Sizing Worksheet](docs/sizing_worksheet.md) - Interactive cluster sizing workbook
- [Facilitator Guide](docs/facilitator_guide.md) - For instructors running the lab

### External References

- [Python Data Source API](https://docs.databricks.com/en/pyspark/datasources.html)
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/)
- [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
- [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/)
- [MMS Data Model Report](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report.htm)

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines and CLA requirements.

## License

Educational use only. NEMWEB data is provided by AEMO under their [terms of use](https://www.aemo.com.au/privacy-and-legal-notices/copyright-permissions).
