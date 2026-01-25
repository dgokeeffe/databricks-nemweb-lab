# Lab Instructions: NEMWEB Custom Data Source & Cluster Sizing

**Duration:** 40 minutes
**Audience:** Experienced Databricks Data Engineers

## Overview

This hands-on lab teaches two advanced Databricks skills:

1. **Custom PySpark Data Sources** (20 min) - Build production-ready data sources using the Python Data Source API
2. **Cluster Right-Sizing** (15 min) - Make data-driven sizing decisions based on Spark UI metrics
3. **Setup & Validation** (5 min) - Environment check

## Prerequisites

- Databricks workspace with DBR 15.4+ cluster
- Basic PySpark and Lakeflow knowledge
- Internet access to nemweb.com.au

## Lab Schedule

| Time | Activity | Notebook |
|------|----------|----------|
| 0:00-0:05 | Environment setup | `00_setup_and_validation.py` |
| 0:05-0:20 | Exercise 1: Custom data source | `01_custom_source_exercise.py` |
| 0:20-0:30 | Exercise 2: Lakeflow pipeline | `02_lakeflow_pipeline.py` |
| 0:30-0:40 | Exercise 3: Cluster sizing | `03_cluster_sizing_exercise.py` |
| Extension | Exercise 4: Fault tolerance | `04_fault_tolerance_exercise.py` |

## Getting Started

### Step 1: Import Lab Materials

1. Clone or download the lab repository
2. Import to your Databricks workspace:
   - Workspace > Import > Upload ZIP or Git URL
   - Or use Repos to clone directly

### Step 2: Create a Cluster

1. Go to Compute > Create Cluster
2. Select DBR 15.4 or later (required for Python Data Source API)
3. Use the template in `config/cluster_template.json` or:
   - Node type: m5.xlarge (or equivalent)
   - Workers: 1-4 (autoscale)
   - Photon enabled (recommended)

### Step 3: Run Setup Validation

1. Open `notebooks/00_setup_and_validation.py`
2. Attach to your cluster
3. Run All cells
4. Verify all checks pass

## Exercise 1: Custom Data Source (15 min)

**Goal:** Implement a custom PySpark data source for NEMWEB data.

### Tasks

1. **Schema Definition** (TODO 1.1)
   - Complete the schema for DISPATCHREGIONSUM table
   - Reference: MMS Data Model Report

2. **Partition Planning** (TODO 1.2)
   - Create partitions for parallel reading
   - One partition per NEM region

3. **Data Source Class** (TODO 1.3)
   - Complete the NemwebDataSource class
   - Register and test with Spark

### Key Concepts

- `DataSource`: Main entry point, defines format name
- `DataSourceReader`: Plans partitions, reads data
- `InputPartition`: Unit of parallel work

### Success Criteria

- Schema has 12+ fields including TOTALDEMAND, AVAILABLEGENERATION
- Partitions created for specified regions
- Data source reads sample data successfully

## Exercise 2: Lakeflow Pipeline (10 min)

**Goal:** Integrate the custom data source into a medallion architecture pipeline.

### Tasks

1. **Bronze Layer** (TODO 2.1)
   - Configure custom data source read
   - Add ingestion timestamp

2. **Data Quality** (TODO 2.2)
   - Add expectations for valid data
   - Drop invalid rows

3. **Gold Aggregation** (TODO 2.3)
   - Implement hourly aggregations

### Key Concepts

- `@dlt.table`: Declares a managed table
- `@dlt.expect_or_drop`: Quality expectation that drops failures
- `dlt.read()`: Read from upstream tables

### Success Criteria

- Bronze table ingests raw data
- Silver table applies quality checks
- Gold table produces hourly aggregations

## Exercise 3: Cluster Sizing (15 min)

**Goal:** Apply data-driven methodology to size clusters correctly.

### Tasks

1. **Workload Analysis** (TODO 3.1)
   - Calculate partition requirements
   - Understand task parallelism

2. **Core Calculation** (TODO 3.2)
   - Determine minimum cores from task metrics
   - Add overhead buffer

3. **Instance Selection** (TODO 3.3)
   - Rate workload characteristics
   - Select appropriate instance family

4. **Autoscaling** (TODO 3.4)
   - Set min/max workers
   - Based on typical vs peak patterns

5. **Cost Estimation** (TODO 3.5)
   - Calculate DBU consumption
   - Estimate monthly costs

### Key Concepts

- Partitions → Parallelism → Cores
- CPU vs Memory vs IO workload profiles
- DBU consumption = workers × DBU/hour × duration

### Success Criteria

- Justified partition count calculation
- Instance type matches workload profile
- Autoscaling bounds are reasonable (not 1-32)
- Cost estimate completed

## Exercise 4: Fault Tolerance (Extension, 20 min)

**Goal:** Implement production-ready failure recovery patterns.

### Tasks

1. **Retry with Backoff** (TODO 4.1)
   - Implement exponential backoff for transient errors
   - Skip retry for 404 (data doesn't exist)

2. **Checkpoint Recovery** (TODO 4.2)
   - Track completed partitions in Delta table
   - Skip already-completed work on resume

3. **Streaming Configuration** (TODO 4.3)
   - Configure streaming with automatic offset management
   - Understand recovery behavior

4. **Dead Letter Queue** (TODO 4.4)
   - Route bad records to DLQ
   - Preserve error metadata for investigation

### Key Concepts

- Idempotency: Operations must be safe to re-run
- Streaming offsets: Spark manages checkpoints automatically
- DLQ: Don't lose visibility into data issues

### Success Criteria

- Retry logic uses exponential backoff
- Re-running batch skips completed partitions
- Bad records route to DLQ with error info

## Solutions

Reference solutions are in the `solutions/` folder. Try to complete exercises first!

## Troubleshooting

### "Python Data Source API not available"
- Ensure cluster runs DBR 15.4+
- Restart cluster after runtime change

### "Cannot connect to NEMWEB"
- Check internet access
- Verify firewall allows HTTPS to nemweb.com.au

### "Import errors"
- Verify src/ path is in Python path
- Use inline code fallback in notebooks

## Additional Resources

- [Python Data Source API Documentation](https://docs.databricks.com/en/pyspark/datasources.html)
- [Lakeflow (DLT) Documentation](https://docs.databricks.com/en/delta-live-tables/)
- [Cluster Sizing Best Practices](https://docs.databricks.com/en/clusters/cluster-config-best-practices.html)
- [AEMO NEMWEB Portal](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb)
