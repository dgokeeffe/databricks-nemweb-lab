# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3: Cluster Right-Sizing Analysis
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll analyze Spark UI metrics and workload characteristics
# MAGIC to make data-driven cluster sizing decisions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Interpret Spark UI metrics (CPU%, memory%, task duration)
# MAGIC 2. Calculate required compute from data volume → partitions → cores
# MAGIC 3. Select appropriate instance types based on workload
# MAGIC 4. Set autoscaling boundaries based on patterns
# MAGIC 5. Estimate DBU costs for different configurations
# MAGIC
# MAGIC ## The Problem
# MAGIC
# MAGIC Many engineers size clusters by intuition ("let's try 16 cores"). This leads to:
# MAGIC - 50-300% over-provisioning (wasted DBUs)
# MAGIC - Undersized memory causing spill to disk
# MAGIC - Wide autoscaling (min=1, max=32) allowing runaway costs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario: NEMWEB Production Pipeline
# MAGIC
# MAGIC You're deploying the NEMWEB pipeline to production. Requirements:
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Data volume | 6 months historical + daily incremental |
# MAGIC | Historical load | ~50-100 MB (260k rows) |
# MAGIC | Daily incremental | ~1 MB (1,440 rows per region × 5 regions) |
# MAGIC | SLA | Process within 15 minutes of data availability |
# MAGIC | Budget | Minimize DBU spend while meeting SLA |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Understand Your Workload (5 minutes)
# MAGIC
# MAGIC ### TODO 3.1: Analyze the workload characteristics

# COMMAND ----------

# Workload parameters
HISTORICAL_ROWS = 260_000
HISTORICAL_SIZE_MB = 100
DAILY_ROWS = 7_200  # 288 intervals × 5 regions × 5 (days buffer)
DAILY_SIZE_MB = 3

# Spark defaults
DEFAULT_PARTITION_SIZE_MB = 128
CORES_PER_PARTITION = 1  # One task per core

# TODO 3.1a: Calculate optimal partition count for historical load
# Formula: partitions = data_size_mb / target_partition_size_mb
# Target partition size: 64-128 MB for balanced parallelism

target_partition_size_mb = 64  # Smaller partitions for better parallelism
historical_partitions = None  # YOUR CALCULATION HERE

print(f"Historical data: {HISTORICAL_SIZE_MB} MB")
print(f"Recommended partitions: {historical_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark UI Metrics Reference
# MAGIC
# MAGIC | Metric | Target Range | Action if Outside Range |
# MAGIC |--------|--------------|------------------------|
# MAGIC | CPU Utilization | 70-80% | <50%: reduce cores; >90%: add cores |
# MAGIC | Memory Utilization | 60-70% | <40%: use smaller instances; >80%: add memory |
# MAGIC | Task Duration | 30s-5min | <10s: increase partition size; >10min: add partitions |
# MAGIC | Shuffle Spill | 0 | Any spill: add memory or reduce partition size |
# MAGIC | GC Time | <10% of task time | High GC: add memory, check for data skew |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Core Requirements Calculation (5 minutes)
# MAGIC
# MAGIC ### TODO 3.2: Calculate minimum cores needed

# COMMAND ----------

# Timing requirements
SLA_MINUTES = 15
TARGET_COMPLETION_MINUTES = 10  # Leave buffer for variability

# Measured task metrics (from test run)
# These would come from Spark UI in production
AVG_TASK_DURATION_SECONDS = 45
TASK_COUNT = 10  # Number of partitions

# TODO 3.2a: Calculate required parallelism
# Formula: total_task_time = task_count × avg_task_duration_seconds
# Required cores = total_task_time / (target_completion_minutes × 60)

total_task_time_seconds = None  # YOUR CALCULATION HERE
required_cores = None  # YOUR CALCULATION HERE

print(f"Total task time: {total_task_time_seconds} seconds")
print(f"Target completion: {TARGET_COMPLETION_MINUTES} minutes")
print(f"Minimum cores required: {required_cores}")

# TODO 3.2b: Add overhead for Spark driver, executors startup, etc.
# Rule of thumb: Add 20% overhead

cores_with_overhead = None  # YOUR CALCULATION HERE
print(f"Cores with overhead: {cores_with_overhead}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Instance Type Selection
# MAGIC
# MAGIC ### AWS Instance Comparison (example)
# MAGIC
# MAGIC | Instance | vCPUs | Memory | DBU/hr | Use Case |
# MAGIC |----------|-------|--------|--------|----------|
# MAGIC | m5.xlarge | 4 | 16 GB | 0.75 | General purpose |
# MAGIC | m5.2xlarge | 8 | 32 GB | 1.5 | General purpose |
# MAGIC | c5.2xlarge | 8 | 16 GB | 1.0 | CPU-intensive |
# MAGIC | r5.2xlarge | 8 | 64 GB | 2.0 | Memory-intensive |
# MAGIC
# MAGIC ### TODO 3.3: Select appropriate instance type

# COMMAND ----------

# Workload characteristics assessment
# Score 1-5 for each dimension

# TODO 3.3: Rate your workload (1=low, 5=high)

cpu_intensity = None  # HTTP parsing, CSV processing → typically 2-3
memory_intensity = None  # Wide tables, large aggregations → depends on transforms
io_intensity = None  # Network calls to NEMWEB → typically 3-4

# Decision matrix
def recommend_instance_type(cpu: int, memory: int, io: int) -> str:
    """
    Recommend instance type based on workload characteristics.
    """
    if memory >= 4:
        return "r5.xlarge (memory-optimized) - Large aggregations/wide tables"
    elif cpu >= 4:
        return "c5.xlarge (CPU-optimized) - Heavy computation"
    else:
        return "m5.xlarge (general purpose) - Balanced workload"

# Uncomment after setting scores:
# recommendation = recommend_instance_type(cpu_intensity, memory_intensity, io_intensity)
# print(f"Recommended instance: {recommendation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Autoscaling Configuration
# MAGIC
# MAGIC ### Common Autoscaling Mistakes
# MAGIC
# MAGIC 1. **Too wide**: min=1, max=32 allows runaway costs
# MAGIC 2. **Too narrow**: min=max=8 prevents optimization
# MAGIC 3. **Wrong baseline**: min too low causes slow startup
# MAGIC
# MAGIC ### TODO 3.4: Configure autoscaling bounds

# COMMAND ----------

# Workload patterns (from analysis)
TYPICAL_LOAD_CORES = 4  # Daily incremental
PEAK_LOAD_CORES = 8  # Historical backfill or catch-up

# TODO 3.4: Set autoscaling bounds
# Rules:
# - min_workers: Handle typical load with ~70% utilization
# - max_workers: Handle peak load without exceeding budget
# - Always round up to whole workers

CORES_PER_WORKER = 4  # For m5.xlarge

min_workers = None  # YOUR CALCULATION: ceil(typical_cores / cores_per_worker)
max_workers = None  # YOUR CALCULATION: ceil(peak_cores / cores_per_worker)

print(f"Autoscaling: min={min_workers}, max={max_workers} workers")
print(f"Core range: {min_workers * CORES_PER_WORKER} - {max_workers * CORES_PER_WORKER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Cost Estimation (5 minutes)
# MAGIC
# MAGIC ### TODO 3.5: Calculate DBU costs

# COMMAND ----------

# Pricing (example - varies by cloud and commitment)
DBU_RATE_JOBS = 0.15  # $/DBU for Jobs compute
DBU_RATE_INTERACTIVE = 0.55  # $/DBU for All-Purpose compute
DBU_PER_HOUR_M5_XLARGE = 0.75

# Usage patterns
RUNS_PER_DAY = 24  # Hourly incremental
AVG_RUN_DURATION_HOURS = 0.1  # 6 minutes
WORKERS_TYPICAL = 2

# TODO 3.5a: Calculate daily DBU consumption
# Formula: workers × DBU_per_hour × run_duration × runs_per_day

daily_dbus = None  # YOUR CALCULATION HERE
monthly_dbus = None  # daily_dbus × 30

print(f"Daily DBUs: {daily_dbus}")
print(f"Monthly DBUs: {monthly_dbus}")

# TODO 3.5b: Calculate monthly cost
monthly_cost_jobs = None  # monthly_dbus × DBU_RATE_JOBS
monthly_cost_interactive = None  # monthly_dbus × DBU_RATE_INTERACTIVE

print(f"Monthly cost (Jobs): ${monthly_cost_jobs:.2f}")
print(f"Monthly cost (Interactive): ${monthly_cost_interactive:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Final Configuration
# MAGIC
# MAGIC ### TODO 3.6: Complete the cluster configuration

# COMMAND ----------

cluster_config = {
    "cluster_name": "nemweb-pipeline-prod",
    "spark_version": "15.4.x-scala2.12",  # DBR 15.4 for Python Data Source API

    # TODO 3.6: Fill in based on your analysis
    "node_type_id": None,  # e.g., "m5.xlarge"
    "driver_node_type_id": None,  # Usually same as worker

    "autoscale": {
        "min_workers": None,  # From Part 4
        "max_workers": None,  # From Part 4
    },

    # Autotermination for cost control
    "autotermination_minutes": 20,

    # Spark configuration
    "spark_conf": {
        # Optimize for your workload
        "spark.sql.shuffle.partitions": "auto",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
    },

    # Tags for cost tracking
    "custom_tags": {
        "project": "nemweb-pipeline",
        "environment": "production",
        "cost_center": "data-engineering"
    }
}

import json
print("Cluster Configuration:")
print(json.dumps(cluster_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checklist
# MAGIC
# MAGIC Before deploying to production, verify:
# MAGIC
# MAGIC - [ ] Instance type matches workload profile (CPU/memory/IO)
# MAGIC - [ ] Min workers handle typical load at 70% utilization
# MAGIC - [ ] Max workers handle peak load within budget
# MAGIC - [ ] Autotermination configured to prevent idle costs
# MAGIC - [ ] Spark shuffle partitions set appropriately
# MAGIC - [ ] Cost tags configured for chargeback

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Sizing Methodology
# MAGIC
# MAGIC 1. **Measure first**: Run test workload, capture Spark UI metrics
# MAGIC 2. **Calculate cores**: total_task_time / target_duration × 1.2 (overhead)
# MAGIC 3. **Select instance**: Match CPU/memory/IO profile to instance family
# MAGIC 4. **Set autoscaling**: Based on typical vs. peak patterns
# MAGIC 5. **Estimate costs**: DBUs × rate × expected usage
# MAGIC
# MAGIC ### Avoid These Mistakes
# MAGIC
# MAGIC - Intuition-based sizing without metrics
# MAGIC - Memory-optimized instances for CPU-bound work
# MAGIC - min=1, max=32 autoscaling (too wide)
# MAGIC - Forgetting driver sizing for large collects
# MAGIC - Not setting autotermination

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the Databricks NEMWEB Lab. You now know how to:
# MAGIC
# MAGIC 1. Build custom PySpark data sources using Python Data Source API
# MAGIC 2. Integrate custom sources into Lakeflow Declarative Pipelines
# MAGIC 3. Make data-driven cluster sizing decisions
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Review solutions in the `solutions/` folder
# MAGIC - Extend the data source with additional NEMWEB tables
# MAGIC - Apply this methodology to your production workloads
