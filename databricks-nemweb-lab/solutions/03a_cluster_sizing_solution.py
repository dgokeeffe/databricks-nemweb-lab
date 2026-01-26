# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 3a - Cluster Right-Sizing Analysis
# MAGIC
# MAGIC This notebook contains the complete solutions for Exercise 3a.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Interpret Spark UI metrics (CPU%, memory%, task duration)
# MAGIC 2. Calculate required compute from data volume → partitions → cores
# MAGIC 3. Select appropriate instance types based on workload
# MAGIC 4. Set autoscaling boundaries based on patterns
# MAGIC 5. Estimate DBU costs for different configurations

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
# MAGIC ## Solution 3.1: Workload Analysis

# COMMAND ----------

import math

# Workload parameters
HISTORICAL_ROWS = 260_000
HISTORICAL_SIZE_MB = 100
DAILY_ROWS = 7_200  # 288 intervals × 5 regions × 5 (days buffer)
DAILY_SIZE_MB = 3

# Spark defaults
DEFAULT_PARTITION_SIZE_MB = 128
CORES_PER_PARTITION = 1  # One task per core

# SOLUTION 3.1a: Calculate optimal partition count for historical load
# Formula: partitions = data_size_mb / target_partition_size_mb
# Target partition size: 64-128 MB for balanced parallelism

target_partition_size_mb = 64  # Smaller partitions for better parallelism
historical_partitions = math.ceil(HISTORICAL_SIZE_MB / target_partition_size_mb)

print(f"Historical data: {HISTORICAL_SIZE_MB} MB")
print(f"Target partition size: {target_partition_size_mb} MB")
print(f"Recommended partitions: {historical_partitions}")
print(f"\nNote: With {historical_partitions} partitions, each is ~{HISTORICAL_SIZE_MB/historical_partitions:.0f} MB")

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
# MAGIC ## Solution 3.2: Core Requirements Calculation

# COMMAND ----------

# Timing requirements
SLA_MINUTES = 15
TARGET_COMPLETION_MINUTES = 10  # Leave buffer for variability

# Measured task metrics (from test run)
# These would come from Spark UI in production
AVG_TASK_DURATION_SECONDS = 45
TASK_COUNT = 10  # Number of partitions

# SOLUTION 3.2a: Calculate required parallelism
# Formula: total_task_time = task_count × avg_task_duration_seconds
# Required cores = total_task_time / (target_completion_minutes × 60)

total_task_time_seconds = TASK_COUNT * AVG_TASK_DURATION_SECONDS
target_time_seconds = TARGET_COMPLETION_MINUTES * 60
required_cores = math.ceil(total_task_time_seconds / target_time_seconds)

print(f"Task count: {TASK_COUNT}")
print(f"Avg task duration: {AVG_TASK_DURATION_SECONDS} seconds")
print(f"Total task time: {total_task_time_seconds} seconds ({total_task_time_seconds/60:.1f} minutes)")
print(f"Target completion: {TARGET_COMPLETION_MINUTES} minutes ({target_time_seconds} seconds)")
print(f"Minimum cores required: {required_cores}")

# SOLUTION 3.2b: Add overhead for Spark driver, executors startup, etc.
# Rule of thumb: Add 20% overhead
OVERHEAD_FACTOR = 1.2
cores_with_overhead = math.ceil(required_cores * OVERHEAD_FACTOR)

print(f"\nWith 20% overhead: {cores_with_overhead} cores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 3.3: Instance Type Selection
# MAGIC
# MAGIC ### AWS Instance Comparison
# MAGIC
# MAGIC | Instance | vCPUs | Memory | DBU/hr | Use Case |
# MAGIC |----------|-------|--------|--------|----------|
# MAGIC | m5.xlarge | 4 | 16 GB | 0.75 | General purpose |
# MAGIC | m5.2xlarge | 8 | 32 GB | 1.5 | General purpose |
# MAGIC | c5.2xlarge | 8 | 16 GB | 1.0 | CPU-intensive |
# MAGIC | r5.2xlarge | 8 | 64 GB | 2.0 | Memory-intensive |

# COMMAND ----------

# Workload characteristics assessment
# Score 1-5 for each dimension

# SOLUTION 3.3: Rate the NEMWEB workload

# HTTP parsing, CSV processing - moderate CPU work
cpu_intensity = 3

# DataFrame operations, no large aggregations or wide tables
# NEMWEB data is relatively narrow (12 columns)
memory_intensity = 2

# Network calls to NEMWEB HTTP API - significant I/O wait
io_intensity = 4

print("NEMWEB Workload Profile:")
print(f"  CPU intensity:    {cpu_intensity}/5 (CSV parsing, transforms)")
print(f"  Memory intensity: {memory_intensity}/5 (narrow tables, simple aggregations)")
print(f"  I/O intensity:    {io_intensity}/5 (HTTP fetching from NEMWEB)")

# Decision matrix
def recommend_instance_type(cpu: int, memory: int, io: int) -> str:
    """Recommend instance type based on workload characteristics."""
    if memory >= 4:
        return "r5.xlarge (memory-optimized) - Large aggregations/wide tables"
    elif cpu >= 4:
        return "c5.xlarge (CPU-optimized) - Heavy computation"
    else:
        return "m5.xlarge (general purpose) - Balanced workload"

recommendation = recommend_instance_type(cpu_intensity, memory_intensity, io_intensity)
print(f"\nRecommended instance: {recommendation}")

print("\nRationale:")
print("  - NEMWEB workload is I/O bound (waiting for HTTP responses)")
print("  - Memory needs are modest (narrow tables)")
print("  - General purpose instances provide best cost/performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 3.4: Autoscaling Configuration
# MAGIC
# MAGIC ### Common Autoscaling Mistakes
# MAGIC
# MAGIC 1. **Too wide**: min=1, max=32 allows runaway costs
# MAGIC 2. **Too narrow**: min=max=8 prevents optimization
# MAGIC 3. **Wrong baseline**: min too low causes slow startup

# COMMAND ----------

# Workload patterns (from analysis)
TYPICAL_LOAD_CORES = 4   # Daily incremental processing
PEAK_LOAD_CORES = 8      # Historical backfill or catch-up scenarios

# SOLUTION 3.4: Set autoscaling bounds
# Rules:
# - min_workers: Handle typical load with ~70% utilization
# - max_workers: Handle peak load without exceeding budget
# - Always round up to whole workers

CORES_PER_WORKER = 4  # For m5.xlarge

# min_workers should handle typical load at 70% utilization
# If typical needs 4 cores at 100%, at 70% we need 4/0.7 ≈ 6 cores
# But we also don't want to over-provision for small loads
min_workers = math.ceil(TYPICAL_LOAD_CORES / CORES_PER_WORKER)

# max_workers should handle peak load
max_workers = math.ceil(PEAK_LOAD_CORES / CORES_PER_WORKER)

# Ensure min <= max
max_workers = max(max_workers, min_workers)

print(f"Instance type: m5.xlarge ({CORES_PER_WORKER} cores each)")
print(f"Typical load: {TYPICAL_LOAD_CORES} cores")
print(f"Peak load: {PEAK_LOAD_CORES} cores")
print(f"\nAutoscaling configuration:")
print(f"  min_workers: {min_workers}")
print(f"  max_workers: {max_workers}")
print(f"  Core range: {min_workers * CORES_PER_WORKER} - {max_workers * CORES_PER_WORKER}")

print("\nRationale:")
print(f"  - min={min_workers}: Handles daily incremental without scale-up delay")
print(f"  - max={max_workers}: Caps peak usage, prevents runaway costs")
print(f"  - Narrow range (1-2 workers) = predictable costs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 3.5: Cost Estimation

# COMMAND ----------

# Pricing (example - varies by cloud and commitment)
DBU_RATE_JOBS = 0.15        # $/DBU for Jobs compute
DBU_RATE_INTERACTIVE = 0.55  # $/DBU for All-Purpose compute
DBU_PER_HOUR_M5_XLARGE = 0.75

# Usage patterns
RUNS_PER_DAY = 24           # Hourly incremental
AVG_RUN_DURATION_HOURS = 0.1  # 6 minutes
WORKERS_TYPICAL = min_workers  # Use calculated min_workers

# SOLUTION 3.5a: Calculate daily DBU consumption
# Formula: workers × DBU_per_hour × run_duration × runs_per_day

daily_dbus = WORKERS_TYPICAL * DBU_PER_HOUR_M5_XLARGE * AVG_RUN_DURATION_HOURS * RUNS_PER_DAY
monthly_dbus = daily_dbus * 30

print("Usage Pattern:")
print(f"  Runs per day: {RUNS_PER_DAY}")
print(f"  Avg run duration: {AVG_RUN_DURATION_HOURS * 60:.0f} minutes")
print(f"  Typical workers: {WORKERS_TYPICAL}")
print(f"  DBU rate per worker: {DBU_PER_HOUR_M5_XLARGE}/hour")

print(f"\nDBU Consumption:")
print(f"  Daily DBUs: {daily_dbus:.1f}")
print(f"  Monthly DBUs: {monthly_dbus:.1f}")

# SOLUTION 3.5b: Calculate monthly cost
monthly_cost_jobs = monthly_dbus * DBU_RATE_JOBS
monthly_cost_interactive = monthly_dbus * DBU_RATE_INTERACTIVE

print(f"\nMonthly Cost Estimate:")
print(f"  Jobs compute (${DBU_RATE_JOBS}/DBU):        ${monthly_cost_jobs:.2f}")
print(f"  Interactive (${DBU_RATE_INTERACTIVE}/DBU): ${monthly_cost_interactive:.2f}")

print(f"\n💡 Jobs compute saves ${monthly_cost_interactive - monthly_cost_jobs:.2f}/month ({(1 - DBU_RATE_JOBS/DBU_RATE_INTERACTIVE)*100:.0f}% savings)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 3.6: Final Configuration

# COMMAND ----------

import json

cluster_config = {
    "cluster_name": "nemweb-pipeline-prod",
    "spark_version": "15.4.x-scala2.12",  # DBR 15.4 for Python Data Source API

    # SOLUTION 3.6: Completed configuration
    "node_type_id": "m5.xlarge",           # General purpose - matches workload profile
    "driver_node_type_id": "m5.xlarge",    # Same as worker for simplicity

    "autoscale": {
        "min_workers": min_workers,         # From Part 4
        "max_workers": max_workers,         # From Part 4
    },

    # Autotermination for cost control
    "autotermination_minutes": 20,

    # Spark configuration
    "spark_conf": {
        # Optimize for workload
        "spark.sql.shuffle.partitions": "auto",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
        # Connection pooling for HTTP-heavy workload
        "spark.databricks.http.connectionTimeout": "60s",
    },

    # Tags for cost tracking
    "custom_tags": {
        "project": "nemweb-pipeline",
        "environment": "production",
        "cost_center": "data-engineering"
    }
}

print("=" * 60)
print("FINAL CLUSTER CONFIGURATION")
print("=" * 60)
print(json.dumps(cluster_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checklist
# MAGIC
# MAGIC Before deploying to production, verify:
# MAGIC
# MAGIC - [x] Instance type matches workload profile (CPU/memory/IO) → **m5.xlarge (general purpose)**
# MAGIC - [x] Min workers handle typical load at 70% utilization → **1 worker = 4 cores**
# MAGIC - [x] Max workers handle peak load within budget → **2 workers = 8 cores**
# MAGIC - [x] Autotermination configured to prevent idle costs → **20 minutes**
# MAGIC - [x] Spark shuffle partitions set appropriately → **auto**
# MAGIC - [x] Cost tags configured for chargeback → **project, environment, cost_center**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### NEMWEB Pipeline Sizing Results
# MAGIC
# MAGIC | Parameter | Value | Rationale |
# MAGIC |-----------|-------|-----------|
# MAGIC | Instance type | m5.xlarge | I/O bound workload, general purpose best |
# MAGIC | Min workers | 1 | Handles daily incremental |
# MAGIC | Max workers | 2 | Caps peak backfill scenarios |
# MAGIC | Monthly DBUs | ~54 | Based on 24 runs/day × 6 min each |
# MAGIC | Monthly cost | ~$8 (Jobs) | Using Jobs compute pricing |
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
