# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3a: Cluster Right-Sizing Analysis
# MAGIC
# MAGIC **Time:** 15 minutes
# MAGIC
# MAGIC In this exercise, you'll analyze Spark UI metrics and workload characteristics
# MAGIC to make data-driven cluster sizing decisions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Interpret Spark UI metrics (CPU%, memory%, task duration)
# MAGIC 2. Understand why **bigger clusters can cost less** (faster completion)
# MAGIC 3. Learn when to enable **Photon** (almost always!)
# MAGIC 4. Select appropriate Azure instance types
# MAGIC 5. Configure autoscaling for cost optimization
# MAGIC
# MAGIC ## Key Insights
# MAGIC
# MAGIC > **Counterintuitive truth:** A larger cluster often costs LESS than a smaller one
# MAGIC > because it finishes faster. You pay for DBU × time, not just DBU rate.
# MAGIC
# MAGIC > **Photon rule:** Enable Photon for most workloads. The 2-5x speedup typically
# MAGIC > outweighs the ~2x DBU premium. You save money by finishing faster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Cost Paradox: Bigger Can Be Cheaper
# MAGIC
# MAGIC Consider this example:
# MAGIC
# MAGIC | Scenario | Workers | DBU/hr | Duration | Total DBUs | Total Cost |
# MAGIC |----------|---------|--------|----------|------------|------------|
# MAGIC | Small cluster | 2 | 2.0 | 60 min | 2.0 | $0.30 |
# MAGIC | Large cluster | 8 | 8.0 | 12 min | 1.6 | **$0.24** |
# MAGIC
# MAGIC **The large cluster is 20% cheaper!** Why?
# MAGIC - Cost = DBU rate × time
# MAGIC - 4x the workers but 5x faster = net savings
# MAGIC - Plus: faster SLA, happier users, freed resources

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Hidden Cost of Undersized Clusters: Disk Spill
# MAGIC
# MAGIC When a cluster doesn't have enough memory, Spark **spills data to disk**. This is
# MAGIC catastrophic for performance and cost.
# MAGIC
# MAGIC ### What is Disk Spill?
# MAGIC
# MAGIC During shuffles (joins, aggregations, sorts), Spark needs memory to hold intermediate data.
# MAGIC If there isn't enough memory, it writes overflow to local disk, then reads it back later.
# MAGIC
# MAGIC ### The Performance Penalty
# MAGIC
# MAGIC | Operation | Speed |
# MAGIC |-----------|-------|
# MAGIC | Memory access | ~100 GB/s |
# MAGIC | SSD disk access | ~500 MB/s |
# MAGIC | **Penalty** | **~200x slower** |
# MAGIC
# MAGIC A task that takes 10 seconds in memory might take **30+ minutes** when spilling to disk!
# MAGIC
# MAGIC ### The Cost Impact
# MAGIC
# MAGIC | Scenario | Memory | Duration | DBU Cost |
# MAGIC |----------|--------|----------|----------|
# MAGIC | Right-sized | 64 GB | 5 min | $0.25 |
# MAGIC | Undersized (spilling) | 16 GB | 45 min | **$2.25** |
# MAGIC
# MAGIC **The "cheap" undersized cluster costs 9x more!**
# MAGIC
# MAGIC ### How to Detect Spill in Spark UI
# MAGIC
# MAGIC In the Spark UI, check the **Stages** tab:
# MAGIC - **Shuffle Spill (Memory)**: Data spilled from memory
# MAGIC - **Shuffle Spill (Disk)**: Data written to disk
# MAGIC
# MAGIC > **Rule:** If you see ANY disk spill, your cluster is undersized. Add memory or workers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario: NEMWEB Production Pipeline
# MAGIC
# MAGIC You're deploying the NEMWEB pipeline to production on **Azure Databricks**.
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Data volume | 6 months historical + daily incremental |
# MAGIC | Historical load | ~50-100 MB (260k rows) |
# MAGIC | Daily incremental | ~1 MB (1,440 rows per region × 5 regions) |
# MAGIC | SLA | Process within 15 minutes of data availability |
# MAGIC | Budget | Minimize total cost while meeting SLA |

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
# MAGIC | **Shuffle Spill** | **0** | **⚠️ Any spill = 10-100x slower! Add memory immediately** |
# MAGIC | GC Time | <10% of task time | High GC: add memory, check for data skew |
# MAGIC
# MAGIC > **Critical:** Shuffle Spill is the #1 indicator of an undersized cluster. Even small
# MAGIC > amounts of spill can multiply your job duration (and cost) by 10x or more.

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
# MAGIC ## Part 3: Azure Instance Type Selection
# MAGIC
# MAGIC ### Azure VM Instance Comparison
# MAGIC
# MAGIC | Instance | vCPUs | Memory | DBU/hr | Photon DBU/hr | Use Case |
# MAGIC |----------|-------|--------|--------|---------------|----------|
# MAGIC | Standard_DS3_v2 | 4 | 14 GB | 0.75 | 1.5 | General purpose |
# MAGIC | Standard_DS4_v2 | 8 | 28 GB | 1.5 | 3.0 | General purpose |
# MAGIC | Standard_DS5_v2 | 16 | 56 GB | 3.0 | 6.0 | General purpose |
# MAGIC | Standard_F8s_v2 | 8 | 16 GB | 1.0 | 2.0 | CPU-intensive |
# MAGIC | Standard_E8s_v3 | 8 | 64 GB | 2.0 | 4.0 | Memory-intensive |
# MAGIC
# MAGIC ### Photon: When to Enable?
# MAGIC
# MAGIC **Short answer: Almost always.**
# MAGIC
# MAGIC | Workload Type | Photon Speedup | Worth the 2x DBU? |
# MAGIC |---------------|----------------|-------------------|
# MAGIC | SQL/DataFrame transforms | 2-4x | **Yes** |
# MAGIC | Aggregations & joins | 2-5x | **Yes** |
# MAGIC | File parsing (Parquet/Delta) | 2-3x | **Yes** |
# MAGIC | Python UDFs only | 1x (no benefit) | No |
# MAGIC | ML model training | 1x (no benefit) | No |
# MAGIC
# MAGIC > **Rule of thumb:** If your workload is >50% SQL/DataFrame operations,
# MAGIC > enable Photon. The speedup pays for the DBU premium.

# COMMAND ----------

# TODO 3.3: Rate your workload (1=low, 5=high)
# NEMWEB pipeline characteristics:
# - Reads CSV/ZIP files (I/O bound initially)
# - Transforms with SQL/DataFrame (Photon helps!)
# - Aggregations for gold layer (Photon helps!)

cpu_intensity = None  # CSV parsing → typically 2-3
memory_intensity = None  # Wide tables, aggregations → typically 2-3
io_intensity = None  # Network/disk I/O → typically 3-4
sql_dataframe_pct = None  # % of work that's SQL/DataFrame → typically 70-80%

# Decision matrix for Azure
def recommend_azure_instance(cpu: int, memory: int, io: int) -> str:
    """Recommend Azure instance type based on workload characteristics."""
    if memory >= 4:
        return "Standard_E8s_v3 (memory-optimized) - Large aggregations/wide tables"
    elif cpu >= 4:
        return "Standard_F8s_v2 (CPU-optimized) - Heavy computation"
    else:
        return "Standard_DS4_v2 (general purpose) - Balanced workload"

def recommend_photon(sql_pct: int) -> str:
    """Recommend whether to enable Photon."""
    if sql_pct >= 50:
        return "YES - Enable Photon (SQL/DataFrame work will benefit from 2-4x speedup)"
    else:
        return "NO - Disable Photon (mostly Python UDFs or ML, no speedup)"

# Uncomment after setting scores:
# print(f"Instance: {recommend_azure_instance(cpu_intensity, memory_intensity, io_intensity)}")
# print(f"Photon: {recommend_photon(sql_dataframe_pct)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: The "Bigger is Cheaper" Calculation
# MAGIC
# MAGIC Let's prove that a larger cluster can cost less.
# MAGIC
# MAGIC ### TODO 3.4: Compare small vs large cluster costs

# COMMAND ----------

# Scenario: Process 100 MB of data with 10 tasks, 45 seconds each

# Small cluster: 2 workers × Standard_DS4_v2 (8 cores each = 16 total cores)
# Large cluster: 4 workers × Standard_DS4_v2 (8 cores each = 32 total cores)

TASKS = 10
TASK_DURATION_SEC = 45
DBU_PER_WORKER_HOUR = 1.5  # Standard_DS4_v2
DBU_RATE = 0.15  # $/DBU for Jobs compute

# Small cluster calculation
small_workers = 2
small_cores = small_workers * 8
# With 16 cores, we can run all 10 tasks in parallel, completing in ~45 seconds
small_duration_min = (TASKS * TASK_DURATION_SEC) / small_cores / 60
small_dbus = small_workers * DBU_PER_WORKER_HOUR * (small_duration_min / 60)
small_cost = small_dbus * DBU_RATE

print("=== Small Cluster (2 workers) ===")
print(f"Duration: {small_duration_min:.1f} minutes")
print(f"DBUs consumed: {small_dbus:.2f}")
print(f"Cost: ${small_cost:.4f}")

# Large cluster calculation
large_workers = 4
large_cores = large_workers * 8
large_duration_min = (TASKS * TASK_DURATION_SEC) / large_cores / 60
large_dbus = large_workers * DBU_PER_WORKER_HOUR * (large_duration_min / 60)
large_cost = large_dbus * DBU_RATE

print("\n=== Large Cluster (4 workers) ===")
print(f"Duration: {large_duration_min:.1f} minutes")
print(f"DBUs consumed: {large_dbus:.2f}")
print(f"Cost: ${large_cost:.4f}")

# Comparison
print("\n=== Comparison ===")
print(f"Duration savings: {small_duration_min - large_duration_min:.1f} minutes faster")
print(f"Cost difference: ${small_cost - large_cost:.4f}")
if large_cost < small_cost:
    savings_pct = (1 - large_cost/small_cost) * 100
    print(f"Large cluster is {savings_pct:.0f}% CHEAPER!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Photon Cost/Benefit Analysis
# MAGIC
# MAGIC ### TODO 3.5: Calculate when Photon pays for itself

# COMMAND ----------

# Without Photon
base_dbu_rate = 1.5  # Standard_DS4_v2
base_duration_min = 10  # minutes
base_dbus = 2 * base_dbu_rate * (base_duration_min / 60)  # 2 workers
base_cost = base_dbus * DBU_RATE

print("=== Without Photon ===")
print(f"Duration: {base_duration_min} minutes")
print(f"DBUs: {base_dbus:.2f}")
print(f"Cost: ${base_cost:.4f}")

# With Photon (2x DBU rate, but typically 2-3x faster)
photon_dbu_rate = 3.0  # 2x the base rate
photon_speedup = 2.5  # Conservative estimate
photon_duration_min = base_duration_min / photon_speedup
photon_dbus = 2 * photon_dbu_rate * (photon_duration_min / 60)
photon_cost = photon_dbus * DBU_RATE

print("\n=== With Photon (2.5x speedup) ===")
print(f"Duration: {photon_duration_min:.1f} minutes")
print(f"DBUs: {photon_dbus:.2f}")
print(f"Cost: ${photon_cost:.4f}")

# Break-even analysis
print("\n=== Break-Even Analysis ===")
breakeven_speedup = 2.0  # At 2x speedup, cost is equal
print(f"Break-even speedup: {breakeven_speedup}x")
print(f"Typical Photon speedup: 2-4x")
if photon_cost < base_cost:
    savings = base_cost - photon_cost
    print(f"Photon SAVES ${savings:.4f} per run ({(1-photon_cost/base_cost)*100:.0f}% cheaper)")
else:
    extra = photon_cost - base_cost
    print(f"Photon costs ${extra:.4f} more per run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Autoscaling Configuration
# MAGIC
# MAGIC ### Common Autoscaling Mistakes
# MAGIC
# MAGIC 1. **Too wide**: min=1, max=32 → unpredictable costs
# MAGIC 2. **Too narrow**: min=max=8 → no flexibility
# MAGIC 3. **min too low**: Slow cold start when cluster scales from 1
# MAGIC
# MAGIC ### Best Practice: Set min based on typical load, max based on SLA

# COMMAND ----------

# Workload patterns (from analysis)
TYPICAL_LOAD_CORES = 8   # Daily incremental
PEAK_LOAD_CORES = 16     # Historical backfill or catch-up

# TODO 3.6: Set autoscaling bounds
# Rules:
# - min_workers: Handle typical load at ~70% utilization
# - max_workers: Handle peak load within SLA
# - Always round up to whole workers

CORES_PER_WORKER = 8  # For Standard_DS4_v2

min_workers = None  # YOUR CALCULATION: ceil(typical_cores / cores_per_worker)
max_workers = None  # YOUR CALCULATION: ceil(peak_cores / cores_per_worker)

# Uncomment after calculation:
# print(f"Autoscaling: min={min_workers}, max={max_workers} workers")
# print(f"Core range: {min_workers * CORES_PER_WORKER} - {max_workers * CORES_PER_WORKER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Final Configuration
# MAGIC
# MAGIC ### TODO 3.7: Complete the cluster configuration

# COMMAND ----------

import json

cluster_config = {
    "cluster_name": "nemweb-pipeline-prod",
    "spark_version": "15.4.x-scala2.12",  # DBR 15.4 for Python Data Source API

    # Azure VM type
    "node_type_id": "Standard_DS4_v2",  # 8 vCPUs, 28 GB RAM
    "driver_node_type_id": "Standard_DS4_v2",

    # Photon - ENABLE for SQL/DataFrame workloads
    "runtime_engine": "PHOTON",  # or "STANDARD" for Python UDF-heavy work

    "autoscale": {
        "min_workers": 1,  # TODO: Set based on your calculation
        "max_workers": 4,  # TODO: Set based on your calculation
    },

    # Autotermination for cost control
    "autotermination_minutes": 20,

    # Spark configuration
    "spark_conf": {
        "spark.sql.shuffle.partitions": "auto",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
    },

    # Azure-specific settings
    "azure_attributes": {
        "availability": "ON_DEMAND_AZURE",
        "first_on_demand": 1,  # Driver always on-demand
        "spot_bid_max_price": -1,  # Use spot for workers (cost savings)
    },

    # Tags for cost tracking
    "custom_tags": {
        "project": "nemweb-pipeline",
        "environment": "production",
        "cost_center": "data-engineering"
    }
}

print("Cluster Configuration:")
print(json.dumps(cluster_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Key Takeaways
# MAGIC
# MAGIC ### 1. Bigger Clusters Can Cost Less
# MAGIC - Cost = DBU rate × time
# MAGIC - 2x workers finishing in half the time = same cost
# MAGIC - Often faster than linear → actual savings
# MAGIC
# MAGIC ### 2. Disk Spill = Money on Fire 🔥
# MAGIC - Undersized clusters spill to disk (200x slower than memory)
# MAGIC - A "cheap" cluster that spills can cost **10x more** than a right-sized one
# MAGIC - Check Spark UI: any Shuffle Spill means add memory/workers
# MAGIC
# MAGIC ### 3. Enable Photon for Most Workloads
# MAGIC - 2x DBU cost but 2-4x speedup = net savings
# MAGIC - Only disable for pure Python UDF or ML workloads
# MAGIC - Default to ON, measure, then decide
# MAGIC
# MAGIC ### 4. Azure Instance Selection
# MAGIC
# MAGIC | Workload | Instance | When to Use |
# MAGIC |----------|----------|-------------|
# MAGIC | General | Standard_DS4_v2 | Default choice |
# MAGIC | CPU-heavy | Standard_F8s_v2 | Heavy transforms |
# MAGIC | Memory-heavy | Standard_E8s_v3 | Large aggregations |
# MAGIC
# MAGIC ### 5. Autoscaling Strategy
# MAGIC - **min**: Typical load at 70% utilization
# MAGIC - **max**: Peak load within SLA
# MAGIC - Use spot instances for workers (30-60% savings)
# MAGIC
# MAGIC ### 6. Measure, Don't Guess
# MAGIC - Run test workload, check Spark UI
# MAGIC - Calculate: total_task_time / target_duration = required_parallelism
# MAGIC - Add 20% overhead for startup/coordination

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Exercise 3b** for optimization techniques (liquid clustering, ANALYZE TABLE).
