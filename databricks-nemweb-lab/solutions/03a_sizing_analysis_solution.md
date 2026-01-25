# Solution: Exercise 3 - Cluster Sizing Analysis

This document contains the complete solutions for the cluster sizing exercise.

## Part 1: Workload Analysis

### Solution 3.1: Partition Calculation

```python
# Given values
HISTORICAL_SIZE_MB = 100
target_partition_size_mb = 64

# SOLUTION 3.1a: Calculate partitions
historical_partitions = HISTORICAL_SIZE_MB / target_partition_size_mb
# = 100 / 64 = 1.56 → round up to 2 partitions

# For production, we'd want more parallelism
# With 5 regions, minimum 5 partitions (1 per region)
# For 100MB data, 64MB partitions gives good balance
recommended_partitions = max(2, 5)  # 5 (one per region)
```

**Explanation:**
- 100 MB / 64 MB = ~1.6 partitions
- But we also partition by region (5 regions)
- So minimum 5 partitions for regional parallelism
- This is a small dataset; for 6-month production (600+ MB), we'd have 10+ partitions

## Part 2: Core Requirements

### Solution 3.2: Core Calculation

```python
# Given values from test run
AVG_TASK_DURATION_SECONDS = 45
TASK_COUNT = 10  # Number of partitions/tasks
TARGET_COMPLETION_MINUTES = 10

# SOLUTION 3.2a: Calculate total task time
total_task_time_seconds = TASK_COUNT * AVG_TASK_DURATION_SECONDS
# = 10 * 45 = 450 seconds

# Calculate required cores for parallel execution
target_completion_seconds = TARGET_COMPLETION_MINUTES * 60  # 600 seconds

required_cores = total_task_time_seconds / target_completion_seconds
# = 450 / 600 = 0.75 → round up to 1 core minimum

# But we want parallelism, so match task count
# If we have 10 tasks and want them done in parallel: 10 cores
# For target completion in 10 min with 45s tasks: ceil(450/600) = 1 core minimum

# Practical calculation:
# To run 10 tasks in parallel: 10 cores
# To run 10 tasks in 10 minutes with 45s tasks: need 1 core (sequential)
# Sweet spot: 2-4 cores for reasonable parallelism

practical_cores = 4  # Run 2-3 tasks concurrently

# SOLUTION 3.2b: Add overhead
cores_with_overhead = practical_cores * 1.2
# = 4 * 1.2 = 4.8 → round up to 5 cores
```

**Explanation:**
- Total task time: 10 tasks × 45s = 450 seconds (7.5 minutes sequential)
- With 4 cores: 450s / 4 ≈ 112s (< 2 minutes)
- Adding 20% overhead: 5 cores recommended
- This is a small workload; actual production may need 8-16 cores

## Part 3: Instance Type Selection

### Solution 3.3: Workload Profile

```python
# SOLUTION 3.3: Rate workload characteristics

# NEMWEB pipeline profile:
cpu_intensity = 3  # Moderate - CSV parsing, HTTP handling
memory_intensity = 2  # Low - simple aggregations, no wide joins
io_intensity = 4  # High - network calls to NEMWEB

# Recommendation: General purpose (m5.xlarge)
# - CPU-optimized would work but wastes less memory
# - Memory-optimized is overkill for this workload
# - I/O patterns are external (network) not disk
```

**Decision Matrix:**

| Workload Type | CPU | Memory | I/O | Recommendation |
|---------------|-----|--------|-----|----------------|
| NEMWEB ETL | 3 | 2 | 4 | m5.xlarge (general purpose) |
| Complex joins | 2 | 5 | 3 | r5.xlarge (memory) |
| ML training | 5 | 3 | 2 | c5.xlarge (CPU) |

**Why m5.xlarge for NEMWEB:**
- CSV parsing is not CPU-heavy
- No large in-memory aggregations
- Network I/O doesn't benefit from special instances
- Cost-effective balance

## Part 4: Autoscaling Configuration

### Solution 3.4: Autoscaling Bounds

```python
# Load patterns
TYPICAL_LOAD_CORES = 4   # Daily incremental
PEAK_LOAD_CORES = 8      # Historical backfill or catch-up
CORES_PER_WORKER = 4     # m5.xlarge has 4 vCPUs

# SOLUTION 3.4: Calculate workers
import math

min_workers = math.ceil(TYPICAL_LOAD_CORES / CORES_PER_WORKER)
# = ceil(4 / 4) = 1 worker

max_workers = math.ceil(PEAK_LOAD_CORES / CORES_PER_WORKER)
# = ceil(8 / 4) = 2 workers

# Final configuration
autoscale_config = {
    "min_workers": 1,
    "max_workers": 2
}
```

**Explanation:**
- Typical load (daily incremental): 4 cores → 1 worker
- Peak load (backfill): 8 cores → 2 workers
- Narrow range (1-2) prevents runaway costs
- Autotermination: 20 minutes (balance between cost and restart time)

**Anti-patterns avoided:**
- ❌ min=1, max=32 (too wide, cost risk)
- ❌ min=8, max=8 (no elasticity)
- ✅ min=1, max=2 (right-sized for workload)

## Part 5: Cost Estimation

### Solution 3.5: DBU Calculation

```python
# Pricing
DBU_RATE_JOBS = 0.15  # $/DBU
DBU_RATE_INTERACTIVE = 0.55  # $/DBU
DBU_PER_HOUR_M5_XLARGE = 0.75

# Usage pattern
RUNS_PER_DAY = 24  # Hourly incremental
AVG_RUN_DURATION_HOURS = 0.1  # 6 minutes
WORKERS_TYPICAL = 2  # Average workers during run

# SOLUTION 3.5a: Calculate daily DBU
daily_dbus = WORKERS_TYPICAL * DBU_PER_HOUR_M5_XLARGE * AVG_RUN_DURATION_HOURS * RUNS_PER_DAY
# = 2 * 0.75 * 0.1 * 24 = 3.6 DBUs/day

monthly_dbus = daily_dbus * 30
# = 3.6 * 30 = 108 DBUs/month

# SOLUTION 3.5b: Calculate costs
monthly_cost_jobs = monthly_dbus * DBU_RATE_JOBS
# = 108 * 0.15 = $16.20/month

monthly_cost_interactive = monthly_dbus * DBU_RATE_INTERACTIVE
# = 108 * 0.55 = $59.40/month
```

**Cost Summary:**

| Configuration | Monthly DBUs | Jobs Cost | Interactive Cost |
|---------------|-------------|-----------|------------------|
| Current estimate | 108 | $16.20 | $59.40 |
| With 50% buffer | 162 | $24.30 | $89.10 |

**Recommendation:** Use Jobs compute ($16-25/month) instead of Interactive ($60-90/month).

## Part 6: Final Configuration

### Solution 3.6: Complete Cluster Configuration

```json
{
  "cluster_name": "nemweb-pipeline-prod",
  "spark_version": "15.4.x-scala2.12",

  "node_type_id": "m5.xlarge",
  "driver_node_type_id": "m5.xlarge",

  "autoscale": {
    "min_workers": 1,
    "max_workers": 2
  },

  "autotermination_minutes": 20,

  "spark_conf": {
    "spark.sql.shuffle.partitions": "auto",
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
    "spark.sql.adaptive.enabled": "true"
  },

  "custom_tags": {
    "project": "nemweb-pipeline",
    "environment": "production",
    "cost_center": "data-engineering"
  }
}
```

## Sizing Summary

| Dimension | Value | Justification |
|-----------|-------|---------------|
| Instance type | m5.xlarge | Balanced workload, network I/O focus |
| Min workers | 1 | Handle typical daily incremental load |
| Max workers | 2 | Handle peak/backfill scenarios |
| Autotermination | 20 min | Balance cost vs startup time |
| Shuffle partitions | auto | Let Spark optimize dynamically |
| Monthly cost | ~$16-25 | Jobs compute, hourly runs |

## Validation Checklist

- [x] Instance type matches workload profile (general purpose for balanced load)
- [x] Min workers handle typical load at 70% utilization
- [x] Max workers handle peak without exceeding budget
- [x] Autoscaling range is narrow (2x, not 10x)
- [x] Autotermination configured (20 minutes)
- [x] Spark shuffle optimized (auto partitions, AQE enabled)
- [x] Cost tags configured for chargeback
- [x] Monthly cost within budget (<$50 for Jobs)

## Key Takeaways

1. **Measure first:** Always run test workload to get actual metrics
2. **Formula matters:** Data → Partitions → Cores → Workers → Cost
3. **Right instance:** Match CPU/memory/IO profile to instance family
4. **Narrow autoscaling:** 2x range, not 10x+
5. **Jobs over Interactive:** 70% cost savings for scheduled workloads
