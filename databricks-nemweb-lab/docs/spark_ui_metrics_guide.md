# Spark UI Metrics Guide

This guide explains how to interpret Spark UI metrics for cluster sizing decisions.

## Accessing Spark UI

1. **From Cluster Page:** Clusters > Your Cluster > Spark UI
2. **From Notebook:** Click "Spark Jobs" link after running a cell
3. **Direct URL:** `https://<workspace>/sparkui/<cluster-id>/`

## Key Metrics for Sizing

### 1. Jobs Tab

**What to look for:**
- **Duration:** Total job execution time
- **Stages:** Number of stages (more stages = more shuffles)
- **Tasks:** Total tasks across all stages

**Sizing implications:**
- Long duration with few tasks = under-parallelized
- Many short tasks = over-parallelized (task overhead)

### 2. Stages Tab

**Key columns:**
| Column | Target | Action |
|--------|--------|--------|
| Duration | 30s - 5min | <10s: increase partition size; >10min: add cores |
| Input | - | Indicates data volume per stage |
| Output | - | Indicates data volume produced |
| Shuffle Read | Minimize | Large values = consider join optimization |
| Shuffle Write | Minimize | Large values = review partitioning strategy |

**Spill indicators:**
- **Shuffle Spill (Memory):** Data shuffled in memory
- **Shuffle Spill (Disk):** Data spilled to disk (BAD - add memory!)

### 3. Storage Tab

Shows cached RDDs and DataFrames.

**What to look for:**
- **Size in Memory:** Should fit in cluster memory
- **Fraction Cached:** 100% = good; <100% = need more memory

### 4. Executors Tab

**Critical metrics:**

| Metric | Target Range | Problem Indication |
|--------|--------------|-------------------|
| CPU Usage | 70-80% | <50% = over-provisioned |
| Memory Used | 60-70% of max | >80% = risk of OOM |
| GC Time | <10% of task time | >20% = memory pressure |
| Shuffle Read/Write | Minimize | High values = review partitioning |

### 5. SQL Tab

For DataFrame/SQL operations, shows:
- **Query plans:** Identify expensive operations
- **Scan statistics:** Data read volumes
- **Exchange statistics:** Shuffle volumes

## Diagnosing Common Issues

### Issue: High CPU, Low Memory

**Symptoms:**
- CPU utilization > 90%
- Memory utilization < 40%
- Tasks CPU-bound

**Diagnosis:** Workload is compute-intensive

**Action:**
- Use CPU-optimized instances (c5/c6i)
- Add more cores (workers)
- Don't upgrade to memory-optimized (waste of RAM)

### Issue: Low CPU, High Memory

**Symptoms:**
- CPU utilization < 50%
- Memory utilization > 80%
- Possible GC pauses

**Diagnosis:** Workload is memory-intensive

**Action:**
- Use memory-optimized instances (r5/r6i)
- Reduce partition sizes (more parallelism)
- Check for large aggregations or wide tables

### Issue: Spill to Disk

**Symptoms:**
- "Shuffle spill (disk)" > 0 in Stages tab
- Slower than expected execution
- High disk I/O

**Diagnosis:** Not enough memory for shuffle operations

**Action:**
- Increase `spark.sql.shuffle.partitions`
- Use memory-optimized instances
- Review join strategies (broadcast vs shuffle)

### Issue: Uneven Task Duration

**Symptoms:**
- Some tasks take 10x longer than others
- "Max" task duration >> "Median"
- Skewed task timeline in stage view

**Diagnosis:** Data skew

**Action:**
- Enable AQE: `spark.sql.adaptive.enabled = true`
- Use salting for skewed joins
- Increase partition count to spread skewed keys

### Issue: Too Many Small Tasks

**Symptoms:**
- 1000s of tasks per stage
- Task duration < 10 seconds
- High scheduling overhead

**Diagnosis:** Over-partitioned

**Action:**
- Reduce `spark.sql.shuffle.partitions`
- Use `coalesce()` before writes
- Increase input partition size

### Issue: Few Large Tasks

**Symptoms:**
- < 10 tasks for large data
- Task duration > 10 minutes
- Under-utilized cores

**Diagnosis:** Under-partitioned

**Action:**
- Increase `spark.sql.shuffle.partitions`
- Use `repartition()` for better distribution
- Check for partition filters being pushed down

## Spark Configuration Tuning

### Memory Settings

```python
spark.conf.set("spark.executor.memory", "8g")  # Per executor
spark.conf.set("spark.executor.memoryOverhead", "1g")  # For non-heap
spark.conf.set("spark.memory.fraction", "0.8")  # Execution + storage
spark.conf.set("spark.memory.storageFraction", "0.5")  # For caching
```

### Shuffle Settings

```python
# Automatic partitioning (DBR 7.3+)
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# Or fixed (if you know your data)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# AQE for dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Parallelism Settings

```python
# Default parallelism for RDD operations
spark.conf.set("spark.default.parallelism", "200")

# Task concurrency per executor
spark.conf.set("spark.executor.cores", "4")
```

## Quick Reference: Metrics to Instance Type

| Observation | Suggested Instance |
|-------------|-------------------|
| High CPU, low memory | c5.xlarge, c5.2xlarge |
| Low CPU, high memory | r5.xlarge, r5.2xlarge |
| Balanced utilization | m5.xlarge, m5.2xlarge |
| Heavy shuffle/disk | i3.xlarge (local SSD) |
| GPU workloads | p3.2xlarge, g4dn.xlarge |

## Monitoring Best Practices

1. **Baseline first:** Run test workload, capture metrics
2. **Monitor trends:** Track metrics over time, not just one run
3. **Alert on anomalies:** Set up alerts for spill, OOM, long GC
4. **Review weekly:** Cluster costs vs utilization

## Tools for Analysis

### Databricks SQL Analytics

```sql
-- Query cluster metrics from system tables
SELECT
  cluster_id,
  avg(cpu_utilization) as avg_cpu,
  avg(memory_utilization) as avg_memory,
  sum(dbus) as total_dbus
FROM system.compute.cluster_metrics
WHERE cluster_id = 'your-cluster-id'
GROUP BY cluster_id
```

### Ganglia Metrics (if enabled)

- Real-time CPU, memory, network graphs
- Historical trends
- Per-node breakdown

### External Tools

- Datadog Databricks integration
- Splunk for log analysis
- Custom Spark listeners for detailed metrics
