# Cluster Sizing Worksheet

Use this worksheet to document your sizing analysis for the NEMWEB pipeline.

## 1. Workload Characteristics

### Data Volume

| Metric | Value | Notes |
|--------|-------|-------|
| Historical data size | _____ MB | 6-month backfill |
| Daily incremental size | _____ MB | Per day |
| Row count (historical) | _____ rows | |
| Row count (daily) | _____ rows | |

### Processing Pattern

- [ ] Batch (scheduled)
- [ ] Streaming (continuous)
- [ ] Hybrid (batch + streaming)

**Schedule:** ___________________ (e.g., hourly, daily)

**SLA:** Process within _____ minutes

## 2. Partition Analysis

### Calculation

```
Target partition size: _____ MB (recommended: 64-128 MB)

Historical partitions = data_size / target_partition_size
                      = _____ / _____ = _____ partitions

Daily partitions = data_size / target_partition_size
                 = _____ / _____ = _____ partitions
```

### Recommended Shuffle Partitions

- [ ] Auto (let Spark decide)
- [ ] Fixed: _____

## 3. Core Requirements

### From Test Run Metrics

| Metric | Value |
|--------|-------|
| Average task duration | _____ seconds |
| Total tasks | _____ |
| Shuffle read | _____ MB |
| Shuffle write | _____ MB |
| Spill to disk | _____ MB |

### Core Calculation

```
Total task time = tasks × avg_duration
                = _____ × _____ = _____ seconds

Target completion = _____ minutes = _____ seconds

Minimum cores = total_task_time / target_completion
              = _____ / _____ = _____ cores

With 20% overhead = _____ × 1.2 = _____ cores
```

## 4. Instance Type Selection

### Workload Profile (1-5 scale)

| Dimension | Score | Indicators |
|-----------|-------|------------|
| CPU intensity | ___ / 5 | Heavy parsing, complex transforms |
| Memory intensity | ___ / 5 | Wide tables, large aggregations, joins |
| I/O intensity | ___ / 5 | Network calls, large shuffle |

### Recommended Instance Family

Based on profile:
- [ ] General purpose (m5/m6i) - Balanced
- [ ] CPU optimized (c5/c6i) - High CPU, low memory
- [ ] Memory optimized (r5/r6i) - Large aggregations, wide tables

**Selected instance type:** _____________________

| Property | Value |
|----------|-------|
| vCPUs | _____ |
| Memory | _____ GB |
| DBU/hour | _____ |

## 5. Autoscaling Configuration

### Load Analysis

| Pattern | Cores Needed |
|---------|--------------|
| Typical load | _____ cores |
| Peak load | _____ cores |
| Backfill load | _____ cores |

### Worker Calculation

```
Cores per worker = _____ (from instance type)

Min workers = ceil(typical_cores / cores_per_worker)
            = ceil(_____ / _____) = _____ workers

Max workers = ceil(peak_cores / cores_per_worker)
            = ceil(_____ / _____) = _____ workers
```

### Final Configuration

| Setting | Value |
|---------|-------|
| Min workers | _____ |
| Max workers | _____ |
| Autotermination | _____ minutes |

## 6. Cost Estimation

### Usage Assumptions

| Metric | Value |
|--------|-------|
| Runs per day | _____ |
| Average run duration | _____ hours |
| Average workers per run | _____ |
| Days per month | 30 |

### DBU Calculation

```
DBU per run = workers × DBU_per_hour × duration
            = _____ × _____ × _____ = _____ DBU

Daily DBU = runs × DBU_per_run
          = _____ × _____ = _____ DBU

Monthly DBU = daily × 30
            = _____ × 30 = _____ DBU
```

### Cost Estimation

| Compute Type | Rate ($/DBU) | Monthly Cost |
|--------------|--------------|--------------|
| Jobs | $0.15 | $_____ |
| All-Purpose | $0.55 | $_____ |

## 7. Final Configuration Summary

```json
{
  "cluster_name": "_____",
  "node_type_id": "_____",
  "autoscale": {
    "min_workers": _____,
    "max_workers": _____
  },
  "autotermination_minutes": _____
}
```

## 8. Validation Checklist

- [ ] Instance type matches workload profile
- [ ] Min workers handle typical load at 70% CPU utilization
- [ ] Max workers handle peak without exceeding budget
- [ ] Autotermination prevents idle costs
- [ ] Monthly cost within budget

## Notes

_____________________________________________________________________

_____________________________________________________________________

_____________________________________________________________________
