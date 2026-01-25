# Facilitator Guide: NEMWEB Lab

This guide helps facilitators run the NEMWEB Custom Data Source & Cluster Sizing lab.

## Overview

| Item | Details |
|------|---------|
| Duration | 40 minutes (core) + 20 minutes (extension) |
| Participants | 5-20 experienced Databricks engineers |
| Format | Hands-on coding exercises |
| Prerequisites | DBR 15.4+ cluster, internet access |

## Pre-Session Checklist

### 1 Week Before

- [ ] Verify workspace has DBR 15.4+ available
- [ ] Test NEMWEB connectivity from workspace
- [ ] Import lab materials to shared location
- [ ] Create cluster template for participants
- [ ] Send pre-work email with setup instructions

### Day Before

- [ ] Verify sample data loads correctly
- [ ] Run through all exercises end-to-end
- [ ] Prepare backup plan for connectivity issues
- [ ] Test screen sharing setup

### Day Of

- [ ] Start clusters 15 min before session
- [ ] Have solutions ready to share if needed
- [ ] Open Spark UI for live demos

## Session Agenda

### Introduction (5 min)

**Key points to cover:**

1. Why custom data sources matter
   - Before: Complex Java/Scala DataSourceV2
   - After: Simple Python classes (DBR 15.4+)

2. Why cluster sizing matters
   - Common problem: 50-300% over-provisioning
   - Root cause: Intuition-based decisions

3. Lab objectives
   - Build a production-ready data source
   - Apply data-driven sizing methodology

### Exercise 1: Custom Data Source (15 min)

**Setup (2 min):**
- Ensure all participants have cluster attached
- Verify setup notebook passes

**Teaching points:**

1. **Schema definition (TODO 1.1)**
   - Show MMS Data Model Report briefly
   - Emphasize typed schemas vs inferSchema
   - Common mistake: Missing nullable fields

2. **Partition planning (TODO 1.2)**
   - Partitions = parallelism
   - One partition per region enables 5-way parallel
   - Extension: Date-based partitioning for larger ranges

3. **DataSource class (TODO 1.3)**
   - `name()` is the format identifier
   - `schema()` returns StructType
   - `reader()` creates the reader instance

**Common issues:**
- Import errors: Check sys.path includes src/
- Schema mismatch: Ensure field names match CSV headers exactly

**Time check:** Participants should complete by 0:20

### Exercise 2: Lakeflow Pipeline (10 min)

**Teaching points:**

1. **Bronze layer (TODO 2.1)**
   - Custom sources work like any other format
   - Always add ingestion metadata (_ingested_at)

2. **Expectations (TODO 2.2)**
   - `expect_or_drop` vs `expect_or_fail`
   - Real-world: Invalid region IDs, negative demand

3. **Gold aggregations (TODO 2.3)**
   - Standard groupBy/agg patterns
   - Note: This is just logic validation, not real DLT run

**Common issues:**
- Syntax errors in decorators
- Missing imports

**Time check:** Participants should complete by 0:30

### Exercise 3: Cluster Sizing (15 min)

**This is the most important exercise. Slow down here.**

**Teaching points:**

1. **Partition calculation (TODO 3.1)**
   - Formula: data_size / target_partition_size
   - Target: 64-128 MB partitions
   - Show: Why too small = overhead, too large = skew

2. **Core calculation (TODO 3.2)**
   - Formula: total_task_time / target_duration
   - Always add 20% overhead
   - Demo: Show Spark UI task timeline

3. **Instance selection (TODO 3.3)**
   - CPU-bound: c5/c6i family
   - Memory-bound: r5/r6i family
   - Balanced: m5/m6i family
   - Ask: What makes NEMWEB pipeline CPU vs memory bound?

4. **Autoscaling (TODO 3.4)**
   - Anti-pattern: min=1, max=32
   - Better: Narrow range based on actual patterns
   - Rule: Max should be 2-3x min, not 10x+

5. **Cost estimation (TODO 3.5)**
   - DBU = workers × DBU/hour × duration
   - Jobs compute = $0.15/DBU (cheaper)
   - All-Purpose = $0.55/DBU (interactive)

**Discussion prompts:**
- "What instance type would you choose and why?"
- "How would your answer change for a streaming workload?"
- "What's the cost difference between Jobs and All-Purpose?"

## Common Participant Questions

### Q: Why not just use inferSchema?

**A:** inferSchema scans data twice (once for schema, once for reading) and may infer incorrect types (strings for timestamps). Explicit schemas are more efficient and reliable.

### Q: Can I use this pattern for REST APIs?

**A:** Yes! Replace HTTP/CSV logic with your API calls. Consider rate limiting, pagination, and error handling.

### Q: How do I handle partitioning for very large date ranges?

**A:** Add date-based partitioning. Create partitions for region × date combinations. This is shown in the extension challenge.

### Q: What if my workload has high memory requirements?

**A:** Switch to r5/r6i (memory-optimized) instances. Also check for:
- Unnecessary `collect()` operations
- Large broadcast variables
- Data skew causing memory pressure on specific tasks

### Q: How do I know if I'm over-provisioned?

**A:** Check Spark UI:
- CPU utilization < 50% = over-provisioned
- Tasks completing in < 10 seconds = too many partitions
- Workers sitting idle = reduce max_workers

## Extension Topics (If Time Permits)

### Date-Based Partitioning (10 min)

```python
def partitions(self):
    partitions = []
    for region in self.regions:
        for date in date_range(self.start_date, self.end_date):
            partitions.append(NemwebPartition(region, date, date))
    return partitions
```

### Real HTTP Fetching (10 min)

- Show nemweb_utils.py implementation
- Discuss error handling strategies
- Demo actual NEMWEB data fetch

### Streaming Pattern (10 min)

- Python Data Source API supports streaming
- Implement `streamReader()` method
- Configure with checkpointing

## Wrap-Up (5 min)

**Key takeaways:**

1. Custom data sources are now simple Python classes
2. Sizing should be data-driven, not intuition-based
3. The formula: Data → Partitions → Cores → Instances → Cost

**Next steps for participants:**
- Apply methodology to their production workloads
- Explore additional NEMWEB tables
- Review cluster costs with new knowledge

## Resources to Share

- Lab repository URL
- Python Data Source API documentation
- Cluster sizing best practices guide
- AEMO NEMWEB documentation
