# Solution: Exercise 3 - Data Layout Optimization Comparison

## Expected Results

Performance varies based on data size, cluster configuration, and caching. Below are typical observations.

### Query 1: Single Day Filter

| Approach | Expected Behavior |
|----------|-------------------|
| Liquid Clustered | Fast - clusters by settlement_date enable efficient data skipping |
| Partitioned | Fast - partition pruning eliminates most partitions (only 1 month scanned) |

**Expected**: Similar performance. Both approaches excel at time-range filtering.

### Query 2: Single Region, Full History

| Approach | Expected Behavior |
|----------|-------------------|
| Liquid Clustered | Moderate - can skip files without NSW1 data due to clustering |
| Partitioned | Slower - must scan all partitions (no region-based partitioning) |

**Expected**: Liquid clustering may outperform, especially with more data.

### Query 3: Month Range + Region Filter

| Approach | Expected Behavior |
|----------|-------------------|
| Liquid Clustered | Fast - benefits from both clustering dimensions |
| Partitioned | Moderate - partition pruning helps, but region filter scans within partitions |

**Expected**: Liquid clustering advantage due to multi-column clustering.

### Query 4: Price Spike Detection (Full Scan)

| Approach | Expected Behavior |
|----------|-------------------|
| Liquid Clustered | Must scan all files (price not a clustering key) |
| Partitioned | Must scan all files (price not partitioned) |

**Expected**: Similar performance - neither approach optimizes for this query pattern.

## Key Insights

### When Liquid Clustering Wins

1. **Multi-dimensional filtering**: Queries filter on multiple columns (time + region)
2. **High-cardinality keys**: Timestamps have many unique values
3. **Evolving query patterns**: Can change clustering keys without rewriting data
4. **Concurrent writes**: Better handling of concurrent operations

### When Partitioning + Generated Columns Wins

1. **Strict partition elimination**: Known partition boundaries
2. **Existing infrastructure**: Already have partitioned tables
3. **Very large tables**: Partition pruning at storage level
4. **Compliance requirements**: Explicit data organization

### Optimization Recommendations for NEMWEB

For NEMWEB time-series data, **liquid clustering is recommended** because:

1. Queries typically filter by time range AND region
2. 5-minute granularity creates high-cardinality timestamps
3. Query patterns may evolve (hourly aggregations, daily reports, ad-hoc analysis)
4. Continuous streaming ingestion benefits from liquid clustering's write handling

**Recommended clustering keys:**
```sql
CLUSTER BY (settlement_date, region_id)
```

**Alternative if adding price-based queries:**
```sql
CLUSTER BY (settlement_date, region_id, rrp)
```

Note: Maximum 4 clustering keys. More keys = better multi-column queries but potentially worse single-column queries.

## Generated Columns: Still Useful

Even with liquid clustering, generated columns have value:

1. **Computed fields** - Pre-compute expensive expressions
2. **API compatibility** - Expose simplified date parts for BI tools
3. **Query simplification** - `WHERE settlement_day = 15` vs `WHERE DAY(settlement_date) = 15`

Example hybrid approach:
```sql
CREATE TABLE nemweb_hybrid (
    settlement_date TIMESTAMP,
    region_id STRING,
    total_demand_mw DOUBLE,
    -- Generated for convenience (not partitioning)
    settlement_hour INT GENERATED ALWAYS AS (HOUR(settlement_date)),
    is_peak_hour BOOLEAN GENERATED ALWAYS AS (HOUR(settlement_date) BETWEEN 7 AND 22)
)
CLUSTER BY (settlement_date, region_id)
```

## Performance Tuning Checklist

- [ ] Run `OPTIMIZE` after bulk loads
- [ ] Enable predictive optimization in Unity Catalog
- [ ] Choose clustering keys based on query patterns
- [ ] Monitor files scanned via `DESCRIBE DETAIL`
- [ ] Consider Z-ORDER only for non-clustered legacy tables
