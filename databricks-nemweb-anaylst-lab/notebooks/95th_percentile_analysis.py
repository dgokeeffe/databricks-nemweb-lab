# Databricks notebook source
# MAGIC %md
# MAGIC # NEM Price 95th Percentile Analysis
# MAGIC
# MAGIC This notebook demonstrates PySpark analysis for NEM price data.
# MAGIC Use this as an extension to the 60-minute demo if time permits.
# MAGIC
# MAGIC **Purpose**: Calculate 95th percentile prices by region - useful for risk analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration
CATALOG = "agldata"
SCHEMA = "trading"
TABLE = "curated_nem_prices"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Read the curated NEM prices table
df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")

# Show schema
df.printSchema()

# COMMAND ----------

# Quick preview
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 95th Percentile Price Analysis
# MAGIC
# MAGIC The 95th percentile price is useful for:
# MAGIC - Understanding "typical worst case" prices
# MAGIC - Risk exposure calculations
# MAGIC - Contract pricing benchmarks

# COMMAND ----------

# Calculate percentiles by region
percentile_analysis = df.groupBy("region_id").agg(
    F.count("*").alias("intervals"),
    F.round(F.avg("rrp"), 2).alias("avg_price"),
    F.round(F.expr("percentile(rrp, 0.5)"), 2).alias("median_price"),
    F.round(F.expr("percentile(rrp, 0.95)"), 2).alias("p95_price"),
    F.round(F.expr("percentile(rrp, 0.99)"), 2).alias("p99_price"),
    F.round(F.max("rrp"), 2).alias("max_price"),
).orderBy("p95_price", ascending=False)

display(percentile_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hourly 95th Percentile Pattern
# MAGIC
# MAGIC Understanding how the 95th percentile varies by hour helps identify high-risk periods.

# COMMAND ----------

# Hourly percentile pattern by region
hourly_percentiles = (
    df.withColumn("hour", F.hour("interval_start"))
    .groupBy("region_id", "hour")
    .agg(
        F.round(F.avg("rrp"), 2).alias("avg_price"),
        F.round(F.expr("percentile(rrp, 0.95)"), 2).alias("p95_price"),
    )
    .orderBy("region_id", "hour")
)

display(hourly_percentiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Spike Analysis
# MAGIC
# MAGIC Identify intervals where price exceeded the 95th percentile threshold.

# COMMAND ----------

# Get regional P95 thresholds
p95_thresholds = (
    df.groupBy("region_id")
    .agg(F.expr("percentile(rrp, 0.95)").alias("p95_threshold"))
)

# Join back to identify spike intervals
spikes = (
    df.join(p95_thresholds, "region_id")
    .filter(F.col("rrp") > F.col("p95_threshold"))
    .select(
        "interval_start",
        "date",
        "region_id",
        F.round("rrp", 2).alias("price"),
        F.round("p95_threshold", 2).alias("p95_threshold"),
        "demand_mw",
        "is_peak"
    )
    .orderBy("price", ascending=False)
)

display(spikes.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spike Summary by Region

# COMMAND ----------

# Count spikes by region and peak/off-peak
spike_summary = (
    spikes.groupBy("region_id", "is_peak")
    .agg(
        F.count("*").alias("spike_count"),
        F.round(F.avg("price"), 2).alias("avg_spike_price"),
        F.round(F.max("price"), 2).alias("max_spike_price"),
        F.round(F.avg("demand_mw"), 0).alias("avg_demand_during_spike"),
    )
    .orderBy("region_id", "is_peak")
)

display(spike_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results
# MAGIC
# MAGIC Optionally save results to a new table for further analysis.

# COMMAND ----------

# Uncomment to save results
# percentile_analysis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.nem_price_percentiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **P95 prices vary significantly by region** - SA1 typically has highest volatility
# MAGIC 2. **Peak hours (17:00-21:00) show elevated P95** - aligns with demand patterns
# MAGIC 3. **Price spikes correlate with high demand** - useful for risk forecasting
# MAGIC
# MAGIC This analysis could feed into:
# MAGIC - Trading risk models
# MAGIC - Contract pricing benchmarks
# MAGIC - Demand response triggers
