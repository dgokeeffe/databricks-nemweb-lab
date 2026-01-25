# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 4: Fault Tolerance & Recovery (Extension)
# MAGIC
# MAGIC **Time:** 20 minutes (extension exercise)
# MAGIC
# MAGIC In this exercise, you'll implement fault tolerance patterns for production
# MAGIC data pipelines, including retry logic, checkpointing, and streaming recovery.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Implement retry with exponential backoff for transient failures
# MAGIC 2. Use checkpoint tables to resume failed batch jobs
# MAGIC 3. Configure streaming with automatic offset management
# MAGIC 4. Implement dead letter queues for unrecoverable errors
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Exercises 1-3
# MAGIC - Understanding of Spark task retry behavior

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background: Failure Modes in Data Pipelines
# MAGIC
# MAGIC | Failure Type | Cause | Recovery Strategy |
# MAGIC |--------------|-------|-------------------|
# MAGIC | Transient | Network timeout, rate limit | Retry with backoff |
# MAGIC | Task failure | OOM, executor lost | Spark auto-retry |
# MAGIC | Job failure | Driver crash, cluster termination | Checkpoint + resume |
# MAGIC | Data error | Invalid records, schema mismatch | Dead letter queue |
# MAGIC
# MAGIC **Key Principle:** Your data source must be **idempotent** - re-running the same
# MAGIC partition should produce the same results without side effects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import time

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Retry with Exponential Backoff (5 minutes)
# MAGIC
# MAGIC Transient failures (network timeouts, rate limits) should be retried automatically.
# MAGIC
# MAGIC ### TODO 4.1: Implement retry logic

# COMMAND ----------

def fetch_with_retry(url: str, max_retries: int = 3, base_delay: float = 1.0) -> bytes:
    """
    Fetch URL with exponential backoff retry.

    TODO 4.1: Complete the retry logic
    - On failure, wait base_delay * (2 ** attempt) seconds
    - Don't retry on 404 (data doesn't exist)
    - Log each retry attempt
    """
    import urllib.request
    from urllib.error import HTTPError, URLError

    last_error = None

    for attempt in range(max_retries):
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "NemwebLab/1.0"}
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()

        except (HTTPError, URLError) as e:
            last_error = e

            # TODO 4.1a: Don't retry on 404
            # YOUR CODE HERE

            if attempt < max_retries - 1:
                # TODO 4.1b: Calculate delay with exponential backoff
                delay = None  # YOUR CODE HERE: base_delay * (2 ** attempt)

                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"Retrying in {delay:.1f} seconds...")

                # TODO 4.1c: Wait before retry
                # YOUR CODE HERE

    raise last_error


# Test retry logic
test_url = "https://httpstat.us/503"  # Always returns 503
try:
    fetch_with_retry(test_url, max_retries=2, base_delay=0.5)
except Exception as e:
    print(f"Expected failure after retries: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Checkpoint-Based Recovery for Batch Jobs (5 minutes)
# MAGIC
# MAGIC For batch jobs, track completed partitions in a Delta table so failed jobs
# MAGIC can resume from where they left off.
# MAGIC
# MAGIC ### Checkpoint Table Schema

# COMMAND ----------

# Create checkpoint table
checkpoint_table = "nemweb_checkpoints"

checkpoint_schema = StructType([
    StructField("partition_id", StringType(), False),
    StructField("region", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("row_count", StringType(), True),
])

# Create empty checkpoint table if not exists
spark.createDataFrame([], checkpoint_schema).write.format("delta").mode("ignore").saveAsTable(checkpoint_table)

print(f"Checkpoint table: {checkpoint_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.2: Implement checkpoint-aware reading

# COMMAND ----------

def get_completed_partitions(checkpoint_table: str) -> set:
    """
    Load completed partition IDs from checkpoint table.

    TODO 4.2a: Query the checkpoint table and return set of partition_ids
    """
    try:
        df = spark.read.table(checkpoint_table)
        # YOUR CODE HERE: Extract partition_ids as a set
        return set()
    except Exception as e:
        print(f"Could not read checkpoints: {e}")
        return set()


def mark_partition_complete(
    checkpoint_table: str,
    partition_id: str,
    region: str,
    start_date: str,
    end_date: str,
    row_count: int
):
    """
    Mark a partition as completed in the checkpoint table.

    TODO 4.2b: Insert a record into the checkpoint table
    """
    checkpoint_record = spark.createDataFrame([{
        "partition_id": partition_id,
        "region": region,
        "start_date": start_date,
        "end_date": end_date,
        "completed_at": datetime.now(),
        "row_count": str(row_count),
    }])

    # YOUR CODE HERE: Append to checkpoint table


def read_with_checkpoints(checkpoint_table: str, regions: list, start_date: str, end_date: str):
    """
    Read NEMWEB data, skipping already-completed partitions.

    TODO 4.2c: Filter out completed partitions before processing
    """
    import hashlib

    completed = get_completed_partitions(checkpoint_table)
    print(f"Found {len(completed)} completed partitions")

    for region in regions:
        # Generate partition ID (same logic as NemwebPartition)
        id_string = f"DISPATCHREGIONSUM:{region}:{start_date}:{end_date}"
        partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]

        # TODO 4.2c: Skip if already completed
        # YOUR CODE HERE

        print(f"Processing partition {partition_id} ({region})")

        # Simulate processing
        row_count = 100  # Would be actual row count

        # Mark complete after successful processing
        mark_partition_complete(
            checkpoint_table, partition_id, region,
            start_date, end_date, row_count
        )


# Test checkpoint logic
test_regions = ["NSW1", "VIC1"]
read_with_checkpoints(checkpoint_table, test_regions, "2024-01-01", "2024-01-07")

# Run again - should skip completed partitions
print("\n--- Second run (should skip completed) ---")
read_with_checkpoints(checkpoint_table, test_regions, "2024-01-01", "2024-01-07")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Streaming with Automatic Recovery (5 minutes)
# MAGIC
# MAGIC Streaming provides the cleanest recovery model - Spark manages offsets automatically.
# MAGIC
# MAGIC ### TODO 4.3: Configure streaming with checkpoints

# COMMAND ----------

# MAGIC %md
# MAGIC The NEMWEB data source now supports streaming with `simpleStreamReader()`.
# MAGIC Spark automatically manages offsets when you specify a checkpoint location.

# COMMAND ----------

# Register our custom data source
# (In production, this would be done via spark.dataSource.register)

# Example streaming configuration
streaming_config = """
# Streaming read with automatic checkpointing
query = (spark.readStream
    .format("nemweb")
    .option("table", "DISPATCHREGIONSUM")
    .option("regions", "NSW1,VIC1")
    .load()
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/nemweb_stream")  # Spark manages this
    .trigger(processingTime="5 minutes")  # Run every 5 minutes
    .toTable("nemweb_bronze_streaming"))

# To stop: query.stop()
# To resume: Just restart - Spark reads from last checkpoint automatically
"""

print(streaming_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Recovery Behavior
# MAGIC
# MAGIC | Scenario | Behavior |
# MAGIC |----------|----------|
# MAGIC | Normal stop | Commits offset, resumes from last position |
# MAGIC | Cluster crash | Resumes from last committed offset |
# MAGIC | Schema change | May require checkpoint reset |
# MAGIC | Code change | Usually safe to resume |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Dead Letter Queue Pattern (5 minutes)
# MAGIC
# MAGIC For records that can't be processed, write them to a DLQ for later investigation
# MAGIC rather than failing the entire job.
# MAGIC
# MAGIC ### TODO 4.4: Implement dead letter queue

# COMMAND ----------

from pyspark.sql.functions import struct, to_json

def process_with_dlq(df, dlq_table: str):
    """
    Process DataFrame, routing bad records to DLQ.

    TODO 4.4: Implement DLQ pattern
    - Good records go to main output
    - Bad records (e.g., null REGIONID) go to DLQ with error info
    """
    # Define what makes a record "bad"
    is_valid = (
        col("REGIONID").isNotNull() &
        col("REGIONID").isin("NSW1", "VIC1", "QLD1", "SA1", "TAS1") &
        (col("TOTALDEMAND").cast("double") > 0)
    )

    # TODO 4.4a: Split into good and bad records
    good_records = df.filter(is_valid)
    bad_records = df.filter(~is_valid)

    # TODO 4.4b: Add error metadata to bad records
    bad_with_metadata = (bad_records
        .withColumn("_error_reason", lit("Invalid region or demand"))
        .withColumn("_error_timestamp", current_timestamp())
        .withColumn("_original_data", to_json(struct("*")))
    )

    # TODO 4.4c: Write bad records to DLQ table
    # YOUR CODE HERE: bad_with_metadata.write...

    print(f"Good records: {good_records.count()}")
    print(f"Bad records sent to DLQ: {bad_records.count()}")

    return good_records


# Test with sample data including bad records
test_data = [
    ("2024-01-01 00:05:00", "NSW1", "7500.5"),
    ("2024-01-01 00:05:00", "VIC1", "5200.3"),
    ("2024-01-01 00:05:00", "INVALID", "1000.0"),  # Bad region
    ("2024-01-01 00:05:00", "SA1", "-500"),  # Negative demand
    (None, "QLD1", "6000.0"),  # Null timestamp
]

test_df = spark.createDataFrame(test_data, ["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"])
good_df = process_with_dlq(test_df, "nemweb_dlq")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Putting It All Together
# MAGIC
# MAGIC Here's a complete fault-tolerant pipeline using all patterns:

# COMMAND ----------

def run_fault_tolerant_pipeline(
    checkpoint_table: str,
    dlq_table: str,
    target_table: str,
    regions: list,
    start_date: str,
    end_date: str
):
    """
    Production-ready pipeline with:
    - Checkpoint-based resume for batch
    - Retry with backoff for transient errors
    - DLQ for bad records
    """
    print("=" * 60)
    print("FAULT-TOLERANT NEMWEB PIPELINE")
    print("=" * 60)

    # Step 1: Read with checkpointing (skip completed partitions)
    print("\n1. Reading data with checkpoint awareness...")
    df = (spark.read
          .format("nemweb")
          .option("table", "DISPATCHREGIONSUM")
          .option("regions", ",".join(regions))
          .option("start_date", start_date)
          .option("end_date", end_date)
          .option("checkpoint_table", checkpoint_table)
          .option("skip_completed", "true")
          .load())

    initial_count = df.count()
    print(f"   Loaded {initial_count} rows")

    if initial_count == 0:
        print("   No new data to process")
        return

    # Step 2: Route bad records to DLQ
    print("\n2. Validating and routing bad records to DLQ...")
    good_df = process_with_dlq(df, dlq_table)

    # Step 3: Write to target table
    print(f"\n3. Writing {good_df.count()} valid rows to {target_table}...")
    (good_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table))

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


# Example usage (commented out to avoid errors in lab)
# run_fault_tolerant_pipeline(
#     checkpoint_table="nemweb_checkpoints",
#     dlq_table="nemweb_dlq",
#     target_table="nemweb_bronze",
#     regions=["NSW1", "VIC1", "QLD1"],
#     start_date="2024-01-01",
#     end_date="2024-01-31"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checklist
# MAGIC
# MAGIC - [ ] Retry logic implements exponential backoff
# MAGIC - [ ] 404 errors are not retried (data doesn't exist)
# MAGIC - [ ] Checkpoint table tracks completed partitions
# MAGIC - [ ] Re-running skips already-completed partitions
# MAGIC - [ ] Bad records route to DLQ with error metadata
# MAGIC - [ ] Good records write to target table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Recovery Patterns
# MAGIC
# MAGIC | Pattern | Use When | Complexity | Recovery Time |
# MAGIC |---------|----------|------------|---------------|
# MAGIC | Retry w/ backoff | Transient HTTP errors | Low | Seconds |
# MAGIC | Spark task retry | Executor failures | Built-in | Seconds-minutes |
# MAGIC | Checkpoint table | Batch job failures | Medium | Resume instantly |
# MAGIC | Streaming checkpoint | Continuous pipelines | Low | Automatic |
# MAGIC | Dead letter queue | Bad data records | Medium | Manual review |
# MAGIC
# MAGIC **Recommendation:** For NEMWEB-style data:
# MAGIC - Use **streaming** for near-real-time ingestion (automatic recovery)
# MAGIC - Use **batch + checkpoints** for historical backfill
# MAGIC - Always use **DLQ** to avoid losing visibility into data issues

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Custom Data Sink - Price Alerts
# MAGIC
# MAGIC Just as you can build custom data sources (readers), you can build custom sinks (writers).
# MAGIC
# MAGIC The lab includes `PriceAlertDataSource` - a sink that triggers alerts when
# MAGIC electricity prices spike above a threshold.

# COMMAND ----------

# Example: Price Alert Sink
alert_sink_example = """
# Register the alert sink
from nemweb_sink import PriceAlertDataSource
spark.dataSource.register(PriceAlertDataSource)

# Read NEMWEB price data and write to alert sink
(spark.read
    .format("nemweb")
    .option("table", "DISPATCHPRICE")
    .option("regions", "NSW1,VIC1,SA1")
    .load()
    .write
    .format("nemweb_alerts")
    .option("price_threshold", "300")      # Alert above $300/MWh
    .option("demand_threshold", "10000")   # Alert above 10GW demand
    .option("webhook_url", "https://hooks.slack.com/...")
    .option("alert_mode", "both")          # Log and webhook
    .mode("append")
    .save())
"""

print(alert_sink_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Sink Architecture
# MAGIC
# MAGIC ```
# MAGIC DataSource (sink)
# MAGIC     └── writer(schema, overwrite) → DataSourceWriter
# MAGIC             └── write(iterator) → WriterCommitMessage
# MAGIC ```
# MAGIC
# MAGIC Key methods:
# MAGIC - `name()`: Returns format identifier (e.g., "nemweb_alerts")
# MAGIC - `writer()`: Creates a writer instance
# MAGIC - `write()`: Processes rows, returns commit message
# MAGIC
# MAGIC The sink in `nemweb_sink.py` demonstrates:
# MAGIC - Threshold-based alerting
# MAGIC - Webhook integration
# MAGIC - Metrics publishing (InfluxDB line protocol)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC You've completed the fault tolerance extension! Key takeaways:
# MAGIC
# MAGIC 1. **Idempotency is essential** - Design operations that can safely re-run
# MAGIC 2. **Streaming simplifies recovery** - Spark manages offsets automatically
# MAGIC 3. **Checkpoints enable resume** - Track progress at partition granularity
# MAGIC 4. **DLQs preserve visibility** - Don't silently drop bad records
# MAGIC 5. **Custom sinks** - Extend the pattern to writers for alerts, metrics, etc.
