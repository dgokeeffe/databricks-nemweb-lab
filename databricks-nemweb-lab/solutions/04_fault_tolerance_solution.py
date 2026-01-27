# Databricks notebook source
# MAGIC %md
# MAGIC # Solution: Exercise 4 - Fault Tolerance & Recovery
# MAGIC
# MAGIC Complete solutions for the fault tolerance exercise.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import time
import hashlib

# Import spark from Databricks SDK for IDE support and local development
from databricks.sdk.runtime import spark

# Configuration widgets
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("schema", "nemweb_lab", "Schema Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Using catalog: {CATALOG}, schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 4.1: Retry with Exponential Backoff

# COMMAND ----------

def fetch_with_retry(url: str, max_retries: int = 3, base_delay: float = 1.0) -> bytes:
    """
    Fetch URL with exponential backoff retry.

    SOLUTION 4.1: Complete retry implementation
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

            # SOLUTION 4.1a: Don't retry on 404
            if isinstance(e, HTTPError) and e.code == 404:
                raise  # 404 means data doesn't exist, don't retry

            if attempt < max_retries - 1:
                # SOLUTION 4.1b: Calculate delay with exponential backoff
                delay = base_delay * (2 ** attempt)

                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"Retrying in {delay:.1f} seconds...")

                # SOLUTION 4.1c: Wait before retry
                time.sleep(delay)

    raise last_error


# Test retry logic
print("Testing retry logic with 503 endpoint...")
test_url = "https://httpstat.us/503"
try:
    fetch_with_retry(test_url, max_retries=2, base_delay=0.5)
except Exception as e:
    print(f"Expected failure after retries: {type(e).__name__}")

print("\nTesting 404 (should not retry)...")
test_url_404 = "https://httpstat.us/404"
try:
    fetch_with_retry(test_url_404, max_retries=3, base_delay=1.0)
except Exception as e:
    print(f"Immediate failure (no retry): {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 4.2: Checkpoint-Based Recovery

# COMMAND ----------

# Create checkpoint table
checkpoint_table = f"{CATALOG}.{SCHEMA}.nemweb_checkpoints_solution"

checkpoint_schema = StructType([
    StructField("partition_id", StringType(), False),
    StructField("region", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("row_count", StringType(), True),
])

spark.createDataFrame([], checkpoint_schema).write.format("delta").mode("ignore").saveAsTable(checkpoint_table)

# COMMAND ----------

def get_completed_partitions(checkpoint_table: str) -> set:
    """
    Load completed partition IDs from checkpoint table.

    SOLUTION 4.2a: Query checkpoint table
    """
    try:
        df = spark.read.table(checkpoint_table)
        # SOLUTION: Extract partition_ids as a set
        return set(
            row.partition_id
            for row in df.select("partition_id").collect()
        )
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

    SOLUTION 4.2b: Insert checkpoint record
    """
    checkpoint_record = spark.createDataFrame([{
        "partition_id": partition_id,
        "region": region,
        "start_date": start_date,
        "end_date": end_date,
        "completed_at": datetime.now(),
        "row_count": str(row_count),
    }])

    # SOLUTION: Append to checkpoint table
    checkpoint_record.write.format("delta").mode("append").saveAsTable(checkpoint_table)


def read_with_checkpoints(checkpoint_table: str, regions: list, start_date: str, end_date: str):
    """
    Read NEMWEB data, skipping already-completed partitions.

    SOLUTION 4.2c: Filter completed partitions
    """
    completed = get_completed_partitions(checkpoint_table)
    print(f"Found {len(completed)} completed partitions")

    processed_count = 0

    for region in regions:
        # Generate partition ID (same logic as NemwebPartition)
        id_string = f"DISPATCHREGIONSUM:{region}:{start_date}:{end_date}"
        partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]

        # SOLUTION 4.2c: Skip if already completed
        if partition_id in completed:
            print(f"Skipping partition {partition_id} ({region}) - already completed")
            continue

        print(f"Processing partition {partition_id} ({region})")

        # Simulate processing
        row_count = 100  # Would be actual row count

        # Mark complete after successful processing
        mark_partition_complete(
            checkpoint_table, partition_id, region,
            start_date, end_date, row_count
        )
        processed_count += 1

    print(f"Processed {processed_count} partitions")
    return processed_count


# Test checkpoint logic
print("=== First run ===")
test_regions = ["NSW1", "VIC1", "QLD1"]
count1 = read_with_checkpoints(checkpoint_table, test_regions, "2024-01-01", "2024-01-07")

print("\n=== Second run (should skip all) ===")
count2 = read_with_checkpoints(checkpoint_table, test_regions, "2024-01-01", "2024-01-07")

print(f"\nFirst run: {count1} partitions, Second run: {count2} partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 4.3: Streaming Configuration
# MAGIC
# MAGIC The streaming reader is implemented in `nemweb_datasource.py` as `NemwebStreamReader`.

# COMMAND ----------

# Complete streaming pipeline example
streaming_example = """
# Production streaming pipeline with automatic recovery

from pyspark.sql.functions import col, current_timestamp

# Register the data source
spark.dataSource.register(NemwebDataSource)

# Start streaming query
query = (spark.readStream
    .format("nemweb")
    .option("table", "DISPATCHREGIONSUM")
    .option("regions", "NSW1,VIC1,QLD1,SA1,TAS1")
    .load()
    # Add processing timestamp
    .withColumn("_processed_at", current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    # Spark manages offsets in this location
    .option("checkpointLocation", "/checkpoints/nemweb_production")
    # Process every 5 minutes
    .trigger(processingTime="5 minutes")
    .toTable("nemweb_bronze"))

# Monitor the query
print(f"Query ID: {query.id}")
print(f"Status: {query.status}")

# Recovery scenarios:
# 1. Stop query: query.stop()
#    - Commits current offset
#    - Next start resumes from committed offset
#
# 2. Cluster crash:
#    - Restart cluster, run same code
#    - Spark reads last committed offset from checkpointLocation
#    - Resumes processing from where it left off
#
# 3. Code change:
#    - Usually safe to restart with same checkpointLocation
#    - Schema changes may require new checkpoint
"""

print(streaming_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution 4.4: Dead Letter Queue

# COMMAND ----------

# Create DLQ table
dlq_table = f"{CATALOG}.{SCHEMA}.nemweb_dlq_solution"

dlq_schema = StructType([
    StructField("SETTLEMENTDATE", StringType(), True),
    StructField("REGIONID", StringType(), True),
    StructField("TOTALDEMAND", StringType(), True),
    StructField("_error_reason", StringType(), True),
    StructField("_error_timestamp", TimestampType(), True),
    StructField("_original_data", StringType(), True),
])

spark.createDataFrame([], dlq_schema).write.format("delta").mode("ignore").saveAsTable(dlq_table)

# COMMAND ----------

def process_with_dlq(df, dlq_table: str):
    """
    Process DataFrame, routing bad records to DLQ.

    SOLUTION 4.4: Complete DLQ implementation
    """
    # Define what makes a record "bad"
    is_valid = (
        col("REGIONID").isNotNull() &
        col("REGIONID").isin("NSW1", "VIC1", "QLD1", "SA1", "TAS1") &
        (col("TOTALDEMAND").cast("double") > 0)
    )

    # SOLUTION 4.4a: Split into good and bad records
    good_records = df.filter(is_valid)
    bad_records = df.filter(~is_valid)

    bad_count = bad_records.count()

    if bad_count > 0:
        # SOLUTION 4.4b: Add error metadata to bad records
        bad_with_metadata = (bad_records
            .withColumn("_error_reason",
                when(col("REGIONID").isNull(), "NULL_REGION")
                .when(~col("REGIONID").isin("NSW1", "VIC1", "QLD1", "SA1", "TAS1"), "INVALID_REGION")
                .when(col("TOTALDEMAND").cast("double") <= 0, "INVALID_DEMAND")
                .otherwise("UNKNOWN"))
            .withColumn("_error_timestamp", current_timestamp())
            .withColumn("_original_data", to_json(struct(
                col("SETTLEMENTDATE"),
                col("REGIONID"),
                col("TOTALDEMAND")
            )))
        )

        # SOLUTION 4.4c: Write bad records to DLQ table
        (bad_with_metadata
            .select("SETTLEMENTDATE", "REGIONID", "TOTALDEMAND",
                    "_error_reason", "_error_timestamp", "_original_data")
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(dlq_table))

        print(f"Routed {bad_count} bad records to DLQ: {dlq_table}")

    good_count = good_records.count()
    print(f"Good records: {good_count}")

    return good_records


# Need to import when for the solution
from pyspark.sql.functions import when

# Test with sample data including bad records
test_data = [
    ("2024-01-01 00:05:00", "NSW1", "7500.5"),  # Good
    ("2024-01-01 00:05:00", "VIC1", "5200.3"),  # Good
    ("2024-01-01 00:05:00", "INVALID", "1000.0"),  # Bad: invalid region
    ("2024-01-01 00:05:00", "SA1", "-500"),  # Bad: negative demand
    (None, "QLD1", "6000.0"),  # Good (null timestamp is allowed)
    ("2024-01-01 00:10:00", None, "3000.0"),  # Bad: null region
]

test_df = spark.createDataFrame(test_data, ["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"])

print("Processing test data...")
good_df = process_with_dlq(test_df, dlq_table)

print("\nGood records:")
good_df.show()

print("\nDLQ contents:")
spark.read.table(dlq_table).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Fault-Tolerant Pipeline

# COMMAND ----------

def run_fault_tolerant_pipeline(
    checkpoint_table: str,
    dlq_table: str,
    target_table: str,
    regions: list,
    start_date: str,
    end_date: str,
    use_sample: bool = True
):
    """
    Production-ready pipeline with all fault tolerance patterns.

    SOLUTION: Complete implementation
    """
    print("=" * 60)
    print("FAULT-TOLERANT NEMWEB PIPELINE")
    print("=" * 60)

    # Step 1: Check for completed partitions
    print("\n1. Checking checkpoint status...")
    completed = get_completed_partitions(checkpoint_table)
    print(f"   Found {len(completed)} completed partitions")

    # Step 2: Read data (with retry built into data source)
    print("\n2. Reading data...")
    df = (spark.read
          .format("nemweb")
          .option("table", "DISPATCHREGIONSUM")
          .option("regions", ",".join(regions))
          .option("start_date", start_date)
          .option("end_date", end_date)
          .option("use_sample", str(use_sample).lower())
          .load())

    initial_count = df.count()
    print(f"   Loaded {initial_count} rows")

    if initial_count == 0:
        print("   No data to process")
        return

    # Step 3: Validate and route bad records to DLQ
    print("\n3. Validating data...")
    good_df = process_with_dlq(df, dlq_table)

    # Step 4: Write to target
    good_count = good_df.count()
    if good_count > 0:
        print(f"\n4. Writing {good_count} rows to {target_table}...")
        (good_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table))

    # Step 5: Update checkpoints
    print("\n5. Updating checkpoints...")
    for region in regions:
        id_string = f"DISPATCHREGIONSUM:{region}:{start_date}:{end_date}"
        partition_id = hashlib.md5(id_string.encode()).hexdigest()[:12]

        if partition_id not in completed:
            mark_partition_complete(
                checkpoint_table, partition_id, region,
                start_date, end_date, good_count // len(regions)
            )

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"  - Processed: {initial_count} rows")
    print(f"  - Valid: {good_count} rows")
    print(f"  - To DLQ: {initial_count - good_count} rows")
    print("=" * 60)


# Demo run
# run_fault_tolerant_pipeline(
#     checkpoint_table="nemweb_checkpoints_solution",
#     dlq_table="nemweb_dlq_solution",
#     target_table="nemweb_bronze_solution",
#     regions=["NSW1", "VIC1"],
#     start_date="2024-01-01",
#     end_date="2024-01-07",
#     use_sample=True
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Retry with backoff**: Handles transient failures automatically
# MAGIC 2. **Checkpoint tables**: Enable batch job resume after failures
# MAGIC 3. **Streaming offsets**: Spark manages recovery automatically
# MAGIC 4. **Dead letter queues**: Preserve bad records for investigation
# MAGIC 5. **Idempotency**: Design operations that can safely re-run
