"""
Unit tests for pipeline transformation logic.

Since pyspark.pipelines only works within the Databricks runtime,
we test the transformation logic separately using regular PySpark.

Run with: pytest test_pipeline_logic.py -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("PipelineLogicTests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())


@pytest.fixture
def sample_bronze_data(spark):
    """Create sample bronze layer data."""
    schema = StructType([
        StructField("SETTLEMENTDATE", StringType(), True),
        StructField("REGIONID", StringType(), True),
        StructField("TOTALDEMAND", StringType(), True),
        StructField("AVAILABLEGENERATION", StringType(), True),
        StructField("NETINTERCHANGE", StringType(), True),
        StructField("DISPATCHINTERVAL", StringType(), True),
    ])

    data = [
        ("2024-01-01 00:05:00", "NSW1", "7500.5", "8000.0", "-200.5", "1"),
        ("2024-01-01 00:10:00", "NSW1", "7600.3", "8100.0", "-180.2", "2"),
        ("2024-01-01 00:05:00", "VIC1", "5200.3", "5500.0", "150.2", "1"),
        ("2024-01-01 00:10:00", "VIC1", "5300.1", "5600.0", "140.5", "2"),
        ("2024-01-01 00:05:00", "QLD1", "6100.8", "6800.0", "-50.5", "1"),
        ("2024-01-01 00:05:00", "INVALID", "-100", "0", "0", "1"),  # Invalid
        (None, "SA1", "1800.0", "2000.0", "100.0", "1"),  # Null timestamp
    ]

    return spark.createDataFrame(data, schema)


# =============================================================================
# Extracted transformation functions (same logic as pipeline, but testable)
# =============================================================================

def apply_silver_transformations(df):
    """
    Apply silver layer transformations.

    This is the same logic as nemweb_silver in the pipeline,
    but extracted for testing.
    """
    return (df
        .select(
            col("SETTLEMENTDATE").cast("timestamp").alias("settlement_date"),
            col("REGIONID").alias("region_id"),
            col("TOTALDEMAND").cast("double").alias("total_demand_mw"),
            col("AVAILABLEGENERATION").cast("double").alias("available_generation_mw"),
            col("NETINTERCHANGE").cast("double").alias("net_interchange_mw"),
            col("DISPATCHINTERVAL").alias("dispatch_interval"),
        )
        .withColumn("demand_generation_ratio",
                    col("total_demand_mw") / col("available_generation_mw"))
    )


def apply_quality_filters(df):
    """
    Apply data quality filters (equivalent to @dp.expect_or_drop).

    Filters:
    - valid_region: REGIONID IN ('NSW1', 'QLD1', 'SA1', 'VIC1', 'TAS1')
    - valid_demand: TOTALDEMAND > 0
    - valid_timestamp: SETTLEMENTDATE IS NOT NULL
    - reasonable_demand: TOTALDEMAND < 20000
    """
    valid_regions = ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]

    return (df
        .filter(col("region_id").isin(valid_regions))
        .filter(col("total_demand_mw") > 0)
        .filter(col("settlement_date").isNotNull())
        .filter(col("total_demand_mw") < 20000)
    )


def compute_hourly_aggregates(df):
    """
    Compute hourly aggregations (equivalent to nemweb_gold_hourly).
    """
    return (df
        .withColumn("hour", expr("date_trunc('hour', settlement_date)"))
        .groupBy("region_id", "hour")
        .agg(
            avg("total_demand_mw").alias("avg_demand_mw"),
            max("total_demand_mw").alias("max_demand_mw"),
            min("total_demand_mw").alias("min_demand_mw"),
            count("*").alias("interval_count"),
        )
    )


# =============================================================================
# Tests
# =============================================================================

class TestSilverTransformations:
    """Tests for silver layer transformation logic."""

    def test_column_renaming(self, sample_bronze_data):
        """Should rename columns to snake_case."""
        result = apply_silver_transformations(sample_bronze_data)

        expected_columns = {
            "settlement_date", "region_id", "total_demand_mw",
            "available_generation_mw", "net_interchange_mw",
            "dispatch_interval", "demand_generation_ratio"
        }
        assert set(result.columns) == expected_columns

    def test_type_casting(self, sample_bronze_data):
        """Should cast string columns to appropriate types."""
        result = apply_silver_transformations(sample_bronze_data)

        # Check schema types
        schema = {f.name: type(f.dataType).__name__ for f in result.schema.fields}

        assert schema["settlement_date"] == "TimestampType"
        assert schema["total_demand_mw"] == "DoubleType"
        assert schema["available_generation_mw"] == "DoubleType"

    def test_demand_generation_ratio(self, sample_bronze_data):
        """Should calculate demand/generation ratio correctly."""
        result = apply_silver_transformations(sample_bronze_data)

        # Get NSW1 first row
        nsw_row = result.filter(col("region_id") == "NSW1").first()

        expected_ratio = 7500.5 / 8000.0
        assert abs(nsw_row["demand_generation_ratio"] - expected_ratio) < 0.001


class TestQualityFilters:
    """Tests for data quality filter logic."""

    def test_filters_invalid_region(self, sample_bronze_data):
        """Should filter out invalid regions."""
        silver = apply_silver_transformations(sample_bronze_data)
        result = apply_quality_filters(silver)

        regions = [row["region_id"] for row in result.collect()]
        assert "INVALID" not in regions

    def test_filters_null_timestamp(self, sample_bronze_data):
        """Should filter out null timestamps."""
        silver = apply_silver_transformations(sample_bronze_data)
        result = apply_quality_filters(silver)

        # Original had 7 rows, should have fewer after filtering
        assert result.count() < sample_bronze_data.count()

        # No nulls in settlement_date
        null_count = result.filter(col("settlement_date").isNull()).count()
        assert null_count == 0

    def test_filters_negative_demand(self, sample_bronze_data):
        """Should filter out negative demand values."""
        silver = apply_silver_transformations(sample_bronze_data)
        result = apply_quality_filters(silver)

        # All remaining demand values should be positive
        min_demand = result.agg(min("total_demand_mw")).first()[0]
        assert min_demand > 0

    def test_expected_valid_rows(self, sample_bronze_data):
        """Should keep only valid rows."""
        silver = apply_silver_transformations(sample_bronze_data)
        result = apply_quality_filters(silver)

        # Original: 7 rows
        # Invalid: INVALID region (1), null timestamp (1), negative demand (in INVALID)
        # Expected valid: 5 rows (NSW1 x2, VIC1 x2, QLD1 x1)
        assert result.count() == 5


class TestHourlyAggregates:
    """Tests for gold layer aggregation logic."""

    def test_aggregation_columns(self, sample_bronze_data):
        """Should produce expected aggregation columns."""
        silver = apply_silver_transformations(sample_bronze_data)
        filtered = apply_quality_filters(silver)
        result = compute_hourly_aggregates(filtered)

        expected_columns = {
            "region_id", "hour", "avg_demand_mw",
            "max_demand_mw", "min_demand_mw", "interval_count"
        }
        assert set(result.columns) == expected_columns

    def test_interval_count(self, sample_bronze_data):
        """Should count intervals correctly per region/hour."""
        silver = apply_silver_transformations(sample_bronze_data)
        filtered = apply_quality_filters(silver)
        result = compute_hourly_aggregates(filtered)

        # NSW1 has 2 intervals, VIC1 has 2, QLD1 has 1
        counts = {row["region_id"]: row["interval_count"]
                  for row in result.collect()}

        assert counts["NSW1"] == 2
        assert counts["VIC1"] == 2
        assert counts["QLD1"] == 1

    def test_avg_demand_calculation(self, sample_bronze_data):
        """Should calculate average demand correctly."""
        silver = apply_silver_transformations(sample_bronze_data)
        filtered = apply_quality_filters(silver)
        result = compute_hourly_aggregates(filtered)

        # NSW1 avg: (7500.5 + 7600.3) / 2 = 7550.4
        nsw_row = result.filter(col("region_id") == "NSW1").first()
        expected_avg = (7500.5 + 7600.3) / 2

        assert abs(nsw_row["avg_demand_mw"] - expected_avg) < 0.01

    def test_max_min_demand(self, sample_bronze_data):
        """Should find max and min demand correctly."""
        silver = apply_silver_transformations(sample_bronze_data)
        filtered = apply_quality_filters(silver)
        result = compute_hourly_aggregates(filtered)

        nsw_row = result.filter(col("region_id") == "NSW1").first()

        assert nsw_row["max_demand_mw"] == 7600.3
        assert nsw_row["min_demand_mw"] == 7500.5


class TestEndToEndPipeline:
    """End-to-end tests for the full pipeline logic."""

    def test_full_transformation_chain(self, sample_bronze_data):
        """Should process data through all layers correctly."""
        # Bronze -> Silver
        silver = apply_silver_transformations(sample_bronze_data)
        assert silver.count() == 7  # All rows still present

        # Silver -> Filtered
        filtered = apply_quality_filters(silver)
        assert filtered.count() == 5  # Invalid rows removed

        # Filtered -> Gold
        gold = compute_hourly_aggregates(filtered)
        assert gold.count() == 3  # 3 regions


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataframe(self, spark):
        """Should handle empty DataFrame gracefully."""
        schema = StructType([
            StructField("SETTLEMENTDATE", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", StringType(), True),
            StructField("AVAILABLEGENERATION", StringType(), True),
            StructField("NETINTERCHANGE", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
        ])

        empty_df = spark.createDataFrame([], schema)
        silver = apply_silver_transformations(empty_df)
        filtered = apply_quality_filters(silver)

        assert filtered.count() == 0

    def test_division_by_zero_raises_error(self, spark):
        """Division by zero raises ArithmeticException in Spark 4.x (ANSI mode)."""
        from pyspark.errors.exceptions.captured import ArithmeticException

        schema = StructType([
            StructField("SETTLEMENTDATE", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", StringType(), True),
            StructField("AVAILABLEGENERATION", StringType(), True),
            StructField("NETINTERCHANGE", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
        ])

        data = [("2024-01-01 00:05:00", "NSW1", "7500.5", "0", "-200.5", "1")]
        df = spark.createDataFrame(data, schema)

        silver = apply_silver_transformations(df)

        # Spark 4.x has ANSI mode enabled by default - division by zero raises error
        # In production, use try_divide() or coalesce() to handle this
        with pytest.raises(ArithmeticException):
            silver.collect()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
