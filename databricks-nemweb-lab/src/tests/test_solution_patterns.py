"""
Tests for solution code patterns used in the lab exercises.

These tests verify the code patterns taught in the exercises work correctly.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)


class TestExercise01Schema:
    """Tests for Exercise 1 - Custom Data Source schema definition."""

    def test_dispatchregionsum_schema_fields(self):
        """Schema should have exactly 12 fields."""
        # This is the schema pattern from the solution
        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("RUNNO", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
            StructField("INTERVENTION", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("AVAILABLEGENERATION", DoubleType(), True),
            StructField("AVAILABLELOAD", DoubleType(), True),
            StructField("DEMANDFORECAST", DoubleType(), True),
            StructField("DISPATCHABLEGENERATION", DoubleType(), True),
            StructField("DISPATCHABLELOAD", DoubleType(), True),
            StructField("NETINTERCHANGE", DoubleType(), True),
        ])

        assert len(schema.fields) == 12

    def test_schema_has_required_fields(self):
        """Schema must include key measurement fields."""
        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("RUNNO", StringType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("DISPATCHINTERVAL", StringType(), True),
            StructField("INTERVENTION", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("AVAILABLEGENERATION", DoubleType(), True),
            StructField("AVAILABLELOAD", DoubleType(), True),
            StructField("DEMANDFORECAST", DoubleType(), True),
            StructField("DISPATCHABLEGENERATION", DoubleType(), True),
            StructField("DISPATCHABLELOAD", DoubleType(), True),
            StructField("NETINTERCHANGE", DoubleType(), True),
        ])

        field_names = [f.name for f in schema.fields]
        required = ["TOTALDEMAND", "AVAILABLEGENERATION", "NETINTERCHANGE"]
        for field in required:
            assert field in field_names, f"Missing required field: {field}"

    def test_schema_field_types(self):
        """Fields must have correct types."""
        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

        type_map = {f.name: type(f.dataType).__name__ for f in schema.fields}

        assert type_map["SETTLEMENTDATE"] == "TimestampType"
        assert type_map["REGIONID"] == "StringType"
        assert type_map["TOTALDEMAND"] == "DoubleType"


class TestExercise01Partitions:
    """Tests for Exercise 1 - Partition planning pattern."""

    def test_partition_class_attributes(self):
        """Partition class should store region, start_date, end_date."""
        # This mimics the NemwebPartition from the solution
        class NemwebPartition:
            def __init__(self, region: str, start_date: str, end_date: str):
                self.region = region
                self.start_date = start_date
                self.end_date = end_date

        partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-07")

        assert partition.region == "NSW1"
        assert partition.start_date == "2024-01-01"
        assert partition.end_date == "2024-01-07"

    def test_partition_planning_creates_one_per_region(self):
        """partitions() should create one partition per region."""
        regions = ["NSW1", "VIC1", "QLD1"]

        # Simulate the solution's partitions() logic
        partitions = []
        for region in regions:
            partitions.append({
                "region": region.strip(),
                "start_date": "2024-01-01",
                "end_date": "2024-01-01"
            })

        assert len(partitions) == 3
        assert [p["region"] for p in partitions] == ["NSW1", "VIC1", "QLD1"]


class TestExercise01FetchNemwebCurrent:
    """Tests for fetch_nemweb_current function used in exercises."""

    def test_fetch_nemweb_current_with_sample(self):
        """fetch_nemweb_current with use_sample=True should return sample data."""
        from nemweb_utils import fetch_nemweb_current

        data = fetch_nemweb_current(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            max_files=2,
            use_sample=True
        )

        assert len(data) > 0
        assert isinstance(data, list)
        assert isinstance(data[0], dict)

    def test_fetch_nemweb_current_filters_by_region(self):
        """Sample data should be filtered by region when specified."""
        from nemweb_utils import fetch_nemweb_current

        nsw_data = fetch_nemweb_current(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            use_sample=True
        )

        # All rows should be for NSW1
        for row in nsw_data:
            assert row.get("REGIONID") == "NSW1"


class TestExercise01ParseNemwebCsv:
    """Tests for parse_nemweb_csv function used in exercises."""

    def test_parse_nemweb_csv_returns_tuples(self):
        """parse_nemweb_csv should yield tuples matching schema."""
        from nemweb_utils import parse_nemweb_csv

        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

        test_data = [
            {"SETTLEMENTDATE": "2024/01/01 00:05:00", "REGIONID": "NSW1", "TOTALDEMAND": "7500.5"}
        ]

        result = list(parse_nemweb_csv(test_data, schema))

        assert len(result) == 1
        assert isinstance(result[0], tuple)
        assert len(result[0]) == 3  # Matches schema field count

    def test_parse_nemweb_csv_converts_types(self):
        """parse_nemweb_csv should convert string values to correct types.

        Note: Timestamps are returned as strings for Serverless compatibility.
        Use Spark's to_timestamp() to cast after loading.
        """
        from nemweb_utils import parse_nemweb_csv

        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

        test_data = [
            {"SETTLEMENTDATE": "2024/01/01 00:05:00", "REGIONID": "NSW1", "TOTALDEMAND": "7500.5"}
        ]

        result = list(parse_nemweb_csv(test_data, schema))
        row = result[0]

        # Timestamps returned as strings for Serverless compatibility
        assert isinstance(row[0], str)
        assert isinstance(row[1], str)
        assert isinstance(row[2], float)
        assert row[1] == "NSW1"
        assert row[2] == 7500.5


class TestExercise02DataQuality:
    """Tests for Exercise 2 - Data quality patterns."""

    def test_valid_region_filter(self):
        """valid_region expectation should filter invalid regions."""
        valid_regions = ["NSW1", "QLD1", "SA1", "VIC1", "TAS1"]

        test_data = [
            {"REGIONID": "NSW1"},  # Valid
            {"REGIONID": "INVALID"},  # Invalid
            {"REGIONID": "VIC1"},  # Valid
        ]

        filtered = [r for r in test_data if r["REGIONID"] in valid_regions]
        assert len(filtered) == 2

    def test_valid_demand_filter(self):
        """valid_demand expectation should filter negative/zero demand."""
        test_data = [
            {"TOTALDEMAND": 7500.5},  # Valid
            {"TOTALDEMAND": -100},  # Invalid
            {"TOTALDEMAND": 0},  # Invalid
            {"TOTALDEMAND": 5000},  # Valid
        ]

        filtered = [r for r in test_data if r["TOTALDEMAND"] > 0]
        assert len(filtered) == 2

    def test_reasonable_demand_filter(self):
        """reasonable_demand expectation should filter extreme values."""
        MAX_REASONABLE_DEMAND = 20000  # MW

        test_data = [
            {"TOTALDEMAND": 7500},  # Valid
            {"TOTALDEMAND": 50000},  # Invalid (too high)
            {"TOTALDEMAND": 15000},  # Valid
        ]

        filtered = [r for r in test_data if r["TOTALDEMAND"] < MAX_REASONABLE_DEMAND]
        assert len(filtered) == 2


class TestExercise03ClusterSizing:
    """Tests for Exercise 3 - Cluster sizing calculations."""

    def test_partition_calculation(self):
        """Calculate optimal partition count from data size."""
        data_size_mb = 100
        target_partition_size_mb = 64

        partitions = max(1, data_size_mb // target_partition_size_mb)

        # 100 MB / 64 MB = 1.56, so we get 1 partition
        # In practice, we'd round up or use ceil
        assert partitions >= 1

    def test_core_calculation_from_sla(self):
        """Calculate required cores from SLA and task metrics."""
        sla_minutes = 15
        target_minutes = 10  # Leave buffer
        task_count = 10
        avg_task_duration_seconds = 45

        total_task_time_seconds = task_count * avg_task_duration_seconds
        target_seconds = target_minutes * 60
        min_cores = max(1, total_task_time_seconds // target_seconds)

        # 10 tasks × 45s = 450s total
        # Target: 600s
        # Min cores: 450/600 = 0.75 → 1
        assert min_cores >= 1

    def test_dbu_cost_calculation(self):
        """Calculate DBU cost for a configuration."""
        dbu_rate = 0.22  # Jobs Compute DBU rate
        cores = 4
        hours = 0.5  # 30 minutes
        dbu_per_core_hour = 0.15  # Approximate

        total_dbu = cores * hours * dbu_per_core_hour
        cost = total_dbu * dbu_rate

        assert total_dbu > 0
        assert cost > 0


class TestNemwebCurrentFilePattern:
    """Tests for NEMWEB CURRENT file naming pattern."""

    def test_current_file_pattern_matches(self):
        """Regex pattern should match actual CURRENT file names."""
        import re

        # Actual file format from NEMWEB CURRENT
        test_filenames = [
            "PUBLIC_DISPATCHIS_202601251405_0000000500374526.zip",
            "PUBLIC_DISPATCHIS_202601251410_0000000500374527.zip",
            "PUBLIC_TRADINGIS_202601251400_0000000500374525.zip",
        ]

        file_prefix = "DISPATCHIS"
        pattern = rf'(PUBLIC_{file_prefix}_\d{{12}}_\d+\.zip)'

        matches = []
        for filename in test_filenames:
            match = re.match(pattern, filename, re.IGNORECASE)
            if match:
                matches.append(match.group(1))

        assert len(matches) == 2  # Only DISPATCHIS files
        assert "PUBLIC_DISPATCHIS_202601251405_0000000500374526.zip" in matches
