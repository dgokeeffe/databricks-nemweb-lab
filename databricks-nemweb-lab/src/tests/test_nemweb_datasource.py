"""
Unit tests for nemweb_datasource.py

Tests the custom PySpark data source implementation for NEMWEB data.
Run with: pytest test_nemweb_datasource.py -v

Note: These tests mock PySpark components to run without a Spark session.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock
import hashlib

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Import the module under test (path configured in conftest.py)
from nemweb_datasource import (
    NemwebPartition,
    NemwebDataSourceReader,
    NemwebStreamReader,
    NemwebDataSource,
)


class TestNemwebPartition:
    """Tests for NemwebPartition class."""

    def test_partition_creation(self):
        """Should create partition with all attributes."""
        partition = NemwebPartition(
            region="NSW1",
            start_date="2024-01-01",
            end_date="2024-01-07",
            table="DISPATCHREGIONSUM"
        )

        assert partition.region == "NSW1"
        assert partition.start_date == "2024-01-01"
        assert partition.end_date == "2024-01-07"
        assert partition.table == "DISPATCHREGIONSUM"
        assert partition.attempt == 0

    def test_partition_id_auto_generated(self):
        """Should auto-generate deterministic partition ID."""
        partition = NemwebPartition(
            region="NSW1",
            start_date="2024-01-01",
            end_date="2024-01-07",
            table="DISPATCHREGIONSUM"
        )

        assert partition.partition_id is not None
        assert len(partition.partition_id) == 12  # MD5 truncated to 12 chars

    def test_partition_id_is_deterministic(self):
        """Same inputs should produce same partition ID."""
        p1 = NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")
        p2 = NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")

        assert p1.partition_id == p2.partition_id

    def test_different_regions_different_ids(self):
        """Different regions should have different partition IDs."""
        p1 = NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")
        p2 = NemwebPartition("VIC1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")

        assert p1.partition_id != p2.partition_id

    def test_different_dates_different_ids(self):
        """Different date ranges should have different partition IDs."""
        p1 = NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")
        p2 = NemwebPartition("NSW1", "2024-01-08", "2024-01-14", "DISPATCHREGIONSUM")

        assert p1.partition_id != p2.partition_id

    def test_custom_partition_id(self):
        """Should accept custom partition ID."""
        partition = NemwebPartition(
            region="NSW1",
            start_date="2024-01-01",
            end_date="2024-01-07",
            table="DISPATCHREGIONSUM",
            partition_id="custom123"
        )

        assert partition.partition_id == "custom123"

    def test_partition_id_format(self):
        """Partition ID should be valid hex string."""
        partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")

        # Should be valid hex
        int(partition.partition_id, 16)  # Raises ValueError if not hex


class TestNemwebDataSourceReader:
    """Tests for NemwebDataSourceReader class."""

    def get_test_schema(self):
        """Return a test schema."""
        return StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

    def test_reader_initialization(self):
        """Should initialize with schema and options."""
        schema = self.get_test_schema()
        options = {
            "table": "DISPATCHREGIONSUM",
            "regions": "NSW1,VIC1",
            "start_date": "2024-01-01",
            "end_date": "2024-01-07"
        }

        reader = NemwebDataSourceReader(schema, options)

        assert reader.table == "DISPATCHREGIONSUM"
        assert reader.regions == ["NSW1", "VIC1"]
        assert reader.start_date == "2024-01-01"
        assert reader.end_date == "2024-01-07"

    def test_reader_default_options(self):
        """Should use defaults for missing options."""
        schema = self.get_test_schema()
        reader = NemwebDataSourceReader(schema, {})

        assert reader.table == "DISPATCHREGIONSUM"
        assert len(reader.regions) == 5  # All 5 NEM regions

    def test_partitions_creates_one_per_region(self):
        """Should create one partition per region."""
        schema = self.get_test_schema()
        options = {"regions": "NSW1,VIC1,QLD1"}

        reader = NemwebDataSourceReader(schema, options)
        partitions = reader.partitions()

        assert len(partitions) == 3
        regions = {p.region for p in partitions}
        assert regions == {"NSW1", "VIC1", "QLD1"}

    def test_partitions_strips_whitespace(self):
        """Should strip whitespace from region names."""
        schema = self.get_test_schema()
        options = {"regions": "NSW1 , VIC1 , QLD1"}

        reader = NemwebDataSourceReader(schema, options)
        partitions = reader.partitions()

        regions = {p.region for p in partitions}
        assert regions == {"NSW1", "VIC1", "QLD1"}

    def test_skip_completed_partitions(self):
        """Should skip completed partitions when checkpointing enabled."""
        schema = self.get_test_schema()
        options = {
            "regions": "NSW1,VIC1,QLD1",
            "checkpoint_table": "test_checkpoints",
            "skip_completed": "true"
        }

        reader = NemwebDataSourceReader(schema, options)

        # Mock the checkpoint table read
        with patch.object(reader, '_get_completed_partitions') as mock_get:
            # Simulate NSW1 already completed
            mock_get.return_value = {
                NemwebPartition("NSW1", "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM").partition_id
            }

            partitions = reader.partitions()

        # Should only have 2 partitions (VIC1, QLD1)
        assert len(partitions) == 2
        regions = {p.region for p in partitions}
        assert "NSW1" not in regions

    def test_read_partition_with_mock(self):
        """Should read data from partition."""
        schema = self.get_test_schema()
        reader = NemwebDataSourceReader(schema, {})

        partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-01", "DISPATCHREGIONSUM")

        mock_data = [
            {"SETTLEMENTDATE": "2024-01-01 00:05:00", "REGIONID": "NSW1", "TOTALDEMAND": "7500.5"}
        ]

        with patch("nemweb_datasource.fetch_nemweb_data", return_value=mock_data):
            rows = list(reader.read(partition))

        assert len(rows) == 1
        # Should be a tuple matching schema order
        assert rows[0][1] == "NSW1"  # REGIONID

    def test_read_handles_errors(self):
        """Should raise on read errors."""
        schema = self.get_test_schema()
        reader = NemwebDataSourceReader(schema, {})
        partition = NemwebPartition("NSW1", "2024-01-01", "2024-01-01", "DISPATCHREGIONSUM")

        with patch("nemweb_datasource.fetch_nemweb_data", side_effect=Exception("HTTP Error")):
            with pytest.raises(Exception):
                list(reader.read(partition))


class TestNemwebStreamReader:
    """Tests for NemwebStreamReader class."""

    def test_stream_reader_initialization(self):
        """Should initialize streaming reader."""
        options = {
            "table": "DISPATCHREGIONSUM",
            "regions": "NSW1,VIC1",
            "lookback_minutes": "60"
        }

        reader = NemwebStreamReader(options)

        assert reader.table == "DISPATCHREGIONSUM"
        assert reader.regions == ["NSW1", "VIC1"]
        assert reader.lookback_minutes == 60

    def test_initial_offset_default(self):
        """Should return default initial offset (1 hour ago)."""
        reader = NemwebStreamReader({})
        offset = reader.initialOffset()

        assert "timestamp" in offset
        # Should be approximately 1 hour ago
        offset_time = datetime.strptime(offset["timestamp"], "%Y-%m-%d %H:%M:%S")
        expected = datetime.now() - timedelta(hours=1)

        # Allow 5 second tolerance
        diff = abs((offset_time - expected).total_seconds())
        assert diff < 5

    def test_initial_offset_custom(self):
        """Should use custom start_timestamp if provided."""
        options = {"start_timestamp": "2024-01-01 00:00:00"}
        reader = NemwebStreamReader(options)
        offset = reader.initialOffset()

        assert offset["timestamp"] == "2024-01-01 00:00:00"

    def test_latest_offset(self):
        """Should return current time as latest offset."""
        reader = NemwebStreamReader({})
        offset = reader.latestOffset()

        assert "timestamp" in offset
        offset_time = datetime.strptime(offset["timestamp"], "%Y-%m-%d %H:%M:%S")

        # Should be within 5 seconds of now
        diff = abs((offset_time - datetime.now()).total_seconds())
        assert diff < 5

    def test_commit_logs_offset(self):
        """Commit should log the offset."""
        reader = NemwebStreamReader({})

        with patch("nemweb_datasource.logger") as mock_logger:
            reader.commit({"timestamp": "2024-01-01 12:00:00"})
            mock_logger.info.assert_called_once()


class TestNemwebDataSource:
    """Tests for NemwebDataSource class."""

    def test_data_source_name(self):
        """Should return 'nemweb' as format name."""
        assert NemwebDataSource.name() == "nemweb"

    def test_schema_returns_struct_type(self):
        """Should return a StructType schema."""
        # Mock the options attribute
        ds = NemwebDataSource.__new__(NemwebDataSource)
        ds.options = {"table": "DISPATCHREGIONSUM"}

        schema = ds.schema()

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names

    def test_schema_for_different_tables(self):
        """Should return appropriate schema for each table."""
        ds = NemwebDataSource.__new__(NemwebDataSource)

        # Test DISPATCHPRICE
        ds.options = {"table": "DISPATCHPRICE"}
        schema = ds.schema()
        field_names = [f.name for f in schema.fields]
        assert "RRP" in field_names  # Regional Reference Price

    def test_reader_returns_reader_instance(self):
        """Should return a NemwebDataSourceReader."""
        ds = NemwebDataSource.__new__(NemwebDataSource)
        ds.options = {"table": "DISPATCHREGIONSUM"}

        schema = StructType([
            StructField("REGIONID", StringType(), True)
        ])

        reader = ds.reader(schema)

        assert isinstance(reader, NemwebDataSourceReader)

    def test_stream_reader_returns_stream_reader_instance(self):
        """Should return a NemwebStreamReader."""
        ds = NemwebDataSource.__new__(NemwebDataSource)
        ds.options = {"table": "DISPATCHREGIONSUM"}

        schema = StructType([
            StructField("REGIONID", StringType(), True)
        ])

        reader = ds.simpleStreamReader(schema)

        assert isinstance(reader, NemwebStreamReader)


class TestPartitionIdGeneration:
    """Tests for partition ID generation logic."""

    def test_partition_id_matches_expected_format(self):
        """Partition ID should match MD5 hash format."""
        partition = NemwebPartition(
            region="NSW1",
            start_date="2024-01-01",
            end_date="2024-01-07",
            table="DISPATCHREGIONSUM"
        )

        # Manually calculate expected ID
        id_string = "DISPATCHREGIONSUM:NSW1:2024-01-01:2024-01-07"
        expected_id = hashlib.md5(id_string.encode()).hexdigest()[:12]

        assert partition.partition_id == expected_id

    def test_all_regions_have_unique_ids(self):
        """All 5 NEM regions should have unique partition IDs."""
        regions = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
        partition_ids = set()

        for region in regions:
            p = NemwebPartition(region, "2024-01-01", "2024-01-07", "DISPATCHREGIONSUM")
            partition_ids.add(p.partition_id)

        assert len(partition_ids) == 5


class TestCheckpointIntegration:
    """Tests for checkpoint-based recovery."""

    def get_test_schema(self):
        """Return a test schema."""
        return StructType([
            StructField("REGIONID", StringType(), True),
        ])

    def test_get_completed_partitions_returns_empty_without_spark(self):
        """Should return empty set when Spark session unavailable."""
        from pyspark.sql import SparkSession

        schema = self.get_test_schema()
        options = {"checkpoint_table": "test_checkpoints"}

        reader = NemwebDataSourceReader(schema, options)

        with patch.object(SparkSession, "getActiveSession", return_value=None):
            completed = reader._get_completed_partitions()

        assert completed == set()

    def test_get_completed_partitions_handles_errors(self):
        """Should return empty set on errors."""
        from pyspark.sql import SparkSession

        schema = self.get_test_schema()
        options = {"checkpoint_table": "nonexistent_table"}

        reader = NemwebDataSourceReader(schema, options)

        # Mock SparkSession to raise an error
        mock_session = MagicMock()
        mock_session.read.table.side_effect = Exception("Table not found")

        with patch.object(SparkSession, "getActiveSession", return_value=mock_session):
            completed = reader._get_completed_partitions()

        assert completed == set()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
