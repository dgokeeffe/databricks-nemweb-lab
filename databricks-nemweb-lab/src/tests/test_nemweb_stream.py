"""
Tests for NEMWEB Streaming Data Source

Tests the streaming data source components including:
- Offset management (initialOffset, latestOffset)
- File listing and filtering
- CSV parsing
- Partition planning
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from nemweb_datasource_stream import (
    NemwebStreamDataSource,
    NemwebStreamReader,
    NemwebStreamPartition,
    SCHEMAS,
)


class TestNemwebStreamDataSource:
    """Tests for the main DataSource class."""

    def test_name(self):
        """Test that the data source name is correct."""
        assert NemwebStreamDataSource.name() == "nemweb_stream"

    def test_schema_default(self):
        """Test default schema is DISPATCHREGIONSUM."""
        ds = NemwebStreamDataSource(options={})
        schema = ds.schema()

        field_names = [f.name for f in schema.fields]
        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names
        assert "TOTALDEMAND" in field_names
        assert len(field_names) == 12

    def test_schema_dispatchprice(self):
        """Test DISPATCHPRICE schema."""
        ds = NemwebStreamDataSource(options={"table": "DISPATCHPRICE"})
        schema = ds.schema()

        field_names = [f.name for f in schema.fields]
        assert "RRP" in field_names
        assert "EEP" in field_names
        assert len(field_names) == 10


class TestNemwebStreamReader:
    """Tests for the streaming reader."""

    @pytest.fixture
    def reader(self):
        """Create a test reader."""
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

        schema = StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])
        options = {
            "table": "DISPATCHREGIONSUM",
            "regions": "NSW1,VIC1",
            "poll_interval_seconds": "30",
        }
        return NemwebStreamReader(schema, options)

    def test_initial_offset(self, reader):
        """Test that initial offset is set correctly."""
        offset = reader.initialOffset()

        assert "last_file" in offset
        assert offset["last_file"].startswith("PUBLIC_DISPATCHIS_")

    def test_initial_offset_custom(self):
        """Test custom start_from offset."""
        from pyspark.sql.types import StructType

        schema = StructType([])
        options = {"start_from": "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"}
        reader = NemwebStreamReader(schema, options)

        offset = reader.initialOffset()
        assert offset["last_file"] == "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"

    @patch.object(NemwebStreamReader, '_list_current_files')
    def test_latest_offset(self, mock_list, reader):
        """Test that latest offset returns the newest file."""
        mock_list.return_value = [
            "PUBLIC_DISPATCHIS_202401011200_0000000001.zip",
            "PUBLIC_DISPATCHIS_202401011205_0000000002.zip",
            "PUBLIC_DISPATCHIS_202401011210_0000000003.zip",
        ]

        offset = reader.latestOffset()

        assert offset["last_file"] == "PUBLIC_DISPATCHIS_202401011210_0000000003.zip"

    @patch.object(NemwebStreamReader, '_list_current_files')
    def test_latest_offset_empty(self, mock_list, reader):
        """Test latest offset when no files are available."""
        mock_list.return_value = []

        offset = reader.latestOffset()

        assert offset["last_file"] == ""

    @patch.object(NemwebStreamReader, '_list_current_files')
    def test_partitions(self, mock_list, reader):
        """Test partition planning for a microbatch."""
        mock_list.return_value = [
            "PUBLIC_DISPATCHIS_202401011200_0000000001.zip",
            "PUBLIC_DISPATCHIS_202401011205_0000000002.zip",
            "PUBLIC_DISPATCHIS_202401011210_0000000003.zip",
            "PUBLIC_DISPATCHIS_202401011215_0000000004.zip",
        ]

        start = {"last_file": "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"}
        end = {"last_file": "PUBLIC_DISPATCHIS_202401011210_0000000003.zip"}

        partitions = reader.partitions(start, end)

        assert len(partitions) == 1
        partition = partitions[0]
        assert isinstance(partition, NemwebStreamPartition)
        # Should include files after start but up to and including end
        assert len(partition.files) == 2
        file_names = [f[0] for f in partition.files]
        assert "PUBLIC_DISPATCHIS_202401011205_0000000002.zip" in file_names
        assert "PUBLIC_DISPATCHIS_202401011210_0000000003.zip" in file_names

    @patch.object(NemwebStreamReader, '_list_current_files')
    def test_partitions_empty_range(self, mock_list, reader):
        """Test partition planning when no new files."""
        mock_list.return_value = [
            "PUBLIC_DISPATCHIS_202401011200_0000000001.zip",
        ]

        start = {"last_file": "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"}
        end = {"last_file": "PUBLIC_DISPATCHIS_202401011200_0000000001.zip"}

        partitions = reader.partitions(start, end)

        assert len(partitions) == 0

    def test_parse_timestamp(self, reader):
        """Test timestamp parsing."""
        # NEMWEB format
        ts = reader._parse_timestamp("2024/01/01 12:05:00")
        assert ts == datetime(2024, 1, 1, 12, 5, 0)

        # Alternative format
        ts = reader._parse_timestamp("2024-01-01 12:05:00")
        assert ts == datetime(2024, 1, 1, 12, 5, 0)

        # Invalid
        ts = reader._parse_timestamp("invalid")
        assert ts is None

        # Empty
        ts = reader._parse_timestamp("")
        assert ts is None

    def test_to_python_float(self, reader):
        """Test float conversion with pure Python type output."""
        result = reader._to_python_float("123.45")
        assert result == 123.45
        assert isinstance(result, float)

        assert reader._to_python_float("0") == 0.0
        assert reader._to_python_float("-100.5") == -100.5
        assert reader._to_python_float("") is None
        assert reader._to_python_float(None) is None
        assert reader._to_python_float("invalid") is None

    def test_to_python_datetime(self, reader):
        """Test datetime conversion returns pure Python datetime."""
        from datetime import datetime

        # Parse timestamp first
        ts = reader._parse_timestamp("2024/01/01 12:05:00")
        result = reader._to_python_datetime(ts)

        assert result == datetime(2024, 1, 1, 12, 5, 0)
        assert type(result).__module__ == "datetime"
        assert type(result).__name__ == "datetime"

        # None handling
        assert reader._to_python_datetime(None) is None


class TestNemwebStreamPartition:
    """Tests for the partition class."""

    def test_partition_creation(self):
        """Test partition can be created with required fields."""
        files = [
            ("file1.zip", "http://example.com/file1.zip"),
            ("file2.zip", "http://example.com/file2.zip"),
        ]
        partition = NemwebStreamPartition(
            table="DISPATCHREGIONSUM",
            files=files,
            regions=["NSW1", "VIC1"],
        )

        assert partition.table == "DISPATCHREGIONSUM"
        assert len(partition.files) == 2
        assert partition.regions == ["NSW1", "VIC1"]


class TestCSVParsing:
    """Tests for CSV parsing logic."""

    def test_parse_csv_nemweb_format(self):
        """Test parsing NEMWEB multi-record CSV format."""
        from io import BytesIO

        # Sample NEMWEB CSV format
        csv_content = b"""C,NEMP.WORLD,MMS Data Model,DISPATCH,REGIONSUM,PUBLIC
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,AVAILABLELOAD,DEMANDFORECAST,DISPATCHABLEGENERATION,DISPATCHABLELOAD,NETINTERCHANGE
D,DISPATCH,REGIONSUM,1,2024/01/01 12:05:00,1,NSW1,1,0,7500.5,8000.0,7800.0,7600.0,7900.0,7500.0,-200.5
D,DISPATCH,REGIONSUM,1,2024/01/01 12:05:00,1,VIC1,1,0,5200.3,5500.0,5300.0,5100.0,5400.0,5100.0,150.2
C,END OF REPORT
"""
        from pyspark.sql.types import StructType

        schema = StructType([])
        options = {"table": "DISPATCHREGIONSUM"}
        reader = NemwebStreamReader(schema, options)

        rows = reader._parse_csv(BytesIO(csv_content), "DISPATCH,REGIONSUM")

        assert len(rows) == 2
        assert rows[0]["REGIONID"] == "NSW1"
        assert rows[0]["TOTALDEMAND"] == "7500.5"
        assert rows[1]["REGIONID"] == "VIC1"


class TestSchemas:
    """Tests for schema definitions."""

    def test_all_tables_have_schemas(self):
        """Test that all expected tables have schema definitions."""
        expected_tables = ["DISPATCHREGIONSUM", "DISPATCHPRICE", "TRADINGPRICE"]

        for table in expected_tables:
            assert table in SCHEMAS
            assert "record_type" in SCHEMAS[table]
            assert "fields" in SCHEMAS[table]
            assert len(SCHEMAS[table]["fields"]) > 0

    def test_schema_field_types(self):
        """Test that schema fields have valid Spark types."""
        from pyspark.sql.types import TimestampType, StringType, DoubleType

        valid_types = (TimestampType, StringType, DoubleType)

        for table, config in SCHEMAS.items():
            for name, dtype in config["fields"]:
                assert isinstance(name, str)
                assert isinstance(dtype, valid_types), f"Invalid type for {table}.{name}"
