"""
Tests for NEMWEB Unified Data Source (Batch + Streaming)

Tests the unified data source components including:
- Schema generation for all 8 tables
- Batch reader (NemwebBatchReader) partitioning and reading
- Stream reader offset management, file listing, and CSV parsing
- Region filtering logic (only for tables with REGIONID)
- Partition planning
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import io
import zipfile

from nemweb_datasource_stream import (
    NemwebStreamDataSource,
    NemwebStreamReader,
    NemwebBatchReader,
    NemwebBatchPartition,
    NemwebStreamPartition,
    _has_region_field,
    _row_to_tuple,
)
from nemweb_utils import SCHEMAS, TABLE_CONFIG


# All 8 tables that should be supported
ALL_TABLES = [
    "DISPATCHREGIONSUM",
    "DISPATCHPRICE",
    "TRADINGPRICE",
    "DISPATCH_UNIT_SCADA",
    "ROOFTOP_PV_ACTUAL",
    "DISPATCH_REGION",
    "DISPATCH_INTERCONNECTOR",
    "DISPATCH_INTERCONNECTOR_TRADING",
]


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

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_schema_all_tables(self, table):
        """Test that all 8 tables produce valid schemas."""
        ds = NemwebStreamDataSource(options={"table": table})
        schema = ds.schema()

        assert len(schema.fields) > 0
        field_names = [f.name for f in schema.fields]
        # All tables should have a timestamp field
        assert any(
            name in field_names
            for name in ["SETTLEMENTDATE", "INTERVAL_DATETIME"]
        )

    def test_reader_returns_batch_reader(self):
        """Test that reader() returns a NemwebBatchReader."""
        from pyspark.sql.types import StructType
        ds = NemwebStreamDataSource(options={"table": "DISPATCHREGIONSUM"})
        schema = StructType([])
        reader = ds.reader(schema)
        assert isinstance(reader, NemwebBatchReader)

    def test_stream_reader_returns_stream_reader(self):
        """Test that streamReader() returns a NemwebStreamReader."""
        from pyspark.sql.types import StructType
        ds = NemwebStreamDataSource(options={"table": "DISPATCHREGIONSUM"})
        schema = StructType([])
        reader = ds.streamReader(schema)
        assert isinstance(reader, NemwebStreamReader)


class TestNemwebBatchReader:
    """Tests for the batch reader."""

    def test_partitions_date_range(self):
        """Test that batch reader creates one partition per date."""
        from pyspark.sql.types import StructType

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCHREGIONSUM",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        })

        partitions = reader.partitions()
        assert len(partitions) == 3
        assert all(isinstance(p, NemwebBatchPartition) for p in partitions)
        assert partitions[0].date == "2024-01-01"
        assert partitions[1].date == "2024-01-02"
        assert partitions[2].date == "2024-01-03"

    def test_partitions_single_day(self):
        """Test batch reader with a single day range."""
        from pyspark.sql.types import StructType

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCH_UNIT_SCADA",
            "start_date": "2024-06-15",
            "end_date": "2024-06-15",
        })

        partitions = reader.partitions()
        assert len(partitions) == 1
        assert partitions[0].table == "DISPATCH_UNIT_SCADA"

    def test_partitions_propagate_regions(self):
        """Test that regions are passed through to partitions."""
        from pyspark.sql.types import StructType

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCHREGIONSUM",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "regions": "NSW1,VIC1",
        })

        partitions = reader.partitions()
        assert partitions[0].regions == ["NSW1", "VIC1"]

    def test_read_mocked_zip(self):
        """Test batch reader reads and parses mocked ZIP data."""
        from pyspark.sql.types import StructType

        # Create a mock ZIP with NEMWEB CSV
        csv_content = b"""C,NEMP.WORLD,MMS Data Model
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,AVAILABLELOAD,DEMANDFORECAST,DISPATCHABLEGENERATION,DISPATCHABLELOAD,NETINTERCHANGE
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,NSW1,1,0,7500.5,8000.0,100.0,7600.0,7900.0,7500.0,-200.5
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,VIC1,1,0,5200.3,5500.0,200.0,5100.0,5400.0,5100.0,150.2
C,END OF REPORT
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("PUBLIC_DISPATCHIS.CSV", csv_content)
        zip_data = zip_buffer.getvalue()

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCHREGIONSUM",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
        })

        partition = NemwebBatchPartition(
            table="DISPATCHREGIONSUM",
            date="2024-01-01",
            regions=[],
        )

        with patch("nemweb_datasource_stream.fetch_with_retry", return_value=zip_data):
            rows = list(reader.read(partition))

        assert len(rows) == 2
        # First field is SETTLEMENTDATE (datetime)
        assert isinstance(rows[0][0], datetime)
        # REGIONID is third field
        assert rows[0][2] == "NSW1"
        assert rows[1][2] == "VIC1"

    def test_read_with_region_filter(self):
        """Test batch reader applies region filter."""
        from pyspark.sql.types import StructType

        csv_content = b"""C,test
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,AVAILABLELOAD,DEMANDFORECAST,DISPATCHABLEGENERATION,DISPATCHABLELOAD,NETINTERCHANGE
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,NSW1,1,0,7500.5,8000.0,100.0,7600.0,7900.0,7500.0,-200.5
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,VIC1,1,0,5200.3,5500.0,200.0,5100.0,5400.0,5100.0,150.2
C,END
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("test.CSV", csv_content)
        zip_data = zip_buffer.getvalue()

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCHREGIONSUM",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "regions": "NSW1",
        })

        partition = NemwebBatchPartition(
            table="DISPATCHREGIONSUM",
            date="2024-01-01",
            regions=["NSW1"],
        )

        with patch("nemweb_datasource_stream.fetch_with_retry", return_value=zip_data):
            rows = list(reader.read(partition))

        assert len(rows) == 1
        assert rows[0][2] == "NSW1"

    def test_read_no_region_filter_for_scada(self):
        """Test that region filter is NOT applied for DISPATCH_UNIT_SCADA."""
        from pyspark.sql.types import StructType

        csv_content = b"""C,test
I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE,LASTCHANGED
D,DISPATCH,UNIT_SCADA,1,"2024/01/01 12:05:00",UNIT1,100.5,"2024/01/01 12:05:00"
D,DISPATCH,UNIT_SCADA,1,"2024/01/01 12:05:00",UNIT2,200.3,"2024/01/01 12:05:00"
C,END
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("test.CSV", csv_content)
        zip_data = zip_buffer.getvalue()

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCH_UNIT_SCADA",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "regions": "NSW1",
        })

        partition = NemwebBatchPartition(
            table="DISPATCH_UNIT_SCADA",
            date="2024-01-01",
            regions=["NSW1"],
        )

        with patch("nemweb_datasource_stream.fetch_with_retry", return_value=zip_data):
            rows = list(reader.read(partition))

        # Both units should be returned (no region filter)
        assert len(rows) == 2

    def test_read_handles_404(self):
        """Test batch reader handles 404 gracefully."""
        from pyspark.sql.types import StructType
        from urllib.error import HTTPError

        reader = NemwebBatchReader(StructType([]), {
            "table": "DISPATCHREGIONSUM",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
        })

        partition = NemwebBatchPartition(
            table="DISPATCHREGIONSUM",
            date="2024-01-01",
            regions=[],
        )

        error = HTTPError("http://example.com", 404, "Not Found", {}, None)
        with patch("nemweb_datasource_stream.fetch_with_retry", side_effect=error):
            rows = list(reader.read(partition))

        assert rows == []


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

    def test_uses_table_config(self):
        """Test that stream reader gets folder/prefix from TABLE_CONFIG."""
        from pyspark.sql.types import StructType

        reader = NemwebStreamReader(StructType([]), {"table": "DISPATCH_UNIT_SCADA"})
        assert reader.folder == "Dispatch_SCADA"
        assert reader.file_prefix == "DISPATCHSCADA"
        assert reader.file_suffix == ""

    def test_uses_legacy_suffix(self):
        """Test that stream reader picks up file_suffix for legacy tables."""
        from pyspark.sql.types import StructType

        reader = NemwebStreamReader(StructType([]), {"table": "DISPATCH_REGION"})
        assert reader.folder == "Dispatch_Reports"
        assert reader.file_prefix == "DISPATCH"
        assert reader.file_suffix == "_LEGACY"

    def test_parse_timestamp(self, reader):
        """Test timestamp parsing via nemweb_utils helper."""
        from nemweb_utils import _parse_timestamp_value

        # NEMWEB format
        ts = _parse_timestamp_value("2024/01/01 12:05:00")
        assert ts == datetime(2024, 1, 1, 12, 5, 0)

        # Alternative format
        ts = _parse_timestamp_value("2024-01-01 12:05:00")
        assert ts == datetime(2024, 1, 1, 12, 5, 0)

        # Invalid
        ts = _parse_timestamp_value("invalid")
        assert ts is None

        # Empty
        ts = _parse_timestamp_value("")
        assert ts is None

    def test_to_python_float(self, reader):
        """Test float conversion via nemweb_utils helper."""
        from nemweb_utils import _to_python_float

        result = _to_python_float("123.45")
        assert result == 123.45
        assert isinstance(result, float)

        assert _to_python_float("0") == 0.0
        assert _to_python_float("-100.5") == -100.5
        assert _to_python_float("") is None
        assert _to_python_float(None) is None
        assert _to_python_float("invalid") is None

    def test_to_python_datetime(self, reader):
        """Test datetime conversion returns pure Python datetime."""
        from datetime import datetime
        from nemweb_utils import _to_python_datetime, _parse_timestamp_value

        # Parse timestamp first
        ts = _parse_timestamp_value("2024/01/01 12:05:00")
        result = _to_python_datetime(ts)

        assert result == datetime(2024, 1, 1, 12, 5, 0)
        assert type(result).__module__ == "datetime"
        assert type(result).__name__ == "datetime"

        # None handling
        assert _to_python_datetime(None) is None


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


class TestRegionFiltering:
    """Tests for region filtering logic."""

    def test_tables_with_regionid(self):
        """Tables with REGIONID should support region filtering."""
        assert _has_region_field("DISPATCHREGIONSUM") is True
        assert _has_region_field("DISPATCHPRICE") is True
        assert _has_region_field("TRADINGPRICE") is True
        assert _has_region_field("DISPATCH_REGION") is True
        assert _has_region_field("ROOFTOP_PV_ACTUAL") is True

    def test_tables_without_regionid(self):
        """Tables without REGIONID should not support region filtering."""
        assert _has_region_field("DISPATCH_UNIT_SCADA") is False
        assert _has_region_field("DISPATCH_INTERCONNECTOR") is False
        assert _has_region_field("DISPATCH_INTERCONNECTOR_TRADING") is False

    def test_unknown_table(self):
        """Unknown tables should not support region filtering."""
        assert _has_region_field("NONEXISTENT_TABLE") is False


class TestRowToTuple:
    """Tests for the shared _row_to_tuple function."""

    def test_basic_conversion(self):
        """Test basic row-to-tuple conversion."""
        from pyspark.sql.types import TimestampType, StringType, DoubleType

        fields = [
            ("SETTLEMENTDATE", TimestampType()),
            ("REGIONID", StringType()),
            ("TOTALDEMAND", DoubleType()),
        ]
        row = {
            "SETTLEMENTDATE": "2024/01/01 12:05:00",
            "REGIONID": "NSW1",
            "TOTALDEMAND": "7500.5",
        }
        result = _row_to_tuple(row, fields)

        assert result is not None
        assert result[0] == datetime(2024, 1, 1, 12, 5, 0)
        assert result[1] == "NSW1"
        assert result[2] == 7500.5

    def test_missing_settlementdate_returns_none(self):
        """Test that missing SETTLEMENTDATE causes row to be skipped."""
        from pyspark.sql.types import TimestampType, StringType

        fields = [
            ("SETTLEMENTDATE", TimestampType()),
            ("REGIONID", StringType()),
        ]
        row = {"SETTLEMENTDATE": "", "REGIONID": "NSW1"}
        result = _row_to_tuple(row, fields)
        assert result is None


class TestSchemas:
    """Tests for schema definitions."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_all_tables_have_schemas(self, table):
        """Test that all 8 tables have schema definitions in SCHEMAS."""
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

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_all_tables_in_table_config(self, table):
        """Test that all 8 tables have TABLE_CONFIG entries."""
        assert table in TABLE_CONFIG
        assert "folder" in TABLE_CONFIG[table]
        assert "file_prefix" in TABLE_CONFIG[table]
