"""
Integration tests for nemweb_datasource_arrow.py

Tests the Arrow-based custom PySpark data source implementation.
These tests use mocks for HTTP calls and real file fixtures where available.

Run with: pytest test_nemweb_datasource_arrow.py -v
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock
import hashlib
import io
import zipfile
from pathlib import Path

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Import the module under test
from nemweb_datasource_arrow import (
    NemwebArrowPartition,
    NemwebArrowReader,
    NemwebArrowDataSource,
    SCHEMAS,
    TABLE_TO_FOLDER,
    NEMWEB_CURRENT_URL,
    NEMWEB_ARCHIVE_URL,
)

# Check if pyarrow is available
try:
    import pyarrow
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestNemwebArrowPartition:
    """Tests for NemwebArrowPartition class."""

    def test_partition_with_file_path(self):
        """Should create partition with file_path."""
        partition = NemwebArrowPartition(
            table="DISPATCHREGIONSUM",
            file_path="/path/to/file.zip"
        )

        assert partition.table == "DISPATCHREGIONSUM"
        assert partition.file_path == "/path/to/file.zip"
        assert partition.region is None
        assert partition.date is None
        assert partition.partition_id is not None

    def test_partition_with_region_and_date(self):
        """Should create partition with region and date for HTTP mode."""
        partition = NemwebArrowPartition(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            date="2024-01-01"
        )

        assert partition.table == "DISPATCHREGIONSUM"
        assert partition.region == "NSW1"
        assert partition.date == "2024-01-01"
        assert partition.file_path is None

    def test_partition_id_deterministic_file_path(self):
        """Same file_path should produce same partition ID."""
        p1 = NemwebArrowPartition(table="DISPATCHREGIONSUM", file_path="/path/file.zip")
        p2 = NemwebArrowPartition(table="DISPATCHREGIONSUM", file_path="/path/file.zip")

        assert p1.partition_id == p2.partition_id

    def test_partition_id_deterministic_http(self):
        """Same region+date should produce same partition ID."""
        p1 = NemwebArrowPartition(table="DISPATCHREGIONSUM", region="NSW1", date="2024-01-01")
        p2 = NemwebArrowPartition(table="DISPATCHREGIONSUM", region="NSW1", date="2024-01-01")

        assert p1.partition_id == p2.partition_id

    def test_different_files_different_ids(self):
        """Different file paths should have different IDs."""
        p1 = NemwebArrowPartition(table="DISPATCHREGIONSUM", file_path="/path/file1.zip")
        p2 = NemwebArrowPartition(table="DISPATCHREGIONSUM", file_path="/path/file2.zip")

        assert p1.partition_id != p2.partition_id

    def test_different_regions_different_ids(self):
        """Different regions should have different IDs."""
        p1 = NemwebArrowPartition(table="DISPATCHREGIONSUM", region="NSW1", date="2024-01-01")
        p2 = NemwebArrowPartition(table="DISPATCHREGIONSUM", region="VIC1", date="2024-01-01")

        assert p1.partition_id != p2.partition_id


class TestNemwebArrowReader:
    """Tests for NemwebArrowReader class."""

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

        reader = NemwebArrowReader(schema, options)

        assert reader.table == "DISPATCHREGIONSUM"
        assert reader.regions == ["NSW1", "VIC1"]
        assert reader.start_date == "2024-01-01"
        assert reader.end_date == "2024-01-07"
        assert reader.volume_path is None

    def test_reader_with_volume_path(self):
        """Should detect volume mode when volume_path is set."""
        schema = self.get_test_schema()
        options = {
            "volume_path": "/Volumes/main/nemweb/raw",
            "table": "DISPATCHREGIONSUM"
        }

        reader = NemwebArrowReader(schema, options)

        assert reader.volume_path == "/Volumes/main/nemweb/raw"

    def test_reader_default_options(self):
        """Should use defaults for missing options."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        assert reader.table == "DISPATCHREGIONSUM"
        assert len(reader.regions) == 5  # All 5 NEM regions
        assert reader.volume_path is None

    def test_http_partitions_creates_region_date_partitions(self):
        """HTTP mode should create one partition per region per day."""
        schema = self.get_test_schema()
        options = {
            "regions": "NSW1,VIC1",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02"  # 2 days
        }

        reader = NemwebArrowReader(schema, options)
        partitions = reader._http_partitions()

        # 2 regions * 2 days = 4 partitions
        assert len(partitions) == 4

        # Check partition attributes
        regions = {p.region for p in partitions}
        dates = {p.date for p in partitions}
        assert regions == {"NSW1", "VIC1"}
        assert dates == {"2024-01-01", "2024-01-02"}

    def test_volume_partitions_empty_when_path_not_exists(self):
        """Should return empty list if volume path doesn't exist."""
        schema = self.get_test_schema()
        options = {"volume_path": "/nonexistent/path"}

        reader = NemwebArrowReader(schema, options)
        partitions = reader._volume_partitions()

        assert partitions == []

    def test_volume_partitions_filters_by_date_range(self, tmp_path):
        """Volume partitions should only include files within date range."""
        # Create archive subfolder with test files
        archive_dir = tmp_path / "dispatchis" / "archive"
        archive_dir.mkdir(parents=True)

        # Create files for different dates
        (archive_dir / "PUBLIC_DISPATCHIS_20240101.zip").touch()
        (archive_dir / "PUBLIC_DISPATCHIS_20240102.zip").touch()
        (archive_dir / "PUBLIC_DISPATCHIS_20240103.zip").touch()
        (archive_dir / "PUBLIC_DISPATCHIS_20240115.zip").touch()  # Outside range

        schema = self.get_test_schema()
        options = {
            "volume_path": str(tmp_path),
            "start_date": "2024-01-01",
            "end_date": "2024-01-03"
        }

        reader = NemwebArrowReader(schema, options)
        partitions = reader._volume_partitions()

        # Should only include 3 files (Jan 1-3), not Jan 15
        assert len(partitions) == 3
        file_names = [p.file_path.split("/")[-1] for p in partitions]
        assert "PUBLIC_DISPATCHIS_20240101.zip" in file_names
        assert "PUBLIC_DISPATCHIS_20240102.zip" in file_names
        assert "PUBLIC_DISPATCHIS_20240103.zip" in file_names
        assert "PUBLIC_DISPATCHIS_20240115.zip" not in file_names

    def test_build_url_recent_date(self):
        """Recent dates should use CURRENT folder."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        recent_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        url = reader._build_url("DISPATCHREGIONSUM", recent_date)

        # Daily consolidated files are always in ARCHIVE
        # CURRENT only has 5-minute interval files
        assert NEMWEB_ARCHIVE_URL in url
        assert "DispatchIS_Reports" in url
        assert "PUBLIC_DISPATCHIS_" in url

    def test_build_url_old_date(self):
        """Old dates should use ARCHIVE folder."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        old_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        url = reader._build_url("DISPATCHREGIONSUM", old_date)

        assert NEMWEB_ARCHIVE_URL in url

    def test_parse_timestamp_slash_format(self):
        """Should parse NEMWEB slash timestamp format."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        result = reader._parse_timestamp("2024/01/15 12:30:00")

        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 12
        assert result.minute == 30

    def test_parse_timestamp_dash_format(self):
        """Should parse ISO timestamp format."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        result = reader._parse_timestamp("2024-01-15 12:30:00")

        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_parse_timestamp_none(self):
        """Should return None for empty/None input."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        assert reader._parse_timestamp(None) is None
        assert reader._parse_timestamp("") is None
        assert reader._parse_timestamp("   ") is None

    def test_parse_timestamp_invalid(self):
        """Should return None for invalid timestamp."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        assert reader._parse_timestamp("not-a-date") is None

    def test_to_float_valid(self):
        """Should convert valid string to float."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        assert reader._to_float("123.45") == 123.45
        assert reader._to_float("100") == 100.0
        assert reader._to_float("-50.5") == -50.5

    def test_to_float_invalid(self):
        """Should return None for invalid float."""
        schema = self.get_test_schema()
        reader = NemwebArrowReader(schema, {})

        assert reader._to_float(None) is None
        assert reader._to_float("") is None
        assert reader._to_float("not-a-number") is None


class TestNemwebArrowReaderParseCsv:
    """Tests for CSV parsing in NemwebArrowReader."""

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        return NemwebArrowReader(schema, {})

    def test_parse_csv_nemweb_format(self):
        """Should parse NEMWEB multi-record CSV format."""
        reader = self.get_reader()

        csv_content = """C,HEADER,INFO
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,REGIONID,TOTALDEMAND
D,DISPATCH,REGIONSUM,1,"2024/01/01 00:05:00",NSW1,7500.5
D,DISPATCH,REGIONSUM,1,"2024/01/01 00:05:00",VIC1,5200.3
C,END OF REPORT
"""
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        rows = reader._parse_csv(csv_file, "DISPATCH,REGIONSUM")

        assert len(rows) == 2
        assert rows[0]["REGIONID"] == "NSW1"
        assert rows[1]["REGIONID"] == "VIC1"
        assert rows[0]["TOTALDEMAND"] == "7500.5"

    def test_parse_csv_ignores_other_record_types(self):
        """Should only return rows matching the record type."""
        reader = self.get_reader()

        csv_content = """C,HEADER
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,REGIONID
D,DISPATCH,REGIONSUM,1,"2024/01/01 00:05:00",NSW1
I,DISPATCH,PRICE,1,SETTLEMENTDATE,REGIONID,RRP
D,DISPATCH,PRICE,1,"2024/01/01 00:05:00",NSW1,100.5
C,END
"""
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        rows = reader._parse_csv(csv_file, "DISPATCH,REGIONSUM")

        # Should only get REGIONSUM rows, not PRICE
        assert len(rows) == 1
        assert "RRP" not in rows[0]

    def test_parse_csv_standard_format(self):
        """Should handle standard CSV when no record_type specified."""
        reader = self.get_reader()

        csv_content = """REGIONID,TOTALDEMAND
NSW1,7500.5
VIC1,5200.3
"""
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        rows = reader._parse_csv(csv_file, None)

        assert len(rows) == 2
        assert rows[0]["REGIONID"] == "NSW1"


class TestNemwebArrowReaderWithFixtures:
    """Tests using real NEMWEB CSV fixtures."""

    @pytest.fixture
    def sample_csv_content(self):
        """Load the sample DISPATCHIS CSV fixture."""
        fixture_path = FIXTURES_DIR / "sample_dispatchis.csv"
        if fixture_path.exists():
            return fixture_path.read_text()
        pytest.skip("Fixture not found")

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        return NemwebArrowReader(schema, {"regions": "NSW1,VIC1,QLD1,SA1,TAS1"})

    def test_parse_real_fixture(self, sample_csv_content):
        """Should parse real NEMWEB fixture correctly."""
        reader = self.get_reader()

        csv_file = io.BytesIO(sample_csv_content.encode("utf-8"))
        rows = reader._parse_csv(csv_file, "DISPATCH,REGIONSUM")

        assert len(rows) == 5  # 5 regions
        regions = {row["REGIONID"] for row in rows}
        assert regions == {"NSW1", "VIC1", "QLD1", "SA1", "TAS1"}

    def test_timestamp_parsing_from_fixture(self, sample_csv_content):
        """Should correctly parse timestamps from fixture."""
        reader = self.get_reader()

        csv_file = io.BytesIO(sample_csv_content.encode("utf-8"))
        rows = reader._parse_csv(csv_file, "DISPATCH,REGIONSUM")

        # Parse timestamp from first row
        ts = reader._parse_timestamp(rows[0]["SETTLEMENTDATE"])

        assert isinstance(ts, datetime)
        assert ts.year == 2025
        assert ts.month == 12
        assert ts.day == 27


class TestNemwebArrowDataSource:
    """Tests for NemwebArrowDataSource class."""

    def test_data_source_name(self):
        """Should return 'nemweb_arrow' as format name."""
        assert NemwebArrowDataSource.name() == "nemweb_arrow"

    def test_schema_dispatchregionsum(self):
        """Should return correct schema for DISPATCHREGIONSUM."""
        ds = NemwebArrowDataSource.__new__(NemwebArrowDataSource)
        ds.options = {"table": "DISPATCHREGIONSUM"}

        schema = ds.schema()

        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names
        assert "TOTALDEMAND" in field_names

        # SETTLEMENTDATE should be TimestampType (Arrow handles conversion)
        ts_field = [f for f in schema.fields if f.name == "SETTLEMENTDATE"][0]
        assert isinstance(ts_field.dataType, TimestampType)

    def test_schema_dispatchprice(self):
        """Should return correct schema for DISPATCHPRICE."""
        ds = NemwebArrowDataSource.__new__(NemwebArrowDataSource)
        ds.options = {"table": "DISPATCHPRICE"}

        schema = ds.schema()

        field_names = [f.name for f in schema.fields]
        assert "RRP" in field_names
        assert "EEP" in field_names

    def test_schema_tradingprice(self):
        """Should return correct schema for TRADINGPRICE."""
        ds = NemwebArrowDataSource.__new__(NemwebArrowDataSource)
        ds.options = {"table": "TRADINGPRICE"}

        schema = ds.schema()

        field_names = [f.name for f in schema.fields]
        assert "PERIODID" in field_names
        assert "RRP" in field_names

    def test_schema_unknown_table_defaults_to_dispatchregionsum(self):
        """Unknown table should fall back to DISPATCHREGIONSUM schema."""
        ds = NemwebArrowDataSource.__new__(NemwebArrowDataSource)
        ds.options = {"table": "UNKNOWN_TABLE"}

        schema = ds.schema()

        # Should default to DISPATCHREGIONSUM
        field_names = [f.name for f in schema.fields]
        assert "TOTALDEMAND" in field_names

    def test_reader_returns_arrow_reader(self):
        """Should return NemwebArrowReader instance."""
        ds = NemwebArrowDataSource.__new__(NemwebArrowDataSource)
        ds.options = {"table": "DISPATCHREGIONSUM"}

        schema = StructType([StructField("REGIONID", StringType(), True)])
        reader = ds.reader(schema)

        assert isinstance(reader, NemwebArrowReader)


class TestSchemaDefinitions:
    """Tests for SCHEMAS and TABLE_TO_FOLDER constants."""

    def test_all_tables_have_schemas(self):
        """All expected tables should have schema definitions."""
        expected_tables = ["DISPATCHREGIONSUM", "DISPATCHPRICE", "TRADINGPRICE"]

        for table in expected_tables:
            assert table in SCHEMAS
            assert "record_type" in SCHEMAS[table]
            assert "fields" in SCHEMAS[table]
            assert len(SCHEMAS[table]["fields"]) > 0

    def test_all_tables_have_folder_mapping(self):
        """All expected tables should have URL folder mapping."""
        expected_tables = ["DISPATCHREGIONSUM", "DISPATCHPRICE", "TRADINGPRICE"]

        for table in expected_tables:
            assert table in TABLE_TO_FOLDER
            folder, prefix = TABLE_TO_FOLDER[table]
            assert isinstance(folder, str)
            assert isinstance(prefix, str)

    def test_schema_field_types(self):
        """Schema fields should have correct types."""
        for table_name, config in SCHEMAS.items():
            for name, dtype in config["fields"]:
                assert isinstance(name, str)
                assert isinstance(dtype, (TimestampType, StringType, DoubleType))

    def test_settlementdate_is_first_field(self):
        """SETTLEMENTDATE should be first field in all schemas."""
        for table_name, config in SCHEMAS.items():
            first_field_name, first_field_type = config["fields"][0]
            assert first_field_name == "SETTLEMENTDATE"
            assert isinstance(first_field_type, TimestampType)


class TestArrowSchemaBuilding:
    """Tests for Arrow schema building."""

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        return NemwebArrowReader(schema, {})

    @pytest.mark.skipif(
        not HAS_PYARROW,
        reason="PyArrow not installed in test environment"
    )
    def test_build_arrow_schema(self):
        """Should build valid PyArrow schema from field definitions."""
        import pyarrow as pa

        reader = self.get_reader()
        fields = SCHEMAS["DISPATCHREGIONSUM"]["fields"]

        arrow_schema = reader._build_arrow_schema(fields)

        assert isinstance(arrow_schema, pa.Schema)
        assert len(arrow_schema) == len(fields)

    @pytest.mark.skipif(
        not HAS_PYARROW,
        reason="PyArrow not installed in test environment"
    )
    def test_rows_to_record_batch(self):
        """Should convert rows to PyArrow RecordBatch."""
        import pyarrow as pa

        reader = self.get_reader()
        fields = [
            ("SETTLEMENTDATE", TimestampType()),
            ("REGIONID", StringType()),
            ("TOTALDEMAND", DoubleType()),
        ]
        arrow_schema = reader._build_arrow_schema(fields)

        rows = [
            {"SETTLEMENTDATE": "2024/01/01 00:05:00", "REGIONID": "NSW1", "TOTALDEMAND": "7500.5"},
            {"SETTLEMENTDATE": "2024/01/01 00:05:00", "REGIONID": "VIC1", "TOTALDEMAND": "5200.3"},
        ]

        batch = reader._rows_to_record_batch(rows, fields, arrow_schema)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 2


class TestHTTPIntegration:
    """Integration tests for HTTP mode (with mocked HTTP)."""

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {
            "table": "DISPATCHREGIONSUM",
            "regions": "NSW1",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01"
        }
        return NemwebArrowReader(schema, options)

    def create_mock_zip(self, csv_content: str) -> bytes:
        """Create a mock ZIP file with CSV content."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.CSV", csv_content)
        return zip_buffer.getvalue()

    @pytest.mark.skipif(
        not HAS_PYARROW,
        reason="PyArrow not installed in test environment"
    )
    def test_read_http_404_handling(self):
        """Should handle 404 errors gracefully."""
        from urllib.error import HTTPError

        reader = self.get_reader()
        partition = NemwebArrowPartition(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            date="2024-01-01"
        )

        error = HTTPError("http://example.com", 404, "Not Found", {}, None)

        # Patch at the urllib.request module level since it's imported inside the method
        with patch("urllib.request.urlopen", side_effect=error):
            results = list(reader._read_http(partition))

        # Should return empty list, not raise
        assert results == []


class TestDownloadOptions:
    """Tests for auto-download configuration options."""

    def test_auto_download_default_false(self):
        """auto_download should default to False."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM"}
        reader = NemwebArrowReader(schema, options)

        assert reader.auto_download is False

    def test_auto_download_enabled(self):
        """auto_download=true should enable downloading."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM", "auto_download": "true"}
        reader = NemwebArrowReader(schema, options)

        assert reader.auto_download is True

    def test_auto_download_case_insensitive(self):
        """auto_download should be case-insensitive."""
        schema = StructType([StructField("REGIONID", StringType(), True)])

        for value in ["True", "TRUE", "true", "TrUe"]:
            options = {"table": "DISPATCHREGIONSUM", "auto_download": value}
            reader = NemwebArrowReader(schema, options)
            assert reader.auto_download is True

    def test_max_workers_default(self):
        """max_workers should default to 8."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM"}
        reader = NemwebArrowReader(schema, options)

        assert reader.max_workers == 8

    def test_max_workers_custom(self):
        """max_workers can be customized."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM", "max_workers": "4"}
        reader = NemwebArrowReader(schema, options)

        assert reader.max_workers == 4

    def test_skip_existing_default_true(self):
        """skip_existing should default to True."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM"}
        reader = NemwebArrowReader(schema, options)

        assert reader.skip_existing is True

    def test_skip_existing_disabled(self):
        """skip_existing=false should disable skipping."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {"table": "DISPATCHREGIONSUM", "skip_existing": "false"}
        reader = NemwebArrowReader(schema, options)

        assert reader.skip_existing is False


class TestBuildDownloadUrl:
    """Tests for _build_download_url method."""

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        return NemwebArrowReader(schema, {})

    def test_all_dates_use_archive_url(self):
        """All dates use ARCHIVE for daily consolidated files.

        CURRENT folder only has 5-minute interval files, not daily aggregates.
        """
        reader = self.get_reader()
        # Use yesterday's date
        recent_date = datetime.now() - timedelta(days=1)

        url = reader._build_download_url("DispatchIS_Reports", "DISPATCHIS", recent_date)

        # Daily files are always in ARCHIVE
        assert "ARCHIVE" in url
        assert "DispatchIS_Reports" in url

    def test_old_date_uses_archive_url(self):
        """Dates older than 7 days should use ARCHIVE URL."""
        reader = self.get_reader()
        old_date = datetime.now() - timedelta(days=30)

        url = reader._build_download_url("DispatchIS_Reports", "DISPATCHIS", old_date)

        assert "ARCHIVE" in url
        assert "DispatchIS_Reports" in url

    def test_url_format(self):
        """URL should have correct format."""
        reader = self.get_reader()
        test_date = datetime(2024, 6, 15)

        url = reader._build_download_url("DispatchIS_Reports", "DISPATCHIS", test_date)

        assert "PUBLIC_DISPATCHIS_20240615.zip" in url


class TestDownloadSingleFile:
    """Tests for _download_single_file method."""

    def get_reader(self):
        """Get a reader instance for testing."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        return NemwebArrowReader(schema, {})

    def test_successful_download(self):
        """Should return success dict on successful download."""
        import tempfile
        import os

        reader = self.get_reader()

        # Create mock response
        mock_response = MagicMock()
        mock_response.read.return_value = b"test data content"
        mock_response.__enter__ = lambda s: mock_response
        mock_response.__exit__ = MagicMock(return_value=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = os.path.join(tmpdir, "test.zip")

            with patch("urllib.request.urlopen", return_value=mock_response):
                result = reader._download_single_file("http://example.com/test.zip", dest_path)

            assert result["success"] is True
            assert result["size"] == len(b"test data content")
            assert result["error"] is None
            assert os.path.exists(dest_path)

    def test_404_returns_not_found(self):
        """Should return not_found error on 404."""
        import tempfile
        import os
        from urllib.error import HTTPError

        reader = self.get_reader()
        error = HTTPError("http://example.com", 404, "Not Found", {}, None)

        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = os.path.join(tmpdir, "test.zip")

            with patch("urllib.request.urlopen", side_effect=error):
                result = reader._download_single_file("http://example.com/test.zip", dest_path)

            assert result["success"] is False
            assert result["error"] == "not_found"

    def test_timeout_retries(self):
        """Should retry on timeout errors."""
        import tempfile
        import os
        from urllib.error import URLError

        reader = self.get_reader()
        error = URLError("Connection timed out")

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise error

        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = os.path.join(tmpdir, "test.zip")

            with patch("urllib.request.urlopen", side_effect=side_effect):
                with patch("time.sleep"):  # Skip actual sleep
                    result = reader._download_single_file("http://example.com/test.zip", dest_path)

            # Should have retried MAX_RETRIES times (3 by default)
            assert call_count == 3
            assert result["success"] is False


class TestDownloadToVolume:
    """Tests for _download_to_volume method."""

    def get_reader(self, volume_path, start_date="2024-01-01", end_date="2024-01-03"):
        """Get a reader instance with volume path."""
        schema = StructType([StructField("REGIONID", StringType(), True)])
        options = {
            "table": "DISPATCHREGIONSUM",
            "volume_path": volume_path,
            "start_date": start_date,
            "end_date": end_date,
            "auto_download": "true",
            "max_workers": "2"
        }
        return NemwebArrowReader(schema, options)

    def test_creates_table_subdirectory(self):
        """Should create file prefix subdirectory in volume.

        Directory structure is based on file_prefix (dispatchis) not table name,
        allowing DISPATCHREGIONSUM and DISPATCHPRICE to share downloaded files.
        """
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            reader = self.get_reader(tmpdir)

            # Mock the download to avoid actual HTTP calls
            with patch.object(reader, "_download_single_file") as mock_download:
                mock_download.return_value = {"success": True, "error": None}
                reader._download_to_volume()

            # Should have created dispatchis/archive subdirectory (not dispatchregionsum)
            archive_path = os.path.join(tmpdir, "dispatchis", "archive")
            assert os.path.exists(archive_path)
            assert os.path.isdir(archive_path)

    def test_skips_existing_files(self):
        """Should skip existing files when skip_existing is True."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            reader = self.get_reader(tmpdir, "2024-01-01", "2024-01-01")

            # Create the actual directory structure: dispatchis/archive/
            archive_path = os.path.join(tmpdir, "dispatchis", "archive")
            os.makedirs(archive_path)
            existing_file = os.path.join(archive_path, "PUBLIC_DISPATCHIS_20240101.zip")
            with open(existing_file, 'wb') as f:
                f.write(b"existing data")

            with patch.object(reader, "_download_single_file") as mock_download:
                reader._download_to_volume()

            # Should not have called download for existing file
            mock_download.assert_not_called()

    def test_downloads_missing_files(self):
        """Should download files that don't exist."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            reader = self.get_reader(tmpdir, "2024-01-01", "2024-01-02")

            with patch.object(reader, "_download_single_file") as mock_download:
                mock_download.return_value = {"success": True, "error": None}
                reader._download_to_volume()

            # Should have called download twice (two days)
            assert mock_download.call_count == 2


class TestPartitionsWithAutoDownload:
    """Tests for partitions() method with auto_download enabled."""

    def test_partitions_triggers_download_when_enabled(self):
        """partitions() should trigger download when auto_download is true."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            schema = StructType([StructField("REGIONID", StringType(), True)])
            options = {
                "table": "DISPATCHREGIONSUM",
                "volume_path": tmpdir,
                "start_date": "2024-01-01",
                "end_date": "2024-01-01",
                "auto_download": "true"
            }
            reader = NemwebArrowReader(schema, options)

            with patch.object(reader, "_download_to_volume") as mock_download:
                reader.partitions()

            mock_download.assert_called_once()

    def test_partitions_skips_download_when_disabled(self):
        """partitions() should not download when auto_download is false."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            schema = StructType([StructField("REGIONID", StringType(), True)])
            options = {
                "table": "DISPATCHREGIONSUM",
                "volume_path": tmpdir,
                "auto_download": "false"
            }
            reader = NemwebArrowReader(schema, options)

            with patch.object(reader, "_download_to_volume") as mock_download:
                reader.partitions()

            mock_download.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
