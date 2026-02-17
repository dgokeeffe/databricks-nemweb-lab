"""
Unit tests for nemweb_utils.py

Tests the utility functions for fetching and parsing AEMO NEMWEB data.
Run with: pytest test_nemweb_utils.py -v
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock
from urllib.error import HTTPError, URLError
import io
import zipfile

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# Import the module under test (path configured in conftest.py)
from nemweb_utils import (
    SCHEMAS,
    TABLE_CONFIG,
    fetch_with_retry,
    fetch_nemweb_data,
    fetch_nemweb_current,
    get_table_schema,
    extract_rows_from_zip,
    list_current_files,
    _build_nemweb_url,
    _get_sample_data,
    _parse_timestamp_value,
    parse_nemweb_csv,
    get_nemweb_schema,
    list_available_tables,
    get_nem_regions,
    TABLE_TO_FOLDER,
    NEMWEB_CURRENT_URL,
    NEMWEB_ARCHIVE_URL,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)

# All 8 tables that should be in SCHEMAS
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


class TestFetchWithRetry:
    """Tests for fetch_with_retry function."""

    def test_successful_fetch_first_try(self):
        """Should return data on successful first attempt."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"test data"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            result = fetch_with_retry("http://example.com/data")
            assert result == b"test data"

    def test_retries_on_transient_error(self):
        """Should retry on transient errors."""
        import time
        mock_response = MagicMock()
        mock_response.read.return_value = b"success"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise URLError("Connection refused")
            return mock_response

        with patch("nemweb_utils.urlopen", side_effect=side_effect):
            with patch.object(time, "sleep"):  # Skip actual sleep
                result = fetch_with_retry("http://example.com/data")
                assert result == b"success"
                assert call_count == 2

    def test_raises_after_max_retries(self):
        """Should raise after exhausting retries."""
        import time
        with patch("nemweb_utils.urlopen", side_effect=URLError("Connection refused")):
            with patch.object(time, "sleep"):
                with pytest.raises(URLError):
                    fetch_with_retry("http://example.com/data", max_retries=3)

    def test_no_retry_on_404(self):
        """Should not retry on 404 errors."""
        error = HTTPError("http://example.com", 404, "Not Found", {}, None)

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise error

        with patch("nemweb_utils.urlopen", side_effect=side_effect):
            with pytest.raises(HTTPError) as exc_info:
                fetch_with_retry("http://example.com/data")
            assert exc_info.value.code == 404
            assert call_count == 1  # Only one attempt

    def test_exponential_backoff_delays(self):
        """Should use exponential backoff for delays."""
        import time
        sleep_calls = []

        original_sleep = time.sleep
        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        with patch("nemweb_utils.urlopen", side_effect=URLError("fail")):
            with patch.object(time, "sleep", side_effect=mock_sleep):
                with pytest.raises(URLError):
                    fetch_with_retry("http://example.com", max_retries=4, base_delay=1.0)

        # Should have delays: 1, 2, 4 (before attempts 2, 3, 4)
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == 1.0
        assert sleep_calls[1] == 2.0
        assert sleep_calls[2] == 4.0


class TestBuildNemwebUrl:
    """Tests for _build_nemweb_url function."""

    def test_recent_date_uses_archive(self):
        """All dates use ARCHIVE for daily consolidated files.

        CURRENT folder only has 5-minute interval files, not daily aggregates.
        Use fetch_nemweb_current() for CURRENT folder access.
        """
        recent_date = datetime.now() - timedelta(days=2)
        url = _build_nemweb_url("Dispatch_SCADA", "DISPATCHREGIONSUM", recent_date)

        # Daily files are always in ARCHIVE
        assert NEMWEB_ARCHIVE_URL in url
        assert "Dispatch_SCADA" in url
        assert "DISPATCHREGIONSUM" in url
        assert url.endswith(".zip")

    def test_old_date_uses_archive(self):
        """Old dates should use ARCHIVE folder."""
        old_date = datetime.now() - timedelta(days=30)
        url = _build_nemweb_url("Dispatch_SCADA", "DISPATCHREGIONSUM", old_date)

        assert NEMWEB_ARCHIVE_URL in url

    def test_date_format_in_filename(self):
        """Filename should contain date in YYYYMMDD format."""
        test_date = datetime(2024, 6, 15)
        url = _build_nemweb_url("Dispatch_SCADA", "DISPATCHREGIONSUM", test_date)

        assert "20240615" in url

    def test_url_structure(self):
        """URL should follow NEMWEB structure."""
        test_date = datetime.now() - timedelta(days=30)
        url = _build_nemweb_url("DispatchIS_Reports", "DISPATCHPRICE", test_date)

        # Should be: base/folder/PUBLIC_TABLE_YYYYMMDD.zip
        assert url.startswith(NEMWEB_ARCHIVE_URL)
        assert "/DispatchIS_Reports/" in url
        assert "PUBLIC_DISPATCHPRICE_" in url


class TestGetSampleData:
    """Tests for _get_sample_data function."""

    def test_returns_list_of_dicts(self):
        """Should return a list of dictionaries."""
        data = _get_sample_data("DISPATCHREGIONSUM")
        assert isinstance(data, list)
        assert all(isinstance(row, dict) for row in data)

    def test_sample_has_all_regions(self):
        """Sample data should cover all 5 NEM regions."""
        data = _get_sample_data("DISPATCHREGIONSUM")
        regions = {row["REGIONID"] for row in data}
        expected_regions = {"NSW1", "VIC1", "QLD1", "SA1", "TAS1"}
        assert regions == expected_regions

    def test_region_filter_works(self):
        """Should filter by region when specified."""
        data = _get_sample_data("DISPATCHREGIONSUM", region="NSW1")
        assert all(row["REGIONID"] == "NSW1" for row in data)
        assert len(data) == 1

    def test_sample_has_required_fields(self):
        """Sample data should have required NEMWEB fields."""
        data = _get_sample_data("DISPATCHREGIONSUM")
        required_fields = {"SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"}
        for row in data:
            assert required_fields.issubset(set(row.keys()))


class TestParseTimestampValue:
    """Tests for _parse_timestamp_value function."""

    def test_slash_format(self):
        """Should parse NEMWEB slash format."""
        result = _parse_timestamp_value("2024/01/15 12:30:00")
        assert result == datetime(2024, 1, 15, 12, 30, 0)

    def test_dash_format(self):
        """Should parse dash format."""
        result = _parse_timestamp_value("2024-01-15 12:30:00")
        assert result == datetime(2024, 1, 15, 12, 30, 0)

    def test_null_value(self):
        """Should return None for null/empty values."""
        assert _parse_timestamp_value(None) is None
        assert _parse_timestamp_value("") is None

    def test_invalid_format(self):
        """Should return None for invalid format."""
        assert _parse_timestamp_value("invalid-date") is None


class TestParseNemwebCsv:
    """Tests for parse_nemweb_csv function."""

    def test_parses_to_tuples(self):
        """Should parse dicts to tuples matching schema."""
        schema = StructType([
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

        data = [{"REGIONID": "NSW1", "TOTALDEMAND": "7500.5"}]
        result = list(parse_nemweb_csv(data, schema))

        assert len(result) == 1
        assert result[0] == ("NSW1", 7500.5)

    def test_handles_missing_fields(self):
        """Should handle missing fields with None."""
        schema = StructType([
            StructField("REGIONID", StringType(), True),
            StructField("MISSING", DoubleType(), True),
        ])

        data = [{"REGIONID": "NSW1"}]
        result = list(parse_nemweb_csv(data, schema))

        assert result[0] == ("NSW1", None)

    def test_preserves_schema_order(self):
        """Should output fields in schema order."""
        schema = StructType([
            StructField("B", StringType(), True),
            StructField("A", StringType(), True),
            StructField("C", StringType(), True),
        ])

        data = [{"A": "1", "B": "2", "C": "3"}]
        result = list(parse_nemweb_csv(data, schema))

        assert result[0] == ("2", "1", "3")

    def test_parses_multiple_rows(self):
        """Should parse multiple rows."""
        schema = StructType([
            StructField("REGIONID", StringType(), True),
        ])

        data = [
            {"REGIONID": "NSW1"},
            {"REGIONID": "VIC1"},
            {"REGIONID": "QLD1"},
        ]
        result = list(parse_nemweb_csv(data, schema))

        assert len(result) == 3
        assert [r[0] for r in result] == ["NSW1", "VIC1", "QLD1"]

    def test_parse_sample_data_with_dispatchregionsum_schema(self):
        """
        Test parsing sample data with DISPATCHREGIONSUM schema (matching solution notebook).
        
        This test verifies that:
        1. fetch_nemweb_current with use_sample=True returns correct data
        2. parse_nemweb_csv correctly converts to tuples matching schema
        3. Present fields are parsed correctly (timestamps, strings, doubles)
        4. Missing fields are set to None (which is correct behavior)
        
        This matches the test case from solutions/01_custom_source_solution.ipynb
        """
        from datetime import datetime
        
        # Define schema matching solution notebook (with TimestampType for SETTLEMENTDATE)
        def get_dispatchregionsum_schema() -> StructType:
            """Return the schema for DISPATCHREGIONSUM table (matching solution notebook)."""
            return StructType([
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
        
        schema = get_dispatchregionsum_schema()
        
        # Fetch sample data (matching solution notebook call)
        data = fetch_nemweb_current(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            max_files=6,
            use_sample=True,  # Use sample data for testing
            debug=False  # Don't print debug info in tests
        )
        
        # Verify we got data
        assert len(data) > 0, "Should get at least one row"
        assert data[0]["REGIONID"] == "NSW1", "Should filter by region"
        
        # Parse to tuples
        tuples = list(parse_nemweb_csv(data, schema))
        
        # Verify we got tuples
        assert len(tuples) > 0, "Should parse at least one tuple"
        assert len(tuples) == len(data), "Should parse all rows"
        
        # Verify first tuple structure (matching expected output from solution notebook)
        first_tuple = tuples[0]
        
        # Verify tuple has correct length (12 fields)
        assert len(first_tuple) == 12, f"Expected 12 fields, got {len(first_tuple)}"
        
        # Verify SETTLEMENTDATE is datetime (index 0)
        assert isinstance(first_tuple[0], datetime), \
            f"SETTLEMENTDATE should be datetime, got {type(first_tuple[0]).__name__}"
        assert first_tuple[0] == datetime(2024, 1, 1, 0, 5), \
            f"SETTLEMENTDATE should be 2024-01-01 00:05:00, got {first_tuple[0]}"
        
        # Verify RUNNO is string (index 1)
        assert first_tuple[1] == "1", f"RUNNO should be '1', got {first_tuple[1]}"
        
        # Verify REGIONID is string (index 2)
        assert first_tuple[2] == "NSW1", f"REGIONID should be 'NSW1', got {first_tuple[2]}"
        
        # Verify DISPATCHINTERVAL is string (index 3)
        assert first_tuple[3] == "1", f"DISPATCHINTERVAL should be '1', got {first_tuple[3]}"
        
        # Verify INTERVENTION is string (index 4)
        assert first_tuple[4] == "0", f"INTERVENTION should be '0', got {first_tuple[4]}"
        
        # Verify TOTALDEMAND is float (index 5)
        assert isinstance(first_tuple[5], float), \
            f"TOTALDEMAND should be float, got {type(first_tuple[5]).__name__}"
        assert first_tuple[5] == 7500.5, f"TOTALDEMAND should be 7500.5, got {first_tuple[5]}"
        
        # Verify AVAILABLEGENERATION is float (index 6)
        assert isinstance(first_tuple[6], float), \
            f"AVAILABLEGENERATION should be float, got {type(first_tuple[6]).__name__}"
        assert first_tuple[6] == 8000.0, f"AVAILABLEGENERATION should be 8000.0, got {first_tuple[6]}"
        
        # Verify missing fields are None (indices 7-10)
        # These fields (AVAILABLELOAD, DEMANDFORECAST, DISPATCHABLEGENERATION, DISPATCHABLELOAD)
        # are not in the sample data, so they should be None
        assert first_tuple[7] is None, \
            f"AVAILABLELOAD should be None (missing from sample data), got {first_tuple[7]}"
        assert first_tuple[8] is None, \
            f"DEMANDFORECAST should be None (missing from sample data), got {first_tuple[8]}"
        assert first_tuple[9] is None, \
            f"DISPATCHABLEGENERATION should be None (missing from sample data), got {first_tuple[9]}"
        assert first_tuple[10] is None, \
            f"DISPATCHABLELOAD should be None (missing from sample data), got {first_tuple[10]}"
        
        # Verify NETINTERCHANGE is float (index 11)
        assert isinstance(first_tuple[11], float), \
            f"NETINTERCHANGE should be float, got {type(first_tuple[11]).__name__}"
        assert first_tuple[11] == -200.5, f"NETINTERCHANGE should be -200.5, got {first_tuple[11]}"
        
        # Verify the tuple matches expected structure exactly
        expected_tuple = (
            datetime(2024, 1, 1, 0, 5),  # SETTLEMENTDATE
            '1',                          # RUNNO
            'NSW1',                      # REGIONID
            '1',                         # DISPATCHINTERVAL
            '0',                         # INTERVENTION
            7500.5,                      # TOTALDEMAND
            8000.0,                      # AVAILABLEGENERATION
            None,                        # AVAILABLELOAD (missing)
            None,                        # DEMANDFORECAST (missing)
            None,                        # DISPATCHABLEGENERATION (missing)
            None,                        # DISPATCHABLELOAD (missing)
            -200.5,                      # NETINTERCHANGE
        )
        assert first_tuple == expected_tuple, \
            f"Tuple doesn't match expected structure.\nGot:      {first_tuple}\nExpected: {expected_tuple}"


class TestGetNemwebSchema:
    """Tests for get_nemweb_schema function."""

    def test_dispatchregionsum_schema(self):
        """Should return schema for DISPATCHREGIONSUM."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        assert isinstance(schema, StructType)

        field_names = [f.name for f in schema.fields]
        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names
        assert "TOTALDEMAND" in field_names

    def test_dispatchprice_schema(self):
        """Should return schema for DISPATCHPRICE."""
        schema = get_nemweb_schema("DISPATCHPRICE")
        field_names = [f.name for f in schema.fields]

        assert "RRP" in field_names  # Regional Reference Price
        assert "REGIONID" in field_names

    def test_tradingprice_schema(self):
        """Should return schema for TRADINGPRICE."""
        schema = get_nemweb_schema("TRADINGPRICE")
        field_names = [f.name for f in schema.fields]

        assert "RRP" in field_names
        assert "PERIODID" in field_names

    def test_unknown_table_returns_generic_schema(self):
        """Should return generic schema for unknown tables."""
        schema = get_nemweb_schema("UNKNOWN_TABLE")
        field_names = [f.name for f in schema.fields]

        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names

    def test_schema_field_types(self):
        """Schema fields should have correct types.

        NOTE: SETTLEMENTDATE is StringType for Serverless compatibility.
        Cast to timestamp in Spark: to_timestamp(col("SETTLEMENTDATE"))
        """
        schema = get_nemweb_schema("DISPATCHREGIONSUM")

        for field in schema.fields:
            if field.name == "SETTLEMENTDATE":
                # StringType for Serverless compatibility
                assert isinstance(field.dataType, StringType)
            elif field.name == "REGIONID":
                assert isinstance(field.dataType, StringType)
            elif field.name == "TOTALDEMAND":
                assert isinstance(field.dataType, DoubleType)


class TestListAvailableTables:
    """Tests for list_available_tables function."""

    def test_returns_list(self):
        """Should return a list of table names."""
        tables = list_available_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0

    def test_contains_expected_tables(self):
        """Should contain expected NEMWEB tables."""
        tables = list_available_tables()
        assert "DISPATCHREGIONSUM" in tables
        assert "DISPATCHPRICE" in tables

    def test_matches_table_to_folder_keys(self):
        """Should match TABLE_TO_FOLDER keys."""
        tables = list_available_tables()
        assert set(tables) == set(TABLE_TO_FOLDER.keys())


class TestGetNemRegions:
    """Tests for get_nem_regions function."""

    def test_returns_five_regions(self):
        """Should return exactly 5 NEM regions."""
        regions = get_nem_regions()
        assert len(regions) == 5

    def test_contains_all_regions(self):
        """Should contain all NEM region IDs."""
        regions = get_nem_regions()
        expected = {"NSW1", "QLD1", "SA1", "VIC1", "TAS1"}
        assert set(regions) == expected


class TestFetchNemwebData:
    """Tests for fetch_nemweb_data function."""

    def test_use_sample_returns_sample_data(self):
        """Should return sample data when use_sample=True."""
        data = fetch_nemweb_data(
            table="DISPATCHREGIONSUM",
            use_sample=True
        )
        assert isinstance(data, list)
        assert len(data) == 5  # One per region

    def test_use_sample_with_region_filter(self):
        """Should filter sample data by region."""
        data = fetch_nemweb_data(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            use_sample=True
        )
        assert len(data) == 1
        assert data[0]["REGIONID"] == "NSW1"

    def test_raises_for_unsupported_table(self):
        """Should raise ValueError for unsupported tables."""
        with pytest.raises(ValueError) as exc_info:
            fetch_nemweb_data(table="INVALID_TABLE")
        assert "Unsupported table" in str(exc_info.value)

    def test_fetches_real_data_with_mock(self):
        """Should fetch and parse real data (mocked)."""
        # Create a mock ZIP file with NEMWEB multi-record CSV format
        csv_content = """C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2024/01/01
I,DISPATCH,REGIONSUM,4,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND
D,DISPATCH,REGIONSUM,4,"2024/01/01 00:05:00",1,NSW1,1,0,7500.5
D,DISPATCH,REGIONSUM,4,"2024/01/01 00:05:00",1,VIC1,1,0,5200.3
C,END OF REPORT"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("PUBLIC_DISPATCHIS.CSV", csv_content)
        zip_data = zip_buffer.getvalue()

        mock_response = MagicMock()
        mock_response.read.return_value = zip_data
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            data = fetch_nemweb_data(
                table="DISPATCHREGIONSUM",
                start_date="2024-01-01",
                end_date="2024-01-01"
            )

        assert len(data) == 2
        assert data[0]["REGIONID"] == "NSW1"
        assert data[0]["TOTALDEMAND"] == "7500.5"

    def test_handles_404_gracefully(self):
        """Should handle 404 errors and continue."""
        error = HTTPError("http://example.com", 404, "Not Found", {}, None)

        with patch("nemweb_utils.fetch_with_retry", side_effect=error):
            # Should not raise, just return empty
            data = fetch_nemweb_data(
                table="DISPATCHREGIONSUM",
                start_date="2024-01-01",
                end_date="2024-01-01"
            )
            assert data == []


class TestTableToFolderMapping:
    """Tests for TABLE_TO_FOLDER constant."""

    def test_dispatchregionsum_maps_to_dispatchis_reports(self):
        """DISPATCHREGIONSUM should map to DispatchIS_Reports folder."""
        assert TABLE_TO_FOLDER["DISPATCHREGIONSUM"] == "DispatchIS_Reports"

    def test_dispatchprice_maps_to_dispatchis_reports(self):
        """DISPATCHPRICE should map to DispatchIS_Reports folder."""
        assert TABLE_TO_FOLDER["DISPATCHPRICE"] == "DispatchIS_Reports"

    def test_all_mappings_are_strings(self):
        """All folder mappings should be strings."""
        for table, folder in TABLE_TO_FOLDER.items():
            assert isinstance(table, str)
            assert isinstance(folder, str)


class TestSchemas:
    """Tests for the consolidated SCHEMAS dict."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_schemas_contains_all_tables(self, table):
        """SCHEMAS should contain all 8 supported tables."""
        assert table in SCHEMAS

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_schemas_have_record_type(self, table):
        """Each schema entry should have a record_type."""
        assert "record_type" in SCHEMAS[table]

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_schemas_have_fields(self, table):
        """Each schema entry should have a non-empty fields list."""
        assert "fields" in SCHEMAS[table]
        assert len(SCHEMAS[table]["fields"]) > 0

    def test_rooftop_pv_has_rooftop_actual_record_type(self):
        """ROOFTOP_PV_ACTUAL should parse MMS multi-record rows."""
        assert SCHEMAS["ROOFTOP_PV_ACTUAL"]["record_type"] == "ROOFTOP,ACTUAL"

    def test_schemas_field_types(self):
        """Schema fields should be (name, SparkType) tuples with valid types."""
        valid_types = (TimestampType, StringType, DoubleType)
        for table, config in SCHEMAS.items():
            for name, dtype in config["fields"]:
                assert isinstance(name, str), f"{table}: field name should be str"
                assert isinstance(dtype, valid_types), f"Invalid type for {table}.{name}"


class TestGetTableSchema:
    """Tests for the get_table_schema helper function."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_returns_struct_type(self, table):
        """get_table_schema should return a StructType for all tables."""
        schema = get_table_schema(table)
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

    def test_dispatchregionsum_fields(self):
        """DISPATCHREGIONSUM schema should have expected fields."""
        schema = get_table_schema("DISPATCHREGIONSUM")
        field_names = [f.name for f in schema.fields]
        assert "SETTLEMENTDATE" in field_names
        assert "REGIONID" in field_names
        assert "TOTALDEMAND" in field_names

    def test_settlementdate_is_timestamp_type(self):
        """get_table_schema should use TimestampType for SETTLEMENTDATE."""
        schema = get_table_schema("DISPATCHREGIONSUM")
        ts_field = next(f for f in schema.fields if f.name == "SETTLEMENTDATE")
        assert isinstance(ts_field.dataType, TimestampType)

    def test_raises_for_unknown_table(self):
        """get_table_schema should raise ValueError for unknown tables."""
        with pytest.raises(ValueError, match="Unknown table"):
            get_table_schema("NONEXISTENT_TABLE")


class TestExtractRowsFromZip:
    """Tests for the extract_rows_from_zip utility function."""

    def test_standard_csv_in_zip(self):
        """Should extract rows from a standard ZIP containing CSVs."""
        csv_content = b"""C,test
I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,NSW1
D,DISPATCH,REGIONSUM,1,"2024/01/01 12:05:00",1,VIC1
C,END
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("test.CSV", csv_content)
        zip_buffer.seek(0)

        rows = extract_rows_from_zip(zip_buffer, "DISPATCH,REGIONSUM")
        assert len(rows) == 2
        assert rows[0]["REGIONID"] == "NSW1"
        assert rows[1]["REGIONID"] == "VIC1"

    def test_nested_zip(self):
        """Should handle nested ZIPs (ARCHIVE format)."""
        csv_content = b"""C,test
I,DISPATCH,PRICE,1,SETTLEMENTDATE,REGIONID,RRP
D,DISPATCH,PRICE,1,"2024/01/01 12:05:00",NSW1,85.5
C,END
"""
        # Create inner ZIP
        inner_zip_buffer = io.BytesIO()
        with zipfile.ZipFile(inner_zip_buffer, "w") as inner_zf:
            inner_zf.writestr("inner.CSV", csv_content)

        # Create outer ZIP containing the inner ZIP
        outer_zip_buffer = io.BytesIO()
        with zipfile.ZipFile(outer_zip_buffer, "w") as outer_zf:
            outer_zf.writestr("inner.zip", inner_zip_buffer.getvalue())
        outer_zip_buffer.seek(0)

        rows = extract_rows_from_zip(outer_zip_buffer, "DISPATCH,PRICE")
        assert len(rows) == 1
        assert rows[0]["RRP"] == "85.5"

    def test_no_record_type_uses_dictreader(self):
        """Should use csv.DictReader when record_type is None."""
        csv_content = b"""INTERVAL_DATETIME,REGIONID,POWER,QI,TYPE,LASTCHANGED
2024/01/01 12:05:00,NSW1,1500.0,0.9,MEASUREMENT,2024/01/01 12:06:00
"""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("rooftop.csv", csv_content)
        zip_buffer.seek(0)

        rows = extract_rows_from_zip(zip_buffer, record_type=None)
        assert len(rows) == 1
        assert rows[0]["REGIONID"] == "NSW1"
        assert rows[0]["POWER"] == "1500.0"

    def test_empty_zip(self):
        """Should return empty list for ZIP with no CSV files."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        zip_buffer.seek(0)

        rows = extract_rows_from_zip(zip_buffer, "DISPATCH,REGIONSUM")
        assert rows == []


class TestListCurrentFiles:
    """Tests for the list_current_files utility function."""

    def test_standard_files(self):
        """Should find standard-pattern files in HTML listing."""
        html = """
        <html><body>
        <a href="PUBLIC_DISPATCHIS_202401011200_0000000001.zip">file1</a>
        <a href="PUBLIC_DISPATCHIS_202401011205_0000000002.zip">file2</a>
        <a href="PUBLIC_DISPATCHIS_202401011210_0000000003.zip">file3</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.read.return_value = html.encode('utf-8')
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            files = list_current_files("DispatchIS_Reports", "DISPATCHIS")

        assert len(files) == 3
        assert "PUBLIC_DISPATCHIS_202401011200_0000000001.zip" in files

    def test_legacy_suffix_files(self):
        """Should find legacy-pattern files with _LEGACY suffix."""
        html = """
        <html><body>
        <a href="PUBLIC_DISPATCH_202401011200_20240101120015_LEGACY.zip">file1</a>
        <a href="PUBLIC_DISPATCH_202401011205_20240101120515_LEGACY.zip">file2</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.read.return_value = html.encode('utf-8')
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            files = list_current_files("Dispatch_Reports", "DISPATCH", "_LEGACY")

        assert len(files) == 2
        assert "PUBLIC_DISPATCH_202401011200_20240101120015_LEGACY.zip" in files

    def test_deduplicates_files(self):
        """Should deduplicate files that appear multiple times in HTML."""
        html = """
        <html><body>
        <a href="PUBLIC_DISPATCHIS_202401011200_0000000001.zip">file1</a>
        <a href="PUBLIC_DISPATCHIS_202401011200_0000000001.zip">file1 again</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.read.return_value = html.encode('utf-8')
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            files = list_current_files("DispatchIS_Reports", "DISPATCHIS")

        assert len(files) == 1

    def test_returns_empty_on_http_error(self):
        """Should return empty list on HTTP error."""
        with patch("nemweb_utils.urlopen", side_effect=URLError("connection refused")):
            files = list_current_files("DispatchIS_Reports", "DISPATCHIS")

        assert files == []

    def test_returns_sorted(self):
        """Should return sorted filenames."""
        html = """
        <html><body>
        <a href="PUBLIC_DISPATCHIS_202401011210_0000000003.zip">file3</a>
        <a href="PUBLIC_DISPATCHIS_202401011200_0000000001.zip">file1</a>
        <a href="PUBLIC_DISPATCHIS_202401011205_0000000002.zip">file2</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.read.return_value = html.encode('utf-8')
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_utils.urlopen", return_value=mock_response):
            files = list_current_files("DispatchIS_Reports", "DISPATCHIS")

        assert files == sorted(files)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
