"""
Tests for NEMWEB CSV parsing with real data fixtures.

These tests use actual NEMWEB CSV files to ensure parsing produces
exact expected output, including correct timestamp conversion.
"""

import io
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql.types import TimestampType

# Import the functions we're testing
from nemweb_utils import (
    _parse_nemweb_csv_file,
    parse_nemweb_csv,
    get_nemweb_schema,
    _convert_value,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestParseNemwebCsvFileWithFixture:
    """Tests using real NEMWEB CSV fixture."""

    @pytest.fixture
    def sample_csv_content(self):
        """Load the sample DISPATCHIS CSV fixture."""
        fixture_path = FIXTURES_DIR / "sample_dispatchis.csv"
        return fixture_path.read_text()

    def test_parses_regionsum_records(self, sample_csv_content):
        """Should extract DISPATCH,REGIONSUM records from multi-record CSV."""
        csv_file = io.BytesIO(sample_csv_content.encode("utf-8"))
        rows = _parse_nemweb_csv_file(csv_file, record_type="DISPATCH,REGIONSUM")

        assert len(rows) > 0, "Should find REGIONSUM records"

        # Check that we got the expected columns
        first_row = rows[0]
        assert "SETTLEMENTDATE" in first_row
        assert "REGIONID" in first_row
        assert "TOTALDEMAND" in first_row

    def test_settlementdate_format(self, sample_csv_content):
        """SETTLEMENTDATE should be in expected format (quotes stripped)."""
        csv_file = io.BytesIO(sample_csv_content.encode("utf-8"))
        rows = _parse_nemweb_csv_file(csv_file, record_type="DISPATCH,REGIONSUM")

        first_row = rows[0]
        ts = first_row["SETTLEMENTDATE"]

        # Should not have quotes (they should be stripped)
        assert not ts.startswith('"'), f"Timestamp has leading quote: {repr(ts)}"
        assert not ts.endswith('"'), f"Timestamp has trailing quote: {repr(ts)}"

        # Should be in YYYY/MM/DD HH:MM:SS format
        assert "/" in ts, f"Expected slash in date: {ts}"
        assert ":" in ts, f"Expected colon in time: {ts}"

    def test_all_five_regions_present(self, sample_csv_content):
        """Should have data for all 5 NEM regions."""
        csv_file = io.BytesIO(sample_csv_content.encode("utf-8"))
        rows = _parse_nemweb_csv_file(csv_file, record_type="DISPATCH,REGIONSUM")

        regions = {row["REGIONID"] for row in rows}
        expected_regions = {"NSW1", "VIC1", "QLD1", "SA1", "TAS1"}

        assert expected_regions == regions, f"Got regions: {regions}"


class TestParseNemwebCsvConversion:
    """Tests for converting parsed CSV to Spark-compatible tuples."""

    @pytest.fixture
    def sample_rows(self):
        """Load and parse sample REGIONSUM rows."""
        fixture_path = FIXTURES_DIR / "sample_dispatchis.csv"
        content = fixture_path.read_text()
        csv_file = io.BytesIO(content.encode("utf-8"))
        return _parse_nemweb_csv_file(csv_file, record_type="DISPATCH,REGIONSUM")

    def test_converts_to_tuples(self, sample_rows):
        """Should convert dict rows to tuples matching schema."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv(sample_rows, schema))

        assert len(tuples) > 0
        assert len(tuples) == len(sample_rows)

        # Each tuple should have same number of fields as schema
        for tup in tuples:
            assert len(tup) == len(schema.fields)

    def test_settlementdate_is_datetime(self, sample_rows):
        """SETTLEMENTDATE must be converted to datetime object."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv(sample_rows, schema))

        for i, tup in enumerate(tuples):
            ts_value = tup[0]  # SETTLEMENTDATE is first column

            # Must be datetime or None, never a string
            assert ts_value is None or isinstance(ts_value, datetime), \
                f"Row {i}: SETTLEMENTDATE = {repr(ts_value)} (type: {type(ts_value).__name__})"

    def test_all_timestamp_fields_are_datetime(self, sample_rows):
        """All TimestampType fields must be datetime objects or None."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv(sample_rows, schema))

        for i, tup in enumerate(tuples):
            for j, (field, value) in enumerate(zip(schema.fields, tup)):
                if isinstance(field.dataType, TimestampType):
                    assert value is None or isinstance(value, datetime), \
                        f"Row {i}, {field.name}: {repr(value)} (type: {type(value).__name__})"

    def test_double_fields_are_float(self, sample_rows):
        """DoubleType fields should be converted to float."""
        from pyspark.sql.types import DoubleType

        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv(sample_rows, schema))

        for tup in tuples:
            for field, value in zip(schema.fields, tup):
                if isinstance(field.dataType, DoubleType):
                    assert value is None or isinstance(value, float), \
                        f"{field.name}: {repr(value)} should be float"


class TestConvertValueTimestamp:
    """Specific tests for timestamp conversion."""

    def test_nemweb_slash_format_with_quotes(self):
        """Should parse NEMWEB format: "2025/12/27 00:05:00" (with quotes stripped)."""
        # This is what we get AFTER stripping quotes in _parse_nemweb_csv_file
        result = _convert_value("2025/12/27 00:05:00", TimestampType())

        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 27
        assert result.hour == 0
        assert result.minute == 5
        assert result.second == 0

    def test_nemweb_dash_format(self):
        """Should parse dash format: 2024-01-01 00:05:00."""
        result = _convert_value("2024-01-01 00:05:00", TimestampType())

        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1

    def test_nemweb_no_seconds_format(self):
        """Should parse format without seconds: 2025/12/27 00:05."""
        result = _convert_value("2025/12/27 00:05", TimestampType())

        assert isinstance(result, datetime)
        assert result.second == 0

    def test_invalid_timestamp_returns_none(self):
        """Invalid timestamp should return None, not raise or return string."""
        result = _convert_value("not-a-date", TimestampType())

        assert result is None, f"Expected None, got {repr(result)}"

    def test_empty_string_returns_none(self):
        """Empty string should return None."""
        result = _convert_value("", TimestampType())
        assert result is None

    def test_none_returns_none(self):
        """None input should return None."""
        result = _convert_value(None, TimestampType())
        assert result is None


class TestExactParsedValues:
    """Tests for exact parsed values from fixture."""

    @pytest.fixture
    def first_nsw_row(self):
        """Get first NSW1 row from fixture."""
        fixture_path = FIXTURES_DIR / "sample_dispatchis.csv"
        content = fixture_path.read_text()
        csv_file = io.BytesIO(content.encode("utf-8"))
        rows = _parse_nemweb_csv_file(csv_file, record_type="DISPATCH,REGIONSUM")

        # Find first NSW1 row
        for row in rows:
            if row.get("REGIONID") == "NSW1":
                return row
        pytest.fail("No NSW1 row found in fixture")

    def test_exact_timestamp_value(self, first_nsw_row):
        """Verify exact timestamp parsing from fixture."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv([first_nsw_row], schema))

        ts = tuples[0][0]  # SETTLEMENTDATE

        # Should be 2025/12/27 00:05:00 based on fixture
        assert ts.year == 2025
        assert ts.month == 12
        assert ts.day == 27
        assert ts.hour == 0
        assert ts.minute == 5
        assert ts.second == 0

    def test_exact_region_value(self, first_nsw_row):
        """Verify exact region ID."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv([first_nsw_row], schema))

        region = tuples[0][2]  # REGIONID
        assert region == "NSW1"

    def test_totaldemand_is_float(self, first_nsw_row):
        """TOTALDEMAND should be a float."""
        schema = get_nemweb_schema("DISPATCHREGIONSUM")
        tuples = list(parse_nemweb_csv([first_nsw_row], schema))

        # TOTALDEMAND is at index 5 in schema
        totaldemand = tuples[0][5]

        assert isinstance(totaldemand, float)
        assert totaldemand > 0  # Demand should be positive
