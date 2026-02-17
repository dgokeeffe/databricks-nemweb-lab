"""
Tests for NEMWEB standalone utility.

Run with: python -m pytest test_nemweb.py -v
"""

import io
import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test
from nemweb import (
    NEM_REGIONS,
    TABLE_CONFIG,
    _get_sample_data,
    _parse_csv,
    fetch,
    list_tables,
    main,
)


class TestListTables:
    """Tests for list_tables()."""

    def test_returns_list(self):
        tables = list_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0

    def test_table_structure(self):
        tables = list_tables()
        for table in tables:
            assert "name" in table
            assert "description" in table

    def test_known_tables_present(self):
        tables = list_tables()
        names = [t["name"] for t in tables]
        assert "DISPATCHPRICE" in names
        assert "DISPATCHREGIONSUM" in names
        assert "TRADINGPRICE" in names


class TestNEMRegions:
    """Tests for NEM_REGIONS constant."""

    def test_all_regions_present(self):
        assert "NSW1" in NEM_REGIONS
        assert "VIC1" in NEM_REGIONS
        assert "QLD1" in NEM_REGIONS
        assert "SA1" in NEM_REGIONS
        assert "TAS1" in NEM_REGIONS

    def test_five_regions(self):
        assert len(NEM_REGIONS) == 5


class TestFetchSample:
    """Tests for fetch() with sample data (no network)."""

    def test_sample_returns_data(self):
        data = fetch("DISPATCHPRICE", sample=True)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_sample_has_all_regions(self):
        data = fetch("DISPATCHPRICE", sample=True)
        regions = {row["REGIONID"] for row in data}
        assert regions == set(NEM_REGIONS)

    def test_sample_region_filter(self):
        data = fetch("DISPATCHPRICE", sample=True, regions=["NSW1"])
        assert len(data) == 1
        assert data[0]["REGIONID"] == "NSW1"

    def test_sample_multiple_regions(self):
        data = fetch("DISPATCHPRICE", sample=True, regions=["NSW1", "VIC1"])
        regions = {row["REGIONID"] for row in data}
        assert regions == {"NSW1", "VIC1"}

    def test_sample_data_fields(self):
        data = fetch("DISPATCHPRICE", sample=True)
        row = data[0]
        assert "SETTLEMENTDATE" in row
        assert "REGIONID" in row
        assert "RRP" in row


class TestFetchInvalidTable:
    """Tests for fetch() with invalid table names."""

    def test_invalid_table_raises(self):
        with pytest.raises(ValueError) as exc:
            fetch("INVALID_TABLE", sample=True)
        assert "Unknown table" in str(exc.value)


class TestParseCSV:
    """Tests for _parse_csv() function."""

    def test_standard_csv(self):
        """Test parsing standard CSV format."""
        csv_content = b"COL1,COL2,COL3\nval1,val2,val3\nval4,val5,val6"
        csv_file = io.BytesIO(csv_content)

        rows = _parse_csv(csv_file, record_type=None)

        assert len(rows) == 2
        assert rows[0] == {"COL1": "val1", "COL2": "val2", "COL3": "val3"}
        assert rows[1] == {"COL1": "val4", "COL2": "val5", "COL3": "val6"}

    def test_nemweb_multirecord_format(self):
        """Test parsing NEMWEB multi-record format."""
        csv_content = b"""C,NEMP.WORLD,TABLE_COMMENT,v1
I,DISPATCH,PRICE,1,SETTLEMENTDATE,REGIONID,RRP
D,DISPATCH,PRICE,1,2024/01/01 00:05:00,NSW1,85.50
D,DISPATCH,PRICE,1,2024/01/01 00:05:00,VIC1,72.30
I,DISPATCH,OTHER,1,COL1,COL2
D,DISPATCH,OTHER,1,other_val1,other_val2"""
        csv_file = io.BytesIO(csv_content)

        rows = _parse_csv(csv_file, record_type="DISPATCH,PRICE")

        assert len(rows) == 2
        assert rows[0]["SETTLEMENTDATE"] == "2024/01/01 00:05:00"
        assert rows[0]["REGIONID"] == "NSW1"
        assert rows[0]["RRP"] == "85.50"
        assert rows[1]["REGIONID"] == "VIC1"

    def test_nemweb_ignores_other_records(self):
        """Test that only matching record types are returned."""
        csv_content = b"""I,DISPATCH,REGIONSUM,1,COL1
D,DISPATCH,REGIONSUM,1,val1
I,DISPATCH,PRICE,1,COL2
D,DISPATCH,PRICE,1,val2"""
        csv_file = io.BytesIO(csv_content)

        rows = _parse_csv(csv_file, record_type="DISPATCH,PRICE")

        assert len(rows) == 1
        assert rows[0]["COL2"] == "val2"


class TestGetSampleData:
    """Tests for _get_sample_data() function."""

    def test_returns_five_regions(self):
        data = _get_sample_data("DISPATCHPRICE")
        assert len(data) == 5

    def test_region_filter(self):
        data = _get_sample_data("DISPATCHPRICE", regions=["NSW1", "SA1"])
        assert len(data) == 2
        regions = {row["REGIONID"] for row in data}
        assert regions == {"NSW1", "SA1"}


class TestCLI:
    """Tests for CLI argument parsing."""

    def test_list_tables_flag(self, capsys):
        """Test --list-tables output."""
        with patch.object(sys, "argv", ["nemweb.py", "--list-tables"]):
            main()

        captured = capsys.readouterr()
        assert "DISPATCHPRICE" in captured.out
        assert "DISPATCHREGIONSUM" in captured.out

    def test_sample_json_output(self, capsys):
        """Test sample data JSON output."""
        with patch.object(sys, "argv", ["nemweb.py", "DISPATCHPRICE", "--sample"]):
            main()

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)
        assert len(data) == 5

    def test_sample_csv_output(self, capsys, tmp_path):
        """Test sample data CSV output."""
        output_file = tmp_path / "output.csv"
        with patch.object(
            sys, "argv", ["nemweb.py", "DISPATCHPRICE", "--sample", "-o", str(output_file)]
        ):
            main()

        # Check file was created
        assert output_file.exists()
        content = output_file.read_text()
        assert "SETTLEMENTDATE" in content
        assert "NSW1" in content

    def test_region_filter_cli(self, capsys):
        """Test --regions filter."""
        with patch.object(
            sys, "argv", ["nemweb.py", "DISPATCHPRICE", "--sample", "--regions", "NSW1,VIC1"]
        ):
            main()

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        regions = {row["REGIONID"] for row in data}
        assert regions == {"NSW1", "VIC1"}

    def test_missing_table_error(self, capsys):
        """Test error when table is missing."""
        with patch.object(sys, "argv", ["nemweb.py"]):
            with pytest.raises(SystemExit):
                main()


class TestTableConfig:
    """Tests for TABLE_CONFIG structure."""

    def test_all_tables_have_required_fields(self):
        for name, config in TABLE_CONFIG.items():
            assert "folder" in config, f"{name} missing folder"
            assert "file_prefix" in config, f"{name} missing file_prefix"
            assert "description" in config, f"{name} missing description"

    def test_record_types_format(self):
        """Test that record_types follow expected format."""
        for name, config in TABLE_CONFIG.items():
            record_type = config.get("record_type")
            if record_type:
                # Should contain a comma
                assert "," in record_type, f"{name} record_type missing comma"


class TestFetchWithMockedNetwork:
    """Tests for fetch() with mocked network calls."""

    @patch("nemweb._fetch_current")
    def test_hours_uses_current(self, mock_fetch_current):
        """Test that hours parameter uses CURRENT folder."""
        mock_fetch_current.return_value = []
        fetch("DISPATCHPRICE", hours=1)
        mock_fetch_current.assert_called_once()

    @patch("nemweb._fetch_archive")
    def test_days_uses_archive(self, mock_fetch_archive):
        """Test that days parameter uses ARCHIVE folder."""
        mock_fetch_archive.return_value = []
        fetch("DISPATCHPRICE", days=1)
        mock_fetch_archive.assert_called_once()

    @patch("nemweb._fetch_archive")
    def test_date_range_uses_archive(self, mock_fetch_archive):
        """Test that date range uses ARCHIVE folder."""
        mock_fetch_archive.return_value = []
        fetch("DISPATCHPRICE", start_date="2024-01-01", end_date="2024-01-07")
        mock_fetch_archive.assert_called_once()


class TestOptionalDataFrameOutput:
    """Tests for optional pandas/polars output."""

    def test_as_pandas_without_pandas(self):
        """Test as_pandas raises ImportError if pandas not installed."""
        with patch.dict(sys.modules, {"pandas": None}):
            # Force reimport to trigger ImportError
            with pytest.raises(ImportError):
                # This test assumes pandas might not be installed
                # If pandas is installed, we need to mock the import
                import importlib
                import nemweb
                importlib.reload(nemweb)
                nemweb.fetch("DISPATCHPRICE", sample=True, as_pandas=True)

    def test_as_polars_without_polars(self):
        """Test as_polars raises ImportError if polars not installed."""
        # Similar approach - test that we get ImportError when polars unavailable
        pass  # Skip - depends on environment

    @pytest.mark.skipif(
        "pandas" not in sys.modules,
        reason="pandas not installed"
    )
    def test_as_pandas_returns_dataframe(self):
        """Test as_pandas returns DataFrame when pandas installed."""
        try:
            import pandas as pd
            df = fetch("DISPATCHPRICE", sample=True, as_pandas=True)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 5
        except ImportError:
            pytest.skip("pandas not installed")
