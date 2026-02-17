"""
Integration tests for NEMWEB data fetching.

These tests hit the real NEMWEB API to verify the fetch logic works.
Run with: uv run pytest src/tests/test_nemweb_integration.py -v
"""

import pytest
from datetime import datetime, timedelta

# Use dates from last week for testing (guaranteed to be in archive)
TEST_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
TEST_DATE_DT = datetime.now() - timedelta(days=7)


class TestNemwebFetch:
    """Integration tests for NEMWEB HTTP fetching."""

    def test_build_nemweb_url_archive(self):
        """Test URL building for archive data (> 7 days old)."""
        from nemweb_utils import _build_nemweb_url

        # Use a date that's definitely in the archive (14 days ago)
        old_date = datetime.now() - timedelta(days=14)
        url = _build_nemweb_url("DispatchIS_Reports", "DISPATCHIS", old_date)

        assert "ARCHIVE" in url
        assert "PUBLIC_DISPATCHIS_" in url
        assert old_date.strftime("%Y%m%d") in url

    def test_build_nemweb_url_always_uses_archive(self):
        """Test URL building always uses ARCHIVE for daily files.

        CURRENT folder only has 5-minute interval files, not daily aggregates.
        Use fetch_nemweb_current() for CURRENT folder access.
        """
        from nemweb_utils import _build_nemweb_url

        # Even recent dates use ARCHIVE for daily consolidated files
        yesterday = datetime.now() - timedelta(days=1)
        url = _build_nemweb_url("Dispatch_SCADA", "DISPATCHSCADA", yesterday)

        assert "ARCHIVE" in url
        assert "PUBLIC_DISPATCHSCADA_" in url

    def test_fetch_and_extract_zip_real(self):
        """Test fetching real data from NEMWEB archive."""
        from nemweb_utils import _fetch_and_extract_zip

        # Fetch archive file from 7 days ago
        date_str = TEST_DATE_DT.strftime("%Y%m%d")
        url = f"https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/PUBLIC_DISPATCHIS_{date_str}.zip"

        rows = _fetch_and_extract_zip(url, record_type="DISPATCH,REGIONSUM")

        # Should have data
        assert len(rows) > 0, "Expected rows from NEMWEB"
        print(f"Fetched {len(rows)} rows")

        # Check expected columns exist
        first_row = rows[0]
        print(f"Columns: {list(first_row.keys())[:10]}...")

        # Check we got REGIONSUM data
        assert "REGIONID" in first_row, "Expected REGIONID column"
        assert "TOTALDEMAND" in first_row, "Expected TOTALDEMAND column"

    def test_fetch_nemweb_data_single_day(self):
        """Test fetching a single day of DISPATCHREGIONSUM data."""
        from nemweb_utils import fetch_nemweb_data

        # Fetch one day from last week
        rows = fetch_nemweb_data(
            table="DISPATCHREGIONSUM",
            region="NSW1",
            start_date=TEST_DATE,
            end_date=TEST_DATE
        )

        print(f"Fetched {len(rows)} rows for NSW1 on {TEST_DATE}")

        if len(rows) > 0:
            # Check expected columns
            first_row = rows[0]
            print(f"Sample row: {first_row}")

            expected_columns = ["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"]
            for col in expected_columns:
                assert col in first_row, f"Expected column {col} in data"

            # Verify region filter worked
            for row in rows:
                assert row.get("REGIONID") == "NSW1", f"Expected NSW1, got {row.get('REGIONID')}"

            # Should have ~288 rows per day (5-min intervals)
            assert len(rows) > 100, f"Expected ~288 rows, got {len(rows)}"
        else:
            pytest.skip("No data returned - check NEMWEB availability")

    def test_fetch_nemweb_data_all_regions(self):
        """Test fetching all regions for a single day."""
        from nemweb_utils import fetch_nemweb_data

        rows = fetch_nemweb_data(
            table="DISPATCHREGIONSUM",
            region=None,  # All regions
            start_date=TEST_DATE,
            end_date=TEST_DATE
        )

        print(f"Fetched {len(rows)} rows for all regions on {TEST_DATE}")

        if len(rows) > 0:
            # Check we got multiple regions
            regions = set(row.get("REGIONID") for row in rows)
            print(f"Regions found: {regions}")

            expected_regions = {"NSW1", "VIC1", "QLD1", "SA1", "TAS1"}
            assert regions == expected_regions, f"Expected {expected_regions}, got {regions}"

            # Should have ~288 * 5 = 1440 rows
            assert len(rows) > 1000, f"Expected ~1440 rows, got {len(rows)}"

    def test_table_config_mapping(self):
        """Test that TABLE_CONFIG has correct mappings."""
        from nemweb_utils import TABLE_CONFIG

        # Check DISPATCHREGIONSUM config
        assert "DISPATCHREGIONSUM" in TABLE_CONFIG
        config = TABLE_CONFIG["DISPATCHREGIONSUM"]

        assert config["folder"] == "DispatchIS_Reports"
        assert config["file_prefix"] == "DISPATCHIS"
        assert config["record_type"] == "DISPATCH,REGIONSUM"

    def test_fetch_with_retry_404(self):
        """Test that 404 errors are raised immediately (not retried)."""
        from nemweb_utils import fetch_with_retry
        from urllib.error import HTTPError

        # Non-existent file
        url = "https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_19000101.zip"

        with pytest.raises(HTTPError) as exc_info:
            fetch_with_retry(url, max_retries=3)

        assert exc_info.value.code == 404


class TestNemwebDataSource:
    """Integration tests for the custom data source (requires understanding of structure)."""

    def test_nemweb_datasource_imports(self):
        """Test that NemwebArrowDataSource can be imported."""
        from nemweb_datasource_arrow import NemwebArrowDataSource
        assert NemwebArrowDataSource is not None

    def test_nemweb_datasource_name(self):
        """Test that data source has correct name."""
        from nemweb_datasource_arrow import NemwebArrowDataSource
        assert NemwebArrowDataSource.name() == "nemweb_arrow"


if __name__ == "__main__":
    # Run a quick smoke test
    print("Running NEMWEB integration smoke test...")

    from nemweb_utils import fetch_nemweb_data, TABLE_CONFIG

    print(f"\nTABLE_CONFIG: {TABLE_CONFIG}")

    print(f"\nFetching single day of NSW1 data for {TEST_DATE}...")
    rows = fetch_nemweb_data(
        table="DISPATCHREGIONSUM",
        region="NSW1",
        start_date=TEST_DATE,
        end_date=TEST_DATE
    )

    print(f"Got {len(rows)} rows")
    if rows:
        print(f"First row REGIONID: {rows[0].get('REGIONID')}")
        print(f"First row TOTALDEMAND: {rows[0].get('TOTALDEMAND')}")
        print(f"Columns: {list(rows[0].keys())[:10]}...")
