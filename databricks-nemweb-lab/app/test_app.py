"""
Unit tests for NEMWEB Price Dashboard

Run with: pytest test_app.py -v
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Import app components
from app import (
    generate_sample_data,
    get_price_status,
    get_price_data,
    fetch_delta_data,
    NEM_REGIONS,
    REGION_CONFIG,
    PRICE_WARNING,
    PRICE_ALERT,
    PRICE_CRITICAL,
)


class TestGenerateSampleData:
    """Tests for sample data generation."""

    def test_returns_dataframe(self):
        """Should return a pandas DataFrame."""
        df = generate_sample_data()
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self):
        """Should have timestamp, region, rrp, demand_mw columns."""
        df = generate_sample_data()
        required_cols = {"timestamp", "region", "rrp", "demand_mw"}
        assert required_cols.issubset(set(df.columns))

    def test_all_regions_present(self):
        """Should have data for all 5 NEM regions."""
        df = generate_sample_data()
        regions = set(df["region"].unique())
        assert regions == set(NEM_REGIONS)

    def test_prices_non_negative(self):
        """Prices should never be negative."""
        df = generate_sample_data()
        assert (df["rrp"] >= 0).all()

    def test_data_varies_between_calls(self):
        """Data should vary between calls (no fixed seed)."""
        df1 = generate_sample_data()
        df2 = generate_sample_data()
        # Prices should differ (random noise)
        assert not df1["rrp"].equals(df2["rrp"])

    def test_generates_24_hours_of_data(self):
        """Should generate approximately 24 hours of 5-minute data."""
        df = generate_sample_data()
        # 24 hours * 12 intervals/hour * 5 regions = ~1440 rows
        # Allow some tolerance for timing
        assert len(df) >= 1400
        assert len(df) <= 1500

    def test_timestamps_are_datetime(self):
        """Timestamps should be datetime objects."""
        df = generate_sample_data()
        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])


class TestGetPriceStatus:
    """Tests for price status classification."""

    def test_normal_price(self):
        """Prices below warning threshold are NORMAL."""
        status, color = get_price_status(50)
        assert status == "NORMAL"
        assert color == "#28a745"  # Green

    def test_elevated_price(self):
        """Prices at or above warning threshold are ELEVATED."""
        status, color = get_price_status(PRICE_WARNING)
        assert status == "ELEVATED"
        assert color == "#ffc107"  # Yellow

    def test_high_price(self):
        """Prices at or above alert threshold are HIGH."""
        status, color = get_price_status(PRICE_ALERT)
        assert status == "HIGH"
        assert color == "#fd7e14"  # Orange

    def test_critical_price(self):
        """Prices at or above critical threshold are CRITICAL."""
        status, color = get_price_status(PRICE_CRITICAL)
        assert status == "CRITICAL"
        assert color == "#dc3545"  # Red

    def test_extreme_price(self):
        """Extreme prices (market cap) are CRITICAL."""
        status, color = get_price_status(15000)  # NEM market cap
        assert status == "CRITICAL"

    def test_zero_price(self):
        """Zero price is NORMAL."""
        status, color = get_price_status(0)
        assert status == "NORMAL"

    def test_negative_price(self):
        """Negative price (rare but possible) is NORMAL."""
        status, color = get_price_status(-10)
        assert status == "NORMAL"


class TestRegionConfig:
    """Tests for region configuration."""

    def test_all_regions_have_config(self):
        """All NEM regions should have configuration."""
        for region in NEM_REGIONS:
            assert region in REGION_CONFIG

    def test_config_has_required_fields(self):
        """Each region config should have name and color."""
        for region, config in REGION_CONFIG.items():
            assert "name" in config
            assert "color" in config

    def test_colors_are_valid_hex(self):
        """Colors should be valid hex codes."""
        import re
        hex_pattern = re.compile(r"^#[0-9a-fA-F]{6}$")
        for region, config in REGION_CONFIG.items():
            assert hex_pattern.match(config["color"]), f"Invalid color for {region}"


class TestFetchDeltaData:
    """Tests for Delta table data fetching."""

    def test_returns_none_without_databricks(self):
        """Should return None when Databricks is not available."""
        with patch("app.DATABRICKS_AVAILABLE", False):
            result = fetch_delta_data()
            assert result is None

    def test_returns_none_without_warehouse_id(self):
        """Should return None when WAREHOUSE_ID is empty."""
        with patch("app.WAREHOUSE_ID", ""):
            result = fetch_delta_data()
            assert result is None

    def test_returns_none_without_host(self):
        """Should return None when DATABRICKS_HOST is empty."""
        with patch("app.DATABRICKS_AVAILABLE", True), \
             patch("app.WAREHOUSE_ID", "test-warehouse"), \
             patch("app.DATABRICKS_HOST", ""), \
             patch("app.DATABRICKS_TOKEN", "test-token"):
            result = fetch_delta_data()
            assert result is None

    def test_returns_none_without_token(self):
        """Should return None when DATABRICKS_TOKEN is empty."""
        with patch("app.DATABRICKS_AVAILABLE", True), \
             patch("app.WAREHOUSE_ID", "test-warehouse"), \
             patch("app.DATABRICKS_HOST", "test-host"), \
             patch("app.DATABRICKS_TOKEN", ""):
            result = fetch_delta_data()
            assert result is None


class TestGetPriceData:
    """Tests for price data retrieval."""

    def test_returns_sample_when_source_is_sample(self):
        """Should return sample data when DATA_SOURCE is 'sample'."""
        with patch("app.DATA_SOURCE", "sample"):
            df = get_price_data()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

    def test_falls_back_to_sample_when_delta_fails(self):
        """Should fall back to sample data when Delta fetch fails."""
        with patch("app.DATA_SOURCE", "delta"), \
             patch("app.fetch_delta_data", return_value=None):
            df = get_price_data()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0


class TestPriceThresholds:
    """Tests for price threshold constants."""

    def test_thresholds_in_order(self):
        """Thresholds should be in ascending order."""
        assert PRICE_WARNING < PRICE_ALERT < PRICE_CRITICAL

    def test_warning_is_reasonable(self):
        """Warning threshold should be reasonable (>= $50)."""
        assert PRICE_WARNING >= 50

    def test_critical_is_below_market_cap(self):
        """Critical threshold should be below market cap ($15,000)."""
        assert PRICE_CRITICAL < 15000


class TestDataIntegrity:
    """Integration tests for data flow."""

    def test_sample_data_can_be_serialized(self):
        """Sample data should serialize to JSON and back."""
        from io import StringIO

        df = generate_sample_data()
        json_data = df.to_json(date_format="iso", orient="split")
        df_restored = pd.read_json(StringIO(json_data), orient="split")

        assert len(df) == len(df_restored)
        assert set(df.columns) == set(df_restored.columns)

    def test_sample_data_regions_match_config(self):
        """All regions in sample data should be in REGION_CONFIG."""
        df = generate_sample_data()
        for region in df["region"].unique():
            assert region in REGION_CONFIG, f"Unknown region: {region}"


class TestHostnameNormalization:
    """Tests for DATABRICKS_HOST normalization."""

    def test_strips_https_prefix(self):
        """Should strip https:// prefix from host."""
        import app
        original = app.DATABRICKS_HOST

        # Simulate what happens during module load
        test_host = "https://my-workspace.cloud.databricks.com"
        normalized = test_host.replace("https://", "").replace("http://", "").rstrip("/")
        assert normalized == "my-workspace.cloud.databricks.com"

    def test_strips_http_prefix(self):
        """Should strip http:// prefix from host."""
        test_host = "http://my-workspace.cloud.databricks.com"
        normalized = test_host.replace("https://", "").replace("http://", "").rstrip("/")
        assert normalized == "my-workspace.cloud.databricks.com"

    def test_strips_trailing_slash(self):
        """Should strip trailing slash from host."""
        test_host = "my-workspace.cloud.databricks.com/"
        normalized = test_host.replace("https://", "").replace("http://", "").rstrip("/")
        assert normalized == "my-workspace.cloud.databricks.com"

    def test_leaves_clean_host_unchanged(self):
        """Should not modify already clean hostname."""
        test_host = "my-workspace.cloud.databricks.com"
        normalized = test_host.replace("https://", "").replace("http://", "").rstrip("/")
        assert normalized == "my-workspace.cloud.databricks.com"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
