"""Tests for the shared ML feature engineering functions.

Validates that create_load_forecast_features() and create_price_forecast_features()
produce the expected columns, handle edge cases, and maintain data integrity.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add src/ to path so ml_features can be imported without the wheel
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "src" / "prediction_pipeline" / "transformations"),
)

from ml_features import (
    LOAD_FORECAST_EXCLUDE_COLS,
    PRICE_FORECAST_EXCLUDE_COLS,
    create_load_forecast_features,
    create_price_forecast_features,
)


# ---------------------------------------------------------------------------
# Load Forecast Features
# ---------------------------------------------------------------------------

class TestLoadForecastFeatures:
    """Tests for create_load_forecast_features()."""

    def test_output_is_dataframe(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        assert isinstance(result, pd.DataFrame)

    def test_rows_dropped_by_dropna(self, nem_dispatch_df: pd.DataFrame):
        """dropna() removes initial rows where lag/rolling features are NaN."""
        result = create_load_forecast_features(nem_dispatch_df)
        assert len(result) < len(nem_dispatch_df)
        assert len(result) > 0

    def test_time_features_present(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        for col in ["hour", "day_of_week", "month", "is_weekend"]:
            assert col in result.columns, f"Missing time feature: {col}"

    def test_cyclical_encoding_bounded(self, nem_dispatch_df: pd.DataFrame):
        """sin/cos values must be in [-1, 1]."""
        result = create_load_forecast_features(nem_dispatch_df)
        for col in ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]:
            assert result[col].min() >= -1.0 - 1e-9
            assert result[col].max() <= 1.0 + 1e-9

    def test_demand_lag_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        expected_lags = [1, 2, 3, 6, 12, 48, 288]
        for lag in expected_lags:
            assert f"demand_lag_{lag}" in result.columns

    def test_rolling_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        for col in ["demand_rolling_1h_mean", "demand_rolling_1h_std", "demand_rolling_1d_mean"]:
            assert col in result.columns

    def test_price_signal_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        assert "rrp_lag_1" in result.columns
        assert "rrp_rolling_1h_mean" in result.columns

    def test_weather_features_forward_filled(self, nem_dispatch_df: pd.DataFrame):
        """Weather columns should have no NaNs after ffill (given enough data)."""
        result = create_load_forecast_features(nem_dispatch_df)
        assert result["air_temp_c"].isna().sum() == 0
        assert result["wind_speed_kmh"].isna().sum() == 0

    def test_aemo_forecast_error_when_present(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        assert "aemo_forecast_error" in result.columns
        # Error = forecast - actual
        expected = nem_dispatch_df["demand_forecast_mw"] - nem_dispatch_df["total_demand_mw"]
        # Compare on the overlapping index (after dropna)
        common_idx = result.index.intersection(expected.index)
        pd.testing.assert_series_equal(
            result.loc[common_idx, "aemo_forecast_error"],
            expected.loc[common_idx],
            check_names=False,
        )

    def test_no_weather_columns_graceful(self, nem_dispatch_no_weather_df: pd.DataFrame):
        """Should work without weather columns (skips that block)."""
        result = create_load_forecast_features(nem_dispatch_no_weather_df)
        assert len(result) > 0
        assert "hour" in result.columns
        assert "air_temp_c" not in result.columns

    def test_no_nans_in_output(self, nem_dispatch_df: pd.DataFrame):
        """Final output should be NaN-free (dropna at the end)."""
        result = create_load_forecast_features(nem_dispatch_df)
        assert result.isna().sum().sum() == 0

    def test_is_weekend_binary(self, nem_dispatch_df: pd.DataFrame):
        result = create_load_forecast_features(nem_dispatch_df)
        assert set(result["is_weekend"].unique()).issubset({0, 1})

    def test_exclude_cols_constant(self):
        """Ensure the exclude list matches what workshops expect."""
        assert "total_demand_mw" in LOAD_FORECAST_EXCLUDE_COLS
        assert "region_id" in LOAD_FORECAST_EXCLUDE_COLS


# ---------------------------------------------------------------------------
# Price Forecast Features
# ---------------------------------------------------------------------------

class TestPriceForecastFeatures:
    """Tests for create_price_forecast_features()."""

    def test_output_is_dataframe(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        assert isinstance(result, pd.DataFrame)

    def test_supply_margin_calculated(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        assert "supply_margin_mw" in result.columns
        # Margin = available_gen - demand
        common_idx = result.index
        expected = (
            nem_dispatch_df.loc[common_idx, "available_generation_mw"]
            - nem_dispatch_df.loc[common_idx, "total_demand_mw"]
        )
        pd.testing.assert_series_equal(
            result["supply_margin_mw"], expected, check_names=False
        )

    def test_price_lag_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        for lag in [1, 2, 3, 6, 12]:
            assert f"rrp_lag_{lag}" in result.columns

    def test_price_rolling_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        for col in ["rrp_rolling_1h_mean", "rrp_rolling_1h_std", "rrp_rolling_1h_max"]:
            assert col in result.columns

    def test_demand_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        for col in ["demand_lag_1", "demand_lag_12", "demand_rolling_1h_mean"]:
            assert col in result.columns

    def test_supply_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        for col in ["gen_lag_1", "margin_lag_1", "margin_rolling_1h_mean", "margin_rolling_1h_min"]:
            assert col in result.columns

    def test_interconnector_features(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        assert "interchange_lag_1" in result.columns
        assert "interchange_rolling_1h" in result.columns

    def test_demand_forecast_error(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        assert "demand_forecast_error" in result.columns

    def test_cyclical_encoding_bounded(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        for col in ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]:
            assert result[col].min() >= -1.0 - 1e-9
            assert result[col].max() <= 1.0 + 1e-9

    def test_no_nans_in_output(self, nem_dispatch_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_df)
        assert result.isna().sum().sum() == 0

    def test_no_weather_columns_graceful(self, nem_dispatch_no_weather_df: pd.DataFrame):
        result = create_price_forecast_features(nem_dispatch_no_weather_df)
        assert len(result) > 0
        assert "supply_margin_mw" in result.columns
        assert "air_temp_c" not in result.columns

    def test_exclude_cols_constant(self):
        assert "rrp" in PRICE_FORECAST_EXCLUDE_COLS
        assert "region_id" in PRICE_FORECAST_EXCLUDE_COLS


# ---------------------------------------------------------------------------
# Cross-cutting
# ---------------------------------------------------------------------------

class TestFeatureConsistency:
    """Both feature functions should produce consistent, non-overlapping outputs."""

    def test_both_produce_time_features(self, nem_dispatch_df: pd.DataFrame):
        load = create_load_forecast_features(nem_dispatch_df)
        price = create_price_forecast_features(nem_dispatch_df)
        for col in ["hour", "day_of_week", "is_weekend"]:
            assert col in load.columns
            assert col in price.columns

    def test_input_not_mutated(self, nem_dispatch_df: pd.DataFrame):
        """Feature functions should not modify the input DataFrame."""
        original_cols = set(nem_dispatch_df.columns)
        original_len = len(nem_dispatch_df)

        create_load_forecast_features(nem_dispatch_df)
        create_price_forecast_features(nem_dispatch_df)

        assert set(nem_dispatch_df.columns) == original_cols
        assert len(nem_dispatch_df) == original_len

    def test_sufficient_rows_with_48h_data(self, nem_dispatch_df: pd.DataFrame):
        """48h of 5-min data (576 rows) should produce >200 usable rows."""
        load = create_load_forecast_features(nem_dispatch_df)
        price = create_price_forecast_features(nem_dispatch_df)
        assert len(load) > 200, f"Only {len(load)} rows from load features"
        assert len(price) > 200, f"Only {len(price)} rows from price features"
