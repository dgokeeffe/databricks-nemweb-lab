"""Shared feature engineering functions for ML prediction pipelines.

These functions mirror the inline definitions in the workshop notebooks
(02_load_forecast_workshop.py and 03_market_modelling_workshop.py) so that
the prediction pipeline can reuse them without sys.path hacks.
"""

import numpy as np
import pandas as pd


# Columns to exclude from features (target + metadata)
LOAD_FORECAST_EXCLUDE_COLS = ["total_demand_mw", "region_id", "rop", "_processed_at"]
PRICE_FORECAST_EXCLUDE_COLS = ["rrp", "rop", "region_id", "_processed_at"]


def create_load_forecast_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for load forecasting (predicting TOTALDEMAND).

    Temperature is the primary driver of electricity demand:
    - Hot days -> air conditioning -> high demand
    - Cold days -> heating -> high demand
    - Mild days -> low demand
    """
    out = df.copy()

    # -- Time features --
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["month"] = out.index.month
    out["is_weekend"] = out.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding (captures circular nature of time)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7)

    # -- Weather features (temperature is #1 driver) --
    if "air_temp_c" in out.columns:
        out["air_temp_c"] = out["air_temp_c"].ffill()
        out["apparent_temp_c"] = out["apparent_temp_c"].ffill()
        out["rel_humidity_pct"] = out["rel_humidity_pct"].ffill()
        out["wind_speed_kmh"] = out["wind_speed_kmh"].ffill()

    # -- Demand lag features --
    # 5-min intervals: lag_1 = 5min ago, lag_12 = 1hr ago, lag_288 = 1 day ago.
    # In live demos, we may not have 1 full day yet, so adaptively generate
    # only lags that are feasible for the currently available history.
    available_lags = [lag for lag in [1, 2, 3, 6, 12, 48, 288] if len(out) > lag]
    for lag in available_lags:
        out[f"demand_lag_{lag}"] = out["total_demand_mw"].shift(lag)

    # -- Rolling statistics --
    if len(out) >= 12:
        out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()
        out["demand_rolling_1h_std"] = out["total_demand_mw"].rolling(12).std()
        out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()
    if len(out) >= 288:
        out["demand_rolling_1d_mean"] = out["total_demand_mw"].rolling(288).mean()

    # -- Price signal (price-responsive demand) --
    out["rrp_lag_1"] = out["rrp"].shift(1)

    # -- AEMO benchmark (their forecast vs actual) --
    if "demand_forecast_mw" in out.columns:
        out["aemo_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    return out.dropna()


def create_price_forecast_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for price forecasting (predicting RRP).

    The key driver of NEM spot prices is the demand-supply margin.
    When available generation barely exceeds demand, prices spike
    as expensive peaking generators are dispatched.
    """
    out = df.copy()

    # -- Demand-supply margin (THE key price driver) --
    out["supply_margin_mw"] = out["available_generation_mw"] - out["total_demand_mw"]

    # -- Demand features (demand drives price) --
    out["demand_lag_1"] = out["total_demand_mw"].shift(1)
    out["demand_lag_12"] = out["total_demand_mw"].shift(12)
    out["demand_rolling_1h_mean"] = out["total_demand_mw"].rolling(12).mean()

    # AEMO demand forecast (what the market expects)
    if "demand_forecast_mw" in out.columns:
        out["demand_forecast_error"] = out["demand_forecast_mw"] - out["total_demand_mw"]

    # -- Supply features --
    out["gen_lag_1"] = out["available_generation_mw"].shift(1)
    out["margin_lag_1"] = out["supply_margin_mw"].shift(1)
    out["margin_rolling_1h_mean"] = out["supply_margin_mw"].rolling(12).mean()
    out["margin_rolling_1h_min"] = out["supply_margin_mw"].rolling(12).min()

    # -- Interconnector features --
    out["interchange_lag_1"] = out["net_interchange_mw"].shift(1)
    out["interchange_rolling_1h"] = out["net_interchange_mw"].rolling(12).mean()

    # -- Price lag features (price momentum) --
    for lag in [1, 2, 3, 6, 12]:
        out[f"rrp_lag_{lag}"] = out["rrp"].shift(lag)
    out["rrp_rolling_1h_mean"] = out["rrp"].rolling(12).mean()
    out["rrp_rolling_1h_std"] = out["rrp"].rolling(12).std()
    out["rrp_rolling_1h_max"] = out["rrp"].rolling(12).max()

    # -- Weather features (affects renewable output) --
    if "air_temp_c" in out.columns:
        out["air_temp_c"] = out["air_temp_c"].ffill()
        out["wind_speed_kmh"] = out["wind_speed_kmh"].ffill()

    # -- Time features --
    out["hour"] = out.index.hour
    out["day_of_week"] = out.index.dayofweek
    out["is_weekend"] = out.index.dayofweek.isin([5, 6]).astype(int)

    # Cyclical encoding
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7)

    return out.dropna()
