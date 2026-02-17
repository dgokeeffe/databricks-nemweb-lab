"""Shared fixtures for the NEMWEB ML Lab test suite.

Generates realistic NEM dispatch data (5-min intervals) resembling actual
AEMO NEMWEB output for NSW1. Values mirror typical summer afternoon profiles
so feature engineering tests exercise real-world patterns.
"""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def nem_dispatch_df() -> pd.DataFrame:
    """48 hours of synthetic NSW1 dispatch data at 5-min resolution.

    576 rows (48h * 12 intervals/h).  Includes weather columns so both
    load-forecast and price-forecast feature pipelines can be exercised.
    """
    rng = np.random.default_rng(42)
    n = 576  # 48 hours of 5-min intervals

    index = pd.date_range(
        "2025-01-15 00:00", periods=n, freq="5min", tz="Australia/Sydney"
    )

    hours = (index.hour + index.minute / 60.0).to_numpy()

    # Demand: sinusoidal diurnal pattern (peak ~18:00, trough ~04:00)
    base_demand = 8000 + 2000 * np.sin(2 * np.pi * (hours - 6) / 24)
    demand = base_demand + rng.normal(0, 200, n)

    # Available generation: slightly above demand with random headroom
    available_gen = demand + rng.uniform(500, 2000, n)

    # Demand forecast: AEMO's prediction (close but not perfect)
    demand_forecast = demand + rng.normal(50, 150, n)

    # Net interchange: positive = exporting, negative = importing
    interchange = rng.normal(-200, 300, n)

    # Price: correlated with demand-supply margin
    margin = available_gen - demand
    base_price = 80 - 0.02 * margin + rng.normal(0, 15, n)
    # Occasional price spikes when margin is tight
    spike_mask = margin < 800
    base_price[spike_mask] += rng.uniform(50, 500, spike_mask.sum())
    rrp = np.clip(base_price, -100, 15000)

    # Weather: summer afternoon pattern
    temp = 22 + 10 * np.sin(2 * np.pi * (hours - 3) / 24) + rng.normal(0, 1.5, n)
    apparent_temp = temp + rng.normal(1, 0.5, n)
    humidity = 55 - 15 * np.sin(2 * np.pi * (hours - 3) / 24) + rng.normal(0, 5, n)
    wind = np.abs(15 + rng.normal(0, 5, n))

    return pd.DataFrame(
        {
            "total_demand_mw": demand,
            "available_generation_mw": available_gen,
            "demand_forecast_mw": demand_forecast,
            "net_interchange_mw": interchange,
            "rrp": rrp,
            "rop": rrp * 0.98,
            "region_id": "NSW1",
            "air_temp_c": temp,
            "apparent_temp_c": apparent_temp,
            "rel_humidity_pct": humidity,
            "wind_speed_kmh": wind,
            "_processed_at": pd.Timestamp.now(),
        },
        index=index,
    )


@pytest.fixture
def nem_dispatch_no_weather_df(nem_dispatch_df: pd.DataFrame) -> pd.DataFrame:
    """Dispatch data without weather columns (tests graceful fallback)."""
    return nem_dispatch_df.drop(
        columns=["air_temp_c", "apparent_temp_c", "rel_humidity_pct", "wind_speed_kmh"]
    )
