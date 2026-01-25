"""
Databricks NEMWEB Lab - Custom Data Source Package

This package provides custom PySpark data sources and sinks for AEMO NEMWEB
electricity market data using Databricks' Python Data Source API.

Requires: DBR 15.4+ / Spark 4.0+

Sources (readers):
    - NemwebDataSource: Batch and streaming reader for NEMWEB data

Sinks (writers):
    - PriceAlertDataSource: Triggers alerts on price/demand thresholds
    - MetricsDataSource: Publishes metrics to observability endpoints
"""

from .nemweb_datasource import NemwebDataSource, NemwebStreamReader
from .nemweb_utils import (
    fetch_nemweb_data,
    fetch_with_retry,
    parse_nemweb_csv,
    get_nemweb_schema,
    get_nem_regions,
    list_available_tables,
)
from .nemweb_sink import PriceAlertDataSource, MetricsDataSource

__all__ = [
    # Data sources (readers)
    "NemwebDataSource",
    "NemwebStreamReader",
    # Data sinks (writers)
    "PriceAlertDataSource",
    "MetricsDataSource",
    # Utilities
    "fetch_nemweb_data",
    "fetch_with_retry",
    "parse_nemweb_csv",
    "get_nemweb_schema",
    "get_nem_regions",
    "list_available_tables",
]
