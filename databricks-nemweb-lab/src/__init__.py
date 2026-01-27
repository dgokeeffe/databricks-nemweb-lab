"""
Databricks NEMWEB Lab - Custom Data Source Package

This package provides custom PySpark data sources and sinks for AEMO NEMWEB
electricity market data using Databricks' Python Data Source API.

Requires: DBR 15.4+ / Spark 4.0+

Sources (readers):
    - NemwebDataSource: Row-tuple based reader (format: 'nemweb')
      Educational implementation for learning the Data Source API
    - NemwebArrowDataSource: PyArrow-based reader (format: 'nemweb_arrow')
      Production implementation with auto-download and zero-copy transfer
    - NemwebStreamDataSource: Streaming reader (format: 'nemweb_stream')
      Polls CURRENT folder for near-real-time 5-minute dispatch data
    - NemRegistryDataSource: Generator metadata (format: 'nem_registry')
      Station names, fuel types, capacities from OpenNEM

Sinks (writers):
    - PriceAlertDataSource: Triggers alerts on price/demand thresholds
    - MetricsDataSource: Publishes metrics to observability endpoints
"""

try:
    from .nemweb_datasource import NemwebDataSource, NemwebStreamReader
except ImportError:
    # nemweb_datasource.py may not exist (educational implementation)
    NemwebDataSource = None
    NemwebStreamReader = None

from .nemweb_datasource_arrow import NemwebArrowDataSource
from .nemweb_datasource_stream import NemwebStreamDataSource
from .nem_registry import NemRegistryDataSource, load_registry_data
from .nemweb_utils import (
    fetch_nemweb_data,
    fetch_with_retry,
    parse_nemweb_csv,
    get_nemweb_schema,
    get_nem_regions,
    get_version,
    list_available_tables,
)
from .nemweb_sink import PriceAlertDataSource, MetricsDataSource

__all__ = [
    # Data sources (readers)
    "NemwebDataSource",       # format: 'nemweb' - educational (may be None)
    "NemwebArrowDataSource",  # format: 'nemweb_arrow' - production batch
    "NemwebStreamDataSource", # format: 'nemweb_stream' - streaming
    "NemRegistryDataSource",  # format: 'nem_registry' - generator metadata
    "NemwebStreamReader",     # may be None
    # Data sinks (writers)
    "PriceAlertDataSource",
    "MetricsDataSource",
    # Utilities
    "fetch_nemweb_data",
    "fetch_with_retry",
    "parse_nemweb_csv",
    "get_nemweb_schema",
    "get_nem_regions",
    "get_version",
    "list_available_tables",
    "load_registry_data",
]
