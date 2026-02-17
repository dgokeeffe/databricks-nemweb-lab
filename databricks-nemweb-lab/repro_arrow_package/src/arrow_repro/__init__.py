"""
Arrow Serialization Error Reproduction Package

Two datasources that trigger the Arrow assertion error on Serverless:

1. BrokenArrowDataSource - Minimal repro with hardcoded datetime
2. NemwebArrowDataSource - Real-world repro fetching NEMWEB CSV data

The bug: Yielding tuples with datetime values causes AssertionError.
The fix: Use Row objects instead of tuples.
"""

from arrow_repro.broken_datasource import BrokenArrowDataSource
from arrow_repro.nemweb_datasource import NemwebArrowDataSource

__version__ = "0.1.6"
__all__ = ["BrokenArrowDataSource", "NemwebArrowDataSource"]
