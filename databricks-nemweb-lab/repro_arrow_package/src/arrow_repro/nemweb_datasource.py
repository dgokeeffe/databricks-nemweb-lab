"""
NEMWEB Data Source - Real-world reproduction of Arrow error.

This datasource fetches real NEMWEB CSV data and has a bug in timestamp parsing:
when parsing fails, it returns the raw string instead of None.

This causes Arrow AssertionError because TimestampType columns must contain
datetime.datetime or None, not strings.
"""

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
from typing import Iterator
from datetime import datetime
import csv
import io
import zipfile
from urllib.request import urlopen, Request
from urllib.error import HTTPError


NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/DispatchIS_Reports/"


class NemwebPartition(InputPartition):
    """Partition holding a NEMWEB file URL to fetch."""

    def __init__(self, url: str):
        self.url = url


class NemwebArrowDataSource(DataSource):
    """
    Datasource that fetches real NEMWEB data with buggy timestamp parsing.

    Usage:
        spark.dataSource.register(NemwebArrowDataSource)
        df = spark.read.format("nemweb_broken").option("max_files", "1").load()
    """

    @classmethod
    def name(cls):
        return "nemweb_broken"

    def schema(self):
        return StructType([
            StructField("SETTLEMENTDATE", TimestampType(), True),
            StructField("REGIONID", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
        ])

    def reader(self, schema):
        return NemwebReader(schema, self.options)


class NemwebReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        self.max_files = int(options.get("max_files", "1"))

    def partitions(self) -> list[InputPartition]:
        """Find recent NEMWEB dispatch files to fetch."""
        import re

        try:
            request = Request(NEMWEB_CURRENT_URL, headers={"User-Agent": "ArrowReproTest/1.0"})
            with urlopen(request, timeout=30) as response:
                html = response.read().decode('utf-8')

            pattern = r'(PUBLIC_DISPATCHIS_\d{12}_\d+\.zip)'
            matches = re.findall(pattern, html, re.IGNORECASE)
            unique_files = sorted(set(matches), reverse=True)[:self.max_files]

            return [NemwebPartition(f"{NEMWEB_CURRENT_URL}{f}") for f in unique_files]

        except Exception as e:
            print(f"Error listing NEMWEB directory: {e}")
            return []

    def read(self, partition: NemwebPartition) -> Iterator[tuple]:
        """Fetch and parse NEMWEB CSV data."""
        try:
            print(f"Fetching: {partition.url}")
            request = Request(partition.url, headers={"User-Agent": "ArrowReproTest/1.0"})
            with urlopen(request, timeout=30) as response:
                raw_data = response.read()

            zip_data = io.BytesIO(raw_data)

            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    if name.upper().endswith(".CSV"):
                        with zf.open(name) as csv_file:
                            yield from self._parse_csv(csv_file)

        except HTTPError as e:
            print(f"HTTP error: {e}")
        except Exception as e:
            print(f"Error: {e}")
            raise

    def _parse_csv(self, csv_file) -> Iterator[tuple]:
        """Parse NEMWEB multi-record CSV format."""
        text = csv_file.read().decode("utf-8")
        reader = csv.reader(io.StringIO(text))

        headers = None
        record_type = "DISPATCH,REGIONSUM"

        for parts in reader:
            if not parts:
                continue

            row_type = parts[0].strip().upper()

            if row_type == "I" and len(parts) > 2:
                row_record = f"{parts[1]},{parts[2]}"
                if row_record == record_type:
                    headers = parts[4:]

            elif row_type == "D" and headers and len(parts) > 2:
                row_record = f"{parts[1]},{parts[2]}"
                if row_record == record_type:
                    values = parts[4:]
                    row = dict(zip(headers, values))

                    # Parse timestamp - BUG: returns string on failure!
                    ts_str = row.get("SETTLEMENTDATE", "").strip().strip('"')
                    timestamp = self._buggy_parse_timestamp(ts_str)

                    regionid = row.get("REGIONID", "")
                    try:
                        demand = float(row.get("TOTALDEMAND", 0))
                    except (ValueError, TypeError):
                        demand = None

                    yield (timestamp, regionid, demand)

    def _buggy_parse_timestamp(self, ts_str: str):
        """
        BUG: Returns the raw string when parsing fails instead of None.

        This is the original bug - when datetime.strptime() fails,
        we return the original string instead of None.
        Spark's Arrow serializer then fails with AssertionError because
        it expects datetime.datetime or None for TimestampType.
        """
        if not ts_str:
            return None

        try:
            return datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            # BUG: Return the string instead of None!
            # This causes Arrow AssertionError
            return ts_str  # <- THIS IS THE BUG
