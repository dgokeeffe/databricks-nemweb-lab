"""
Local development version of NEMWEB data source using DuckDB.

This allows testing the data fetching logic without Spark/Databricks.
Uses DuckDB for local checkpoint storage.

Usage:
    from nemweb_local import NemwebLocalReader

    # Fetch data locally
    reader = NemwebLocalReader(
        regions=["NSW1", "VIC1"],
        start_date="2024-01-01",
        end_date="2024-01-07",
        checkpoint_db="checkpoints.duckdb"
    )

    # Get data as list of dicts
    data = reader.read_all()

    # Or as a DuckDB relation
    rel = reader.to_duckdb()

    # Or as pandas DataFrame
    df = reader.to_pandas()

Requirements:
    pip install duckdb pandas requests
"""

import duckdb
import hashlib
from datetime import datetime, timedelta
from typing import Optional
import logging

from nemweb_utils import fetch_nemweb_data, get_nemweb_schema, get_nem_regions

logger = logging.getLogger(__name__)


class NemwebLocalReader:
    """
    Local reader for NEMWEB data using DuckDB for checkpoints.

    No Spark required - pure Python implementation for local development.
    """

    def __init__(
        self,
        regions: list[str] = None,
        start_date: str = "2024-01-01",
        end_date: str = "2024-01-07",
        table: str = "DISPATCHREGIONSUM",
        checkpoint_db: Optional[str] = None,
        skip_completed: bool = True
    ):
        self.regions = regions or get_nem_regions()
        self.start_date = start_date
        self.end_date = end_date
        self.table = table
        self.checkpoint_db = checkpoint_db
        self.skip_completed = skip_completed

        # Initialize DuckDB connection for checkpoints
        if checkpoint_db:
            self.conn = duckdb.connect(checkpoint_db)
            self._ensure_checkpoint_table()
        else:
            self.conn = None

    def _ensure_checkpoint_table(self):
        """Create checkpoint table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                partition_id VARCHAR PRIMARY KEY,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _get_partition_id(self, region: str, date: str) -> str:
        """Generate deterministic partition ID."""
        id_string = f"{self.table}:{region}:{date}"
        return hashlib.md5(id_string.encode()).hexdigest()[:12]

    def _get_completed_partitions(self) -> set:
        """Load completed partition IDs from DuckDB."""
        if not self.conn:
            return set()

        result = self.conn.execute(
            "SELECT partition_id FROM checkpoints"
        ).fetchall()
        return {row[0] for row in result}

    def _mark_completed(self, partition_id: str):
        """Mark a partition as completed using UPSERT."""
        if not self.conn:
            return

        # DuckDB uses INSERT OR REPLACE for upsert
        self.conn.execute("""
            INSERT OR REPLACE INTO checkpoints (partition_id, completed_at)
            VALUES (?, CURRENT_TIMESTAMP)
        """, [partition_id])

    def _generate_dates(self) -> list[str]:
        """Generate list of dates in range."""
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")

        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    def get_partitions(self) -> list[tuple]:
        """
        Get list of (region, date) partitions to process.

        Filters out completed partitions if checkpointing enabled.
        """
        completed = self._get_completed_partitions() if self.skip_completed else set()

        partitions = []
        for date in self._generate_dates():
            for region in self.regions:
                partition_id = self._get_partition_id(region, date)
                if partition_id not in completed:
                    partitions.append((region, date, partition_id))

        logger.info(f"Partitions: {len(partitions)} to process, {len(completed)} already complete")
        return partitions

    def read_partition(self, region: str, date: str) -> list[dict]:
        """Read data for a single partition."""
        try:
            data = fetch_nemweb_data(
                table=self.table,
                region=region,
                start_date=date,
                end_date=date
            )
            return data
        except Exception as e:
            logger.error(f"Error reading {region}/{date}: {e}")
            return []

    def read_all(self, mark_complete: bool = True) -> list[dict]:
        """
        Read all partitions and return combined data.

        Args:
            mark_complete: If True, mark partitions as completed after reading

        Returns:
            List of row dicts
        """
        all_data = []
        partitions = self.get_partitions()

        for region, date, partition_id in partitions:
            logger.info(f"Reading {region} / {date}")
            data = self.read_partition(region, date)
            all_data.extend(data)

            if mark_complete and data:
                self._mark_completed(partition_id)

        logger.info(f"Total rows: {len(all_data)}")
        return all_data

    def to_duckdb(self, table_name: str = "nemweb_data") -> duckdb.DuckDBPyRelation:
        """
        Read data and return as DuckDB relation for SQL queries.

        Usage:
            rel = reader.to_duckdb()
            result = duckdb.sql("SELECT * FROM nemweb_data WHERE REGIONID = 'NSW1'")
        """
        data = self.read_all()

        if not data:
            return None

        # Create in-memory connection if no checkpoint DB
        conn = self.conn or duckdb.connect()

        # Create table from data
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM (VALUES {self._data_to_values(data)})
        """)

        return conn.table(table_name)

    def to_pandas(self):
        """Read data and return as pandas DataFrame."""
        import pandas as pd
        data = self.read_all()
        return pd.DataFrame(data)

    def _data_to_values(self, data: list[dict]) -> str:
        """Convert list of dicts to SQL VALUES clause."""
        if not data:
            return ""

        # Get columns from first row
        columns = list(data[0].keys())

        rows = []
        for row in data:
            values = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    values.append("NULL")
                elif isinstance(val, str):
                    values.append(f"'{val}'")
                else:
                    values.append(str(val))
            rows.append(f"({', '.join(values)})")

        return ", ".join(rows)

    def reset_checkpoints(self):
        """Clear all checkpoints to reprocess from scratch."""
        if self.conn:
            self.conn.execute("DELETE FROM checkpoints")
            logger.info("Checkpoints cleared")


# Convenience function for quick local testing
def fetch_nemweb_local(
    regions: list[str] = None,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-07",
    as_pandas: bool = True
):
    """
    Quick fetch of NEMWEB data for local development.

    Usage:
        df = fetch_nemweb_local(["NSW1", "VIC1"], "2024-01-01", "2024-01-03")
    """
    reader = NemwebLocalReader(
        regions=regions,
        start_date=start_date,
        end_date=end_date,
        checkpoint_db=None  # No checkpointing for quick fetches
    )

    if as_pandas:
        return reader.to_pandas()
    return reader.read_all()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Quick fetch without checkpointing
    print("Fetching sample data...")
    df = fetch_nemweb_local(
        regions=["NSW1"],
        start_date="2024-01-01",
        end_date="2024-01-01"
    )
    print(f"Got {len(df)} rows")
    print(df.head())

    # With checkpointing
    print("\nFetching with checkpoints...")
    reader = NemwebLocalReader(
        regions=["NSW1", "VIC1"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        checkpoint_db="local_checkpoints.duckdb"
    )

    data = reader.read_all()
    print(f"First run: {len(data)} rows")

    # Second run should skip completed partitions
    data2 = reader.read_all()
    print(f"Second run: {len(data2)} rows (should be 0 if checkpointing works)")
