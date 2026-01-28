"""
NEMWEB Dispatch Reports Fetcher

Simple functions to fetch real-time dispatch data from NEMWEB's Dispatch_Reports folder.
Returns Python dicts that can be easily converted to Spark DataFrames or pandas.

This is the most comprehensive dispatch data source with:
- Regional prices (RRP) and demand
- FCAS (Frequency Control Ancillary Services) prices
- Interconnector flows between regions
- Constraint binding information

Usage with PySpark:
    from nemweb_dispatch import fetch_dispatch_region, fetch_dispatch_interconnector

    # Fetch latest regional data
    region_data = fetch_dispatch_region(max_files=1)
    df = spark.createDataFrame(region_data)

    # Fetch interconnector flows
    interconnector_data = fetch_dispatch_interconnector(max_files=1)
    ic_df = spark.createDataFrame(interconnector_data)

Usage with pandas:
    import pandas as pd
    df = pd.DataFrame(fetch_dispatch_region(max_files=6))
"""

import csv
import io
import logging
import re
import zipfile
from datetime import datetime
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

# NEMWEB configuration
NEMWEB_CURRENT_URL = "https://www.nemweb.com.au/REPORTS/CURRENT"
DISPATCH_REPORTS_FOLDER = "Dispatch_Reports"
FILE_PREFIX = "DISPATCH"
FILE_SUFFIX = "_LEGACY"

# HTTP configuration
REQUEST_TIMEOUT = 30
USER_AGENT = "DatabricksNemwebLab/2.11"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0


def _fetch_with_retry(url: str) -> Optional[bytes]:
    """Fetch URL with exponential backoff retry."""
    import time

    for attempt in range(MAX_RETRIES):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()
        except HTTPError as e:
            if e.code == 404:
                logger.warning(f"File not found: {url}")
                return None
            logger.warning(f"HTTP error {e.code} fetching {url}, attempt {attempt + 1}")
        except (URLError, TimeoutError) as e:
            logger.warning(f"Error fetching {url}: {e}, attempt {attempt + 1}")

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_BASE_DELAY * (2 ** attempt))

    return None


def _list_dispatch_files() -> list[str]:
    """List available Dispatch_Reports ZIP files from CURRENT folder."""
    url = f"{NEMWEB_CURRENT_URL}/{DISPATCH_REPORTS_FOLDER}/"

    try:
        request = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            html = response.read().decode("utf-8")
    except (HTTPError, URLError) as e:
        logger.error(f"Failed to list Dispatch_Reports: {e}")
        return []

    # Pattern: PUBLIC_DISPATCH_YYYYMMDDHHMM_YYYYMMDDHHmmss_LEGACY.zip
    pattern = rf"(PUBLIC_{FILE_PREFIX}_\d{{12}}_\d{{14}}{FILE_SUFFIX}\.zip)"
    matches = re.findall(pattern, html, re.IGNORECASE)
    return sorted(set(matches), reverse=True)


def _parse_csv(csv_bytes: bytes, record_type: str) -> list[dict]:
    """Parse NEMWEB multi-record CSV format."""
    text = csv_bytes.decode("utf-8")
    rows = []
    headers = None

    reader = csv.reader(io.StringIO(text))

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
                row_dict = dict(zip(headers, values))
                rows.append(row_dict)

    return rows


def _fetch_and_parse(filenames: list[str], record_type: str) -> list[dict]:
    """Fetch ZIP files and parse records."""
    all_rows = []

    for filename in filenames:
        url = f"{NEMWEB_CURRENT_URL}/{DISPATCH_REPORTS_FOLDER}/{filename}"
        raw_data = _fetch_with_retry(url)

        if raw_data is None:
            continue

        try:
            zip_data = io.BytesIO(raw_data)
            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    if name.upper().endswith(".CSV"):
                        with zf.open(name) as f:
                            csv_bytes = f.read()
                            rows = _parse_csv(csv_bytes, record_type)
                            all_rows.extend(rows)
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            continue

    return all_rows


def _convert_types(row: dict, numeric_fields: list[str]) -> dict:
    """Convert string values to appropriate types."""
    result = {}
    for key, value in row.items():
        if value is None or value == "":
            result[key] = None
        elif key in numeric_fields:
            try:
                result[key] = float(value)
            except (ValueError, TypeError):
                result[key] = None
        elif key == "SETTLEMENTDATE":
            # Parse timestamp
            try:
                # Handle quoted timestamps
                ts_str = value.strip().strip('"').strip("'")
                for fmt in ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        result[key] = datetime.strptime(ts_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    result[key] = ts_str  # Keep as string if parsing fails
            except Exception:
                result[key] = value
        else:
            result[key] = value
    return result


def fetch_dispatch_region(
    max_files: int = 1,
    regions: Optional[list[str]] = None,
    debug: bool = False,
) -> list[dict]:
    """
    Fetch regional dispatch data with prices, demand, and FCAS.

    This is the most useful single table - contains prices, demand, generation,
    and FCAS prices for all 5 NEM regions in each dispatch interval.

    Args:
        max_files: Number of recent 5-minute files to fetch (default: 1 = latest interval)
        regions: Optional list of regions to filter (e.g., ["NSW1", "VIC1"])
        debug: Print debug info

    Returns:
        List of dicts with keys:
        - SETTLEMENTDATE: Dispatch interval timestamp
        - REGIONID: NSW1, QLD1, SA1, VIC1, TAS1
        - RRP: Regional Reference Price ($/MWh)
        - TOTALDEMAND: Regional demand (MW)
        - NETINTERCHANGE: Import (+) / Export (-) with other regions
        - AVAILABLEGENERATION: Total available generation capacity
        - RAISE6SECRRP, LOWER6SECRRP, etc.: FCAS prices
    """
    if debug:
        print(f"[DISPATCH] Fetching regional data (max_files={max_files})")

    files = _list_dispatch_files()[:max_files]
    if debug:
        print(f"[DISPATCH] Found {len(files)} files to process")

    rows = _fetch_and_parse(files, "DREGION,")

    # Convert types
    numeric_fields = [
        "RRP", "EEP", "ROP", "TOTALDEMAND", "DEMANDFORECAST",
        "DISPATCHABLEGENERATION", "DISPATCHABLELOAD", "NETINTERCHANGE",
        "AVAILABLEGENERATION", "AVAILABLELOAD", "CLEAREDSUPPLY",
        "RAISE6SECRRP", "RAISE60SECRRP", "RAISE5MINRRP",
        "LOWER6SECRRP", "LOWER60SECRRP", "LOWER5MINRRP",
        "RAISEREGRRP", "LOWERREGRRP",
    ]
    rows = [_convert_types(r, numeric_fields) for r in rows]

    # Filter regions
    if regions:
        rows = [r for r in rows if r.get("REGIONID") in regions]

    if debug:
        print(f"[DISPATCH] Returning {len(rows)} rows")

    return rows


def fetch_dispatch_interconnector(
    max_files: int = 1,
    interconnectors: Optional[list[str]] = None,
    debug: bool = False,
) -> list[dict]:
    """
    Fetch interconnector dispatch data with flows and limits.

    Shows power flowing between NEM regions via interconnectors:
    - NSW1-QLD1 (QNI - Queensland/NSW Interconnector)
    - VIC1-NSW1 (VNI - Victoria/NSW Interconnector)
    - V-SA (Heywood - Victoria/SA)
    - T-V-MNSP1 (Basslink - Tasmania/Victoria)
    - N-Q-MNSP1 (Directlink - NSW/QLD MNSP)
    - V-S-MNSP1 (Murraylink - Victoria/SA MNSP)

    Positive MWFLOW = power flowing in direction of interconnector name
    (e.g., NSW1-QLD1 positive = power from NSW to QLD)

    Args:
        max_files: Number of recent files to fetch
        interconnectors: Optional list to filter (e.g., ["NSW1-QLD1", "VIC1-NSW1"])
        debug: Print debug info

    Returns:
        List of dicts with keys:
        - SETTLEMENTDATE: Dispatch interval timestamp
        - INTERCONNECTORID: Name of interconnector
        - METEREDMWFLOW: Actual metered power flow (MW)
        - MWFLOW: Dispatch target flow (MW)
        - MWLOSSES: Losses on interconnector (MW)
        - IMPORTLIMIT: Maximum import to first region (MW)
        - EXPORTLIMIT: Maximum export from first region (MW)
        - MARGINALVALUE: Value of extra interconnector capacity ($/MW)
    """
    if debug:
        print(f"[DISPATCH] Fetching interconnector data (max_files={max_files})")

    files = _list_dispatch_files()[:max_files]
    rows = _fetch_and_parse(files, "DINT,")

    # Convert types
    numeric_fields = [
        "METEREDMWFLOW", "MWFLOW", "MWLOSSES", "MARGINALVALUE",
        "VIOLATIONDEGREE", "IMPORTLIMIT", "EXPORTLIMIT", "MARGINALLOSS",
    ]
    rows = [_convert_types(r, numeric_fields) for r in rows]

    # Filter interconnectors
    if interconnectors:
        rows = [r for r in rows if r.get("INTERCONNECTORID") in interconnectors]

    if debug:
        print(f"[DISPATCH] Returning {len(rows)} rows")

    return rows


def fetch_dispatch_interconnector_metered(
    max_files: int = 1,
    debug: bool = False,
) -> list[dict]:
    """
    Fetch trading interval metered interconnector flows.

    Simpler than DINT - just metered flows and losses.

    Args:
        max_files: Number of recent files to fetch
        debug: Print debug info

    Returns:
        List of dicts with INTERCONNECTORID, METEREDMWFLOW, MWFLOW, MWLOSSES
    """
    if debug:
        print(f"[DISPATCH] Fetching metered interconnector data")

    files = _list_dispatch_files()[:max_files]
    rows = _fetch_and_parse(files, "TINT,")

    numeric_fields = ["METEREDMWFLOW", "MWFLOW", "MWLOSSES"]
    rows = [_convert_types(r, numeric_fields) for r in rows]

    return rows


def get_latest_prices(debug: bool = False) -> dict:
    """
    Get latest prices for all regions - convenience function for dashboards.

    Returns:
        Dict mapping region to price, e.g.:
        {"NSW1": 85.50, "QLD1": 92.10, "SA1": 110.25, "VIC1": 78.90, "TAS1": 65.40}
    """
    data = fetch_dispatch_region(max_files=1, debug=debug)

    prices = {}
    for row in data:
        region = row.get("REGIONID")
        price = row.get("RRP")
        if region and price is not None:
            prices[region] = price

    return prices


def get_interconnector_flows(debug: bool = False) -> dict:
    """
    Get latest interconnector flows - convenience function for dashboards.

    Returns:
        Dict mapping interconnector to flow (MW), e.g.:
        {"NSW1-QLD1": -150.5, "VIC1-NSW1": 425.3, ...}
    """
    data = fetch_dispatch_interconnector(max_files=1, debug=debug)

    flows = {}
    for row in data:
        ic = row.get("INTERCONNECTORID")
        flow = row.get("METEREDMWFLOW")
        if ic and flow is not None:
            flows[ic] = flow

    return flows


class DispatchPoller:
    """
    Polls NEMWEB for new dispatch data and calls a callback when new intervals arrive.

    NEMWEB publishes new 5-minute dispatch files approximately 1-2 minutes after
    each interval ends. This poller detects new files and processes them.

    Usage in Databricks notebook:
        from nemweb_dispatch import DispatchPoller

        def on_new_data(data, interval_time):
            # data is list of dicts with regional dispatch info
            df = spark.createDataFrame(data)
            df.write.mode("append").saveAsTable("nemweb_live.dispatch_region")
            print(f"Saved {len(data)} rows for interval {interval_time}")

        poller = DispatchPoller(
            on_data=on_new_data,
            poll_interval=30,  # Check every 30 seconds
            include_interconnectors=True,
        )

        # Run for 1 hour
        poller.run(duration_minutes=60)

        # Or run indefinitely (Ctrl+C to stop)
        poller.run()
    """

    def __init__(
        self,
        on_data: callable = None,
        on_interconnector: callable = None,
        poll_interval: int = 30,
        include_interconnectors: bool = False,
        regions: Optional[list[str]] = None,
        debug: bool = True,
    ):
        """
        Initialize the poller.

        Args:
            on_data: Callback when new regional data arrives.
                     Signature: on_data(data: list[dict], interval_time: datetime)
            on_interconnector: Callback for interconnector data (optional).
                     Signature: on_interconnector(data: list[dict], interval_time: datetime)
            poll_interval: Seconds between checks for new files (default: 30)
            include_interconnectors: Also fetch interconnector data
            regions: Filter to specific regions
            debug: Print status messages
        """
        self.on_data = on_data
        self.on_interconnector = on_interconnector
        self.poll_interval = poll_interval
        self.include_interconnectors = include_interconnectors
        self.regions = regions
        self.debug = debug
        self._last_file = None
        self._running = False

    def _log(self, msg: str):
        if self.debug:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] {msg}")

    def _extract_interval_time(self, filename: str) -> Optional[datetime]:
        """Extract dispatch interval timestamp from filename."""
        # Pattern: PUBLIC_DISPATCH_YYYYMMDDHHMM_...
        match = re.search(r"PUBLIC_DISPATCH_(\d{12})_", filename)
        if match:
            ts_str = match.group(1)
            return datetime.strptime(ts_str, "%Y%m%d%H%M")
        return None

    def poll_once(self) -> Optional[dict]:
        """
        Check for new data once. Returns dict with data if new interval found.

        Returns:
            None if no new data, or dict with:
            - "interval_time": datetime of the dispatch interval
            - "filename": the source filename
            - "region_data": list of regional dispatch dicts
            - "interconnector_data": list of interconnector dicts (if enabled)
        """
        files = _list_dispatch_files()
        if not files:
            self._log("No files found")
            return None

        latest_file = files[0]

        # Check if this is a new file
        if latest_file == self._last_file:
            return None

        interval_time = self._extract_interval_time(latest_file)
        self._log(f"New interval: {interval_time} ({latest_file})")

        # Fetch the data
        result = {
            "interval_time": interval_time,
            "filename": latest_file,
            "region_data": [],
            "interconnector_data": [],
        }

        # Fetch regional data
        region_data = fetch_dispatch_region(max_files=1, regions=self.regions, debug=False)
        result["region_data"] = region_data

        # Fetch interconnector data if enabled
        if self.include_interconnectors:
            ic_data = fetch_dispatch_interconnector(max_files=1, debug=False)
            result["interconnector_data"] = ic_data

        self._last_file = latest_file
        return result

    def run(self, duration_minutes: Optional[int] = None):
        """
        Start polling for new data.

        Args:
            duration_minutes: Run for this many minutes then stop.
                            If None, runs until interrupted (Ctrl+C).
        """
        import time

        self._running = True
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60) if duration_minutes else None

        self._log(f"Starting poller (interval={self.poll_interval}s)")
        if duration_minutes:
            self._log(f"Will run for {duration_minutes} minutes")
        else:
            self._log("Running indefinitely (Ctrl+C to stop)")

        intervals_processed = 0

        try:
            while self._running:
                # Check for timeout
                if end_time and time.time() > end_time:
                    self._log(f"Duration reached. Processed {intervals_processed} intervals.")
                    break

                # Poll for new data
                result = self.poll_once()

                if result:
                    intervals_processed += 1
                    interval_time = result["interval_time"]

                    # Call callbacks
                    if self.on_data and result["region_data"]:
                        try:
                            self.on_data(result["region_data"], interval_time)
                        except Exception as e:
                            self._log(f"Error in on_data callback: {e}")

                    if self.on_interconnector and result["interconnector_data"]:
                        try:
                            self.on_interconnector(result["interconnector_data"], interval_time)
                        except Exception as e:
                            self._log(f"Error in on_interconnector callback: {e}")

                    # Log summary
                    if result["region_data"]:
                        prices = {r["REGIONID"]: r["RRP"] for r in result["region_data"]}
                        self._log(f"Prices: {', '.join(f'{k}=${v:.2f}' for k, v in prices.items())}")

                else:
                    # No new data, wait and try again
                    pass

                # Wait before next poll
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            self._log(f"Interrupted. Processed {intervals_processed} intervals.")

        self._running = False

    def stop(self):
        """Stop the poller (can be called from another thread)."""
        self._running = False


def poll_to_delta(
    table_name: str,
    catalog: str = "workspace",
    schema: str = "nemweb_live",
    duration_minutes: Optional[int] = None,
    poll_interval: int = 30,
    include_interconnectors: bool = True,
):
    """
    Convenience function to poll and write directly to Delta tables.

    Creates/appends to tables:
    - {catalog}.{schema}.dispatch_region
    - {catalog}.{schema}.dispatch_interconnector (if include_interconnectors=True)

    Usage:
        from nemweb_dispatch import poll_to_delta

        # Run for 1 hour, writing to Delta
        poll_to_delta(
            table_name="dispatch_region",
            catalog="workspace",
            schema="nemweb_live",
            duration_minutes=60,
        )

    Args:
        table_name: Base name for the table
        catalog: Unity Catalog name
        schema: Schema/database name
        duration_minutes: How long to run (None = indefinitely)
        poll_interval: Seconds between polls
        include_interconnectors: Also write interconnector data
    """
    # Import spark lazily - only needed when writing to Delta
    try:
        from databricks.sdk.runtime import spark
    except ImportError:
        raise ImportError("This function requires Databricks runtime. Use DispatchPoller for local testing.")

    full_table = f"{catalog}.{schema}.{table_name}"
    ic_table = f"{catalog}.{schema}.dispatch_interconnector"

    print(f"Writing dispatch data to: {full_table}")
    if include_interconnectors:
        print(f"Writing interconnector data to: {ic_table}")

    def on_region_data(data, interval_time):
        df = spark.createDataFrame(data)
        df.write.mode("append").saveAsTable(full_table)
        print(f"  Saved {len(data)} region rows for {interval_time}")

    def on_ic_data(data, interval_time):
        df = spark.createDataFrame(data)
        df.write.mode("append").saveAsTable(ic_table)
        print(f"  Saved {len(data)} interconnector rows for {interval_time}")

    poller = DispatchPoller(
        on_data=on_region_data,
        on_interconnector=on_ic_data if include_interconnectors else None,
        poll_interval=poll_interval,
        include_interconnectors=include_interconnectors,
        debug=True,
    )

    poller.run(duration_minutes=duration_minutes)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "poll":
        # Demo the poller - run for 2 minutes
        print("=" * 60)
        print("NEMWEB Dispatch Poller Demo")
        print("Polling for new 5-minute intervals...")
        print("=" * 60)

        def print_data(data, interval_time):
            print(f"\n{'='*40}")
            print(f"NEW DATA: {interval_time}")
            print(f"{'='*40}")
            for row in data:
                print(f"  {row['REGIONID']}: ${row['RRP']:.2f}/MWh, {row['TOTALDEMAND']:.0f}MW")

        def print_interconnector(data, interval_time):
            print("  Interconnector flows:")
            for row in data:
                direction = "→" if row["METEREDMWFLOW"] > 0 else "←"
                print(f"    {row['INTERCONNECTORID']}: {abs(row['METEREDMWFLOW']):.0f}MW {direction}")

        poller = DispatchPoller(
            on_data=print_data,
            on_interconnector=print_interconnector,
            poll_interval=30,
            include_interconnectors=True,
        )

        # Run for 2 minutes as demo
        poller.run(duration_minutes=2)

    else:
        # Quick fetch test
        print("Testing fetch_dispatch_region...")
        data = fetch_dispatch_region(max_files=1, debug=True)
        for row in data:
            print(f"  {row['REGIONID']}: ${row['RRP']:.2f}/MWh, {row['TOTALDEMAND']:.0f}MW demand")

    print("\nTesting fetch_dispatch_interconnector...")
    data = fetch_dispatch_interconnector(max_files=1, debug=True)
    for row in data:
        print(f"  {row['INTERCONNECTORID']}: {row['METEREDMWFLOW']:.1f}MW")

    print("\nTesting convenience functions...")
    prices = get_latest_prices()
    print(f"  Latest prices: {prices}")

    flows = get_interconnector_flows()
    print(f"  Interconnector flows: {flows}")
