"""
NEMWEB Custom PySpark Data Sink

This module implements custom data sinks for NEMWEB electricity market data:
1. PriceAlertSink - Triggers alerts when spot prices exceed thresholds
2. MetricsSink - Writes demand/price metrics to an observability endpoint

Usage:
    spark.dataSource.register(PriceAlertDataSource)
    df.write.format("nemweb_alerts").option("threshold", "300").save()

References:
    - Python Data Source API: https://docs.databricks.com/en/pyspark/datasources.html
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceWriter,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType
from typing import Iterator, Tuple
from datetime import datetime
from dataclasses import dataclass
import json
import logging
import urllib.request

logger = logging.getLogger(__name__)


@dataclass
class AlertCommitMessage(WriterCommitMessage):
    """Message returned after writing alerts."""
    partition_id: int
    alerts_sent: int
    regions_affected: list


class PriceAlertWriter(DataSourceWriter):
    """
    Writer that sends alerts when electricity prices exceed thresholds.

    Useful for:
    - Operational alerts when prices spike
    - Demand response triggers
    - Trading signals
    """

    def __init__(self, options: dict):
        self.options = options
        # Price threshold in $/MWh (Australian NEM can spike to $15,000+)
        self.price_threshold = float(options.get("price_threshold", "300"))
        # Demand threshold in MW
        self.demand_threshold = float(options.get("demand_threshold", "10000"))
        # Webhook URL for alerts
        self.webhook_url = options.get("webhook_url")
        # Alert mode: "log", "webhook", or "both"
        self.alert_mode = options.get("alert_mode", "log")

    def write(self, iterator: Iterator[Tuple]) -> WriterCommitMessage:
        """
        Process rows and send alerts for threshold breaches.

        This runs on executors - each partition is processed independently.
        """
        alerts_sent = 0
        regions_affected = set()

        for row in iterator:
            # Expected row format from DISPATCHPRICE or enriched DISPATCHREGIONSUM
            # Row indices depend on schema - this assumes enriched schema
            try:
                region_id = self._get_field(row, "REGIONID", 2)
                price = self._get_field(row, "RRP", 5)  # Regional Reference Price
                demand = self._get_field(row, "TOTALDEMAND", 6)
                timestamp = self._get_field(row, "SETTLEMENTDATE", 0)

                # Check price threshold
                if price and float(price) > self.price_threshold:
                    alert = {
                        "type": "PRICE_SPIKE",
                        "region": region_id,
                        "price": float(price),
                        "threshold": self.price_threshold,
                        "timestamp": str(timestamp),
                        "alert_time": datetime.now().isoformat(),
                    }
                    self._send_alert(alert)
                    alerts_sent += 1
                    regions_affected.add(region_id)

                # Check demand threshold
                if demand and float(demand) > self.demand_threshold:
                    alert = {
                        "type": "HIGH_DEMAND",
                        "region": region_id,
                        "demand_mw": float(demand),
                        "threshold": self.demand_threshold,
                        "timestamp": str(timestamp),
                        "alert_time": datetime.now().isoformat(),
                    }
                    self._send_alert(alert)
                    alerts_sent += 1
                    regions_affected.add(region_id)

            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue

        return AlertCommitMessage(
            partition_id=0,  # Would be actual partition ID
            alerts_sent=alerts_sent,
            regions_affected=list(regions_affected)
        )

    def _get_field(self, row: Tuple, name: str, index: int):
        """Safely get field from row tuple."""
        try:
            return row[index]
        except (IndexError, TypeError):
            return None

    def _send_alert(self, alert: dict):
        """Send alert via configured channel."""
        if self.alert_mode in ("log", "both"):
            logger.warning(f"ALERT: {json.dumps(alert)}")
            print(f"🚨 ALERT: {alert['type']} in {alert['region']}")

        if self.alert_mode in ("webhook", "both") and self.webhook_url:
            try:
                data = json.dumps(alert).encode("utf-8")
                request = urllib.request.Request(
                    self.webhook_url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"Webhook returned {response.status}")
            except Exception as e:
                logger.error(f"Failed to send webhook: {e}")


class PriceAlertDataSource(DataSource):
    """
    Custom sink for NEMWEB price and demand alerts.

    Options:
        price_threshold (float): Alert when RRP exceeds this ($/MWh, default: 300)
        demand_threshold (float): Alert when demand exceeds this (MW, default: 10000)
        webhook_url (str): URL to POST alerts to (optional)
        alert_mode (str): "log", "webhook", or "both" (default: "log")

    Example:
        (df.write
            .format("nemweb_alerts")
            .option("price_threshold", "500")
            .option("demand_threshold", "8000")
            .option("webhook_url", "https://hooks.slack.com/...")
            .option("alert_mode", "both")
            .mode("append")
            .save())
    """

    @classmethod
    def name(cls) -> str:
        return "nemweb_alerts"

    def schema(self) -> str:
        """Writer doesn't need to return schema."""
        return ""

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        """Create a writer for this sink."""
        return PriceAlertWriter(self.options)


class MetricsWriter(DataSourceWriter):
    """
    Writer that publishes NEMWEB metrics to an observability endpoint.

    Useful for:
    - Dashboards (Grafana, Datadog)
    - Time-series databases (InfluxDB, TimescaleDB)
    - Custom monitoring systems
    """

    def __init__(self, options: dict):
        self.options = options
        self.metrics_url = options.get("metrics_url")
        self.metrics_prefix = options.get("metrics_prefix", "nemweb")
        self.batch_size = int(options.get("batch_size", "100"))

    def write(self, iterator: Iterator[Tuple]) -> WriterCommitMessage:
        """
        Convert rows to metrics and publish.

        Outputs metrics in InfluxDB line protocol format:
        measurement,tag=value field=value timestamp
        """
        metrics_batch = []
        total_rows = 0

        for row in iterator:
            try:
                region_id = row[2] if len(row) > 2 else "unknown"
                demand = float(row[5]) if len(row) > 5 and row[5] else 0
                timestamp = row[0]

                # Format as InfluxDB line protocol
                metric_line = (
                    f"{self.metrics_prefix}_demand,"
                    f"region={region_id} "
                    f"value={demand} "
                    f"{self._to_nano_timestamp(timestamp)}"
                )
                metrics_batch.append(metric_line)
                total_rows += 1

                # Flush batch
                if len(metrics_batch) >= self.batch_size:
                    self._flush_metrics(metrics_batch)
                    metrics_batch = []

            except Exception as e:
                logger.warning(f"Error formatting metric: {e}")
                continue

        # Final flush
        if metrics_batch:
            self._flush_metrics(metrics_batch)

        return WriterCommitMessage()

    def _to_nano_timestamp(self, ts) -> int:
        """Convert timestamp to nanoseconds since epoch."""
        if isinstance(ts, datetime):
            return int(ts.timestamp() * 1e9)
        return int(datetime.now().timestamp() * 1e9)

    def _flush_metrics(self, metrics: list):
        """Send metrics batch to endpoint."""
        if not self.metrics_url:
            # Just log if no URL configured
            for m in metrics:
                logger.debug(f"METRIC: {m}")
            return

        try:
            data = "\n".join(metrics).encode("utf-8")
            request = urllib.request.Request(
                self.metrics_url,
                data=data,
                headers={"Content-Type": "text/plain"},
                method="POST"
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status not in (200, 204):
                    logger.warning(f"Metrics endpoint returned {response.status}")
        except Exception as e:
            logger.error(f"Failed to send metrics: {e}")


class MetricsDataSource(DataSource):
    """
    Custom sink for publishing NEMWEB metrics to observability systems.

    Options:
        metrics_url (str): URL to POST metrics (e.g., InfluxDB write endpoint)
        metrics_prefix (str): Prefix for metric names (default: "nemweb")
        batch_size (int): Rows to batch before sending (default: 100)

    Example:
        (df.write
            .format("nemweb_metrics")
            .option("metrics_url", "http://influxdb:8086/write?db=energy")
            .option("metrics_prefix", "nem")
            .mode("append")
            .save())
    """

    @classmethod
    def name(cls) -> str:
        return "nemweb_metrics"

    def schema(self) -> str:
        return ""

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return MetricsWriter(self.options)


# Export sinks for registration
__all__ = ["PriceAlertDataSource", "MetricsDataSource"]
