"""
Unit tests for nemweb_sink.py

Tests the custom PySpark data sink implementations for NEMWEB alerts and metrics.
Run with: pytest test_nemweb_sink.py -v
"""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
import json

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Import the module under test (path configured in conftest.py)
from nemweb_sink import (
    PriceAlertWriter,
    PriceAlertDataSource,
    MetricsWriter,
    MetricsDataSource,
    AlertCommitMessage,
)


class TestPriceAlertWriter:
    """Tests for PriceAlertWriter class."""

    def test_writer_initialization(self):
        """Should initialize with options."""
        options = {
            "price_threshold": "500",
            "demand_threshold": "8000",
            "webhook_url": "https://example.com/webhook",
            "alert_mode": "both"
        }

        writer = PriceAlertWriter(options)

        assert writer.price_threshold == 500.0
        assert writer.demand_threshold == 8000.0
        assert writer.webhook_url == "https://example.com/webhook"
        assert writer.alert_mode == "both"

    def test_writer_default_options(self):
        """Should use defaults for missing options."""
        writer = PriceAlertWriter({})

        assert writer.price_threshold == 300.0
        assert writer.demand_threshold == 10000.0
        assert writer.webhook_url is None
        assert writer.alert_mode == "log"

    def test_write_detects_price_spike(self):
        """Should detect price spikes above threshold."""
        writer = PriceAlertWriter({"price_threshold": "100", "alert_mode": "log"})

        # Row format: (timestamp, runno, regionid, interval, intervention, rrp, totaldemand, ...)
        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", 150.0, 7500.0),  # Above threshold
            (datetime.now(), "1", "VIC1", "1", "0", 50.0, 5000.0),   # Below threshold
        ]

        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        assert result.alerts_sent == 1
        assert "NSW1" in result.regions_affected

    def test_write_detects_high_demand(self):
        """Should detect demand above threshold."""
        writer = PriceAlertWriter({
            "price_threshold": "1000",  # High to avoid price alerts
            "demand_threshold": "6000",
            "alert_mode": "log"
        })

        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", 100.0, 8000.0),  # Above demand threshold
            (datetime.now(), "1", "VIC1", "1", "0", 100.0, 5000.0),  # Below threshold
        ]

        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        assert result.alerts_sent == 1
        assert "NSW1" in result.regions_affected

    def test_write_handles_empty_iterator(self):
        """Should handle empty iterator gracefully."""
        writer = PriceAlertWriter({})

        result = writer.write(iter([]))

        assert result.alerts_sent == 0
        assert result.regions_affected == []

    def test_write_counts_multiple_alerts(self):
        """Should count alerts correctly for multiple regions."""
        writer = PriceAlertWriter({
            "price_threshold": "100",
            "demand_threshold": "5000",
            "alert_mode": "log"
        })

        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", 500.0, 8000.0),  # 2 alerts
            (datetime.now(), "1", "VIC1", "1", "0", 200.0, 6000.0),  # 2 alerts
            (datetime.now(), "1", "QLD1", "1", "0", 50.0, 4000.0),   # 0 alerts
        ]

        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        assert result.alerts_sent == 4
        assert set(result.regions_affected) == {"NSW1", "VIC1"}

    def test_send_alert_log_mode(self):
        """Should log alerts in log mode."""
        writer = PriceAlertWriter({"alert_mode": "log"})

        alert = {
            "type": "PRICE_SPIKE",
            "region": "NSW1",
            "price": 500.0,
            "threshold": 300.0
        }

        with patch("nemweb_sink.logger") as mock_logger:
            writer._send_alert(alert)
            mock_logger.warning.assert_called_once()

    def test_send_alert_webhook_mode(self):
        """Should send webhook in webhook mode."""
        writer = PriceAlertWriter({
            "alert_mode": "webhook",
            "webhook_url": "https://example.com/webhook"
        })

        alert = {"type": "PRICE_SPIKE", "region": "NSW1"}

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_sink.urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            writer._send_alert(alert)
            mock_urlopen.assert_called_once()

    def test_get_field_safe_access(self):
        """Should safely access tuple fields."""
        writer = PriceAlertWriter({})

        row = ("value0", "value1", "value2")

        assert writer._get_field(row, "test", 0) == "value0"
        assert writer._get_field(row, "test", 1) == "value1"
        assert writer._get_field(row, "test", 10) is None  # Out of bounds

    def test_write_handles_malformed_rows(self):
        """Should handle rows with missing fields."""
        writer = PriceAlertWriter({"alert_mode": "log"})

        test_rows = [
            ("short", "row"),  # Too few fields
            (datetime.now(), "1", "NSW1"),  # Missing price/demand
        ]

        # Should not raise
        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        assert result.alerts_sent == 0


class TestPriceAlertDataSource:
    """Tests for PriceAlertDataSource class."""

    def test_data_source_name(self):
        """Should return 'nemweb_alerts' as format name."""
        assert PriceAlertDataSource.name() == "nemweb_alerts"

    def test_schema_returns_empty(self):
        """Writer schema should be empty string."""
        ds = PriceAlertDataSource.__new__(PriceAlertDataSource)
        assert ds.schema() == ""

    def test_writer_returns_writer_instance(self):
        """Should return a PriceAlertWriter."""
        ds = PriceAlertDataSource.__new__(PriceAlertDataSource)
        ds.options = {"price_threshold": "500"}

        schema = StructType([])
        writer = ds.writer(schema, overwrite=False)

        assert isinstance(writer, PriceAlertWriter)
        assert writer.price_threshold == 500.0


class TestMetricsWriter:
    """Tests for MetricsWriter class."""

    def test_writer_initialization(self):
        """Should initialize with options."""
        options = {
            "metrics_url": "http://influxdb:8086/write",
            "metrics_prefix": "nem",
            "batch_size": "50"
        }

        writer = MetricsWriter(options)

        assert writer.metrics_url == "http://influxdb:8086/write"
        assert writer.metrics_prefix == "nem"
        assert writer.batch_size == 50

    def test_writer_default_options(self):
        """Should use defaults for missing options."""
        writer = MetricsWriter({})

        assert writer.metrics_url is None
        assert writer.metrics_prefix == "nemweb"
        assert writer.batch_size == 100

    def test_to_nano_timestamp_from_datetime(self):
        """Should convert datetime to nanoseconds."""
        writer = MetricsWriter({})

        test_time = datetime(2024, 1, 15, 12, 30, 0)
        result = writer._to_nano_timestamp(test_time)

        # Should be nanoseconds since epoch
        expected = int(test_time.timestamp() * 1e9)
        assert result == expected

    def test_to_nano_timestamp_fallback(self):
        """Should return current time for non-datetime input."""
        writer = MetricsWriter({})

        result = writer._to_nano_timestamp("not a datetime")

        # Should be approximately now in nanoseconds
        now_ns = int(datetime.now().timestamp() * 1e9)
        # Allow 1 second tolerance
        assert abs(result - now_ns) < 1e9

    def test_write_formats_metrics(self):
        """Should format rows as InfluxDB line protocol."""
        writer = MetricsWriter({"metrics_prefix": "test"})

        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", 7500.0),
            (datetime.now(), "1", "VIC1", "1", "0", 5000.0),
        ]

        with patch.object(writer, '_flush_metrics') as mock_flush:
            writer.write(iter(test_rows))

            # Should have called flush with metrics
            assert mock_flush.called
            metrics = mock_flush.call_args[0][0]
            assert len(metrics) == 2
            assert "test_demand" in metrics[0]
            assert "region=NSW1" in metrics[0]

    def test_write_batches_metrics(self):
        """Should batch metrics according to batch_size."""
        writer = MetricsWriter({"batch_size": "2"})

        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", 7500.0),
            (datetime.now(), "1", "VIC1", "1", "0", 5000.0),
            (datetime.now(), "1", "QLD1", "1", "0", 6000.0),
        ]

        flush_calls = []

        def mock_flush(metrics):
            flush_calls.append(len(metrics))

        with patch.object(writer, '_flush_metrics', side_effect=mock_flush):
            writer.write(iter(test_rows))

        # Should flush batch of 2, then remaining 1
        assert flush_calls == [2, 1]

    def test_flush_metrics_without_url_logs(self):
        """Should log metrics when no URL configured."""
        writer = MetricsWriter({})  # No metrics_url

        metrics = ["test_metric,tag=value value=1.0 123456"]

        with patch("nemweb_sink.logger") as mock_logger:
            writer._flush_metrics(metrics)
            # Should log but not send HTTP request
            mock_logger.debug.assert_called()

    def test_flush_metrics_with_url(self):
        """Should POST metrics to URL when configured."""
        writer = MetricsWriter({"metrics_url": "http://example.com/write"})

        metrics = ["test_metric value=1.0"]

        mock_response = MagicMock()
        mock_response.status = 204
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("nemweb_sink.urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            writer._flush_metrics(metrics)

            mock_urlopen.assert_called_once()
            # Check that data was sent
            call_args = mock_urlopen.call_args
            request = call_args[0][0]
            assert request.get_full_url() == "http://example.com/write"


class TestMetricsDataSource:
    """Tests for MetricsDataSource class."""

    def test_data_source_name(self):
        """Should return 'nemweb_metrics' as format name."""
        assert MetricsDataSource.name() == "nemweb_metrics"

    def test_schema_returns_empty(self):
        """Writer schema should be empty string."""
        ds = MetricsDataSource.__new__(MetricsDataSource)
        assert ds.schema() == ""

    def test_writer_returns_writer_instance(self):
        """Should return a MetricsWriter."""
        ds = MetricsDataSource.__new__(MetricsDataSource)
        ds.options = {"metrics_prefix": "custom"}

        schema = StructType([])
        writer = ds.writer(schema, overwrite=False)

        assert isinstance(writer, MetricsWriter)
        assert writer.metrics_prefix == "custom"


class TestAlertCommitMessage:
    """Tests for AlertCommitMessage dataclass."""

    def test_commit_message_creation(self):
        """Should create commit message with all fields."""
        msg = AlertCommitMessage(
            partition_id=0,
            alerts_sent=5,
            regions_affected=["NSW1", "VIC1"]
        )

        assert msg.partition_id == 0
        assert msg.alerts_sent == 5
        assert msg.regions_affected == ["NSW1", "VIC1"]


class TestAlertThresholds:
    """Tests for alert threshold behavior."""

    def test_nem_market_cap_price(self):
        """Should handle NEM market cap prices ($15,500/MWh)."""
        writer = PriceAlertWriter({
            "price_threshold": "300",
            "alert_mode": "log"
        })

        test_rows = [
            (datetime.now(), "1", "SA1", "1", "0", 15500.0, 2000.0),  # Extreme price
        ]

        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        assert result.alerts_sent == 1
        assert "SA1" in result.regions_affected

    def test_negative_price(self):
        """Should not alert on negative prices."""
        writer = PriceAlertWriter({
            "price_threshold": "-100",  # Low threshold
            "alert_mode": "log"
        })

        test_rows = [
            (datetime.now(), "1", "NSW1", "1", "0", -50.0, 5000.0),  # Negative price
        ]

        with patch("nemweb_sink.logger"):
            result = writer.write(iter(test_rows))

        # -50 > -100, so should trigger
        assert result.alerts_sent == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
