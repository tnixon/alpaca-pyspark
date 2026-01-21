"""Tests for alpaca_pyspark/common.py - core utilities and base classes."""

import pytest
from datetime import datetime as dt
from alpaca_pyspark.common import (
    build_url,
    retriable_session,
    SymbolTimeRangePartition,
)


@pytest.mark.unit
class TestBuildUrl:
    """Tests for build_url function."""

    def test_simple_url(self):
        """Test URL building with basic parameters."""
        url = build_url("https://api.example.com", ["stocks", "bars"], {"symbol": "AAPL", "limit": 1000})

        assert "https://api.example.com/stocks/bars?" in url
        assert "symbol=AAPL" in url
        assert "limit=1000" in url

    def test_url_with_none_params(self):
        """Test that None parameters are excluded from query string."""
        url = build_url(
            "https://api.example.com", ["stocks", "bars"], {"symbol": "AAPL", "page_token": None, "limit": 1000}
        )

        assert "symbol=AAPL" in url
        assert "limit=1000" in url
        assert "page_token" not in url

    def test_url_encoding_special_characters(self):
        """Test that special characters are properly encoded."""
        url = build_url("https://api.example.com", ["stocks", "bars"], {"symbol": "BRK.B"})

        # Either encoded or unencoded dot is acceptable
        assert "BRK.B" in url or "BRK%2EB" in url

    def test_url_with_multiple_params(self):
        """Test URL with multiple query parameters."""
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {"symbol": "AAPL", "start": "2021-01-01", "end": "2021-01-31", "limit": 5000},
        )

        assert "symbol=AAPL" in url
        assert "start=2021-01-01" in url
        assert "end=2021-01-31" in url
        assert "limit=5000" in url

    def test_url_with_empty_params(self):
        """Test URL building with empty parameters dict."""
        url = build_url("https://api.example.com", ["stocks", "bars"], {})

        assert url == "https://api.example.com/stocks/bars?"

    def test_url_with_nested_path(self):
        """Test URL with multiple path elements."""
        url = build_url("https://api.example.com/v2", ["stocks", "bars", "latest"], {"symbol": "AAPL"})

        assert "https://api.example.com/v2/stocks/bars/latest?" in url
        assert "symbol=AAPL" in url


@pytest.mark.unit
class TestSymbolTimeRangePartition:
    """Tests for SymbolTimeRangePartition dataclass."""

    def test_partition_creation(self):
        """Test creating a partition with all fields."""
        start = dt(2021, 1, 1)
        end = dt(2021, 1, 2)

        partition = SymbolTimeRangePartition("AAPL", start, end)

        assert partition.symbol == "AAPL"
        assert partition.start == start
        assert partition.end == end

    def test_partition_attribute_access(self):
        """Test accessing partition attributes."""
        start = dt(2021, 1, 1, 9, 30)
        end = dt(2021, 1, 1, 16, 0)

        partition = SymbolTimeRangePartition("MSFT", start, end)

        # Test all attributes are accessible
        assert isinstance(partition.symbol, str)
        assert isinstance(partition.start, dt)
        assert isinstance(partition.end, dt)

    def test_partition_equality(self):
        """Test that partitions with same values are equal."""
        start = dt(2021, 1, 1)
        end = dt(2021, 1, 2)

        partition1 = SymbolTimeRangePartition("AAPL", start, end)
        partition2 = SymbolTimeRangePartition("AAPL", start, end)

        assert partition1 == partition2

    def test_partition_inequality(self):
        """Test that partitions with different values are not equal."""
        start = dt(2021, 1, 1)
        end = dt(2021, 1, 2)

        partition1 = SymbolTimeRangePartition("AAPL", start, end)
        partition2 = SymbolTimeRangePartition("MSFT", start, end)

        assert partition1 != partition2


@pytest.mark.unit
class TestRetriableSession:
    """Tests for retriable_session function."""

    def test_default_retry_count(self):
        """Test retriable session with default retry count."""
        session = retriable_session()

        # Verify session is created
        assert session is not None

        # Verify adapter has retry configuration
        adapter = session.get_adapter("https://")
        assert adapter is not None
        assert adapter.max_retries.total == 3  # MAX_RETRIES constant

    def test_custom_retry_count(self):
        """Test retriable session with custom retry count."""
        session = retriable_session(num_retries=5)

        adapter = session.get_adapter("https://")
        assert adapter.max_retries.total == 5

    def test_session_type(self):
        """Test that retriable_session returns a Session object."""
        from requests import Session

        session = retriable_session()
        assert isinstance(session, Session)


@pytest.mark.unit
class TestOptionalApiParameters:
    """Tests for optional API parameter functionality."""

    def test_unknown_options_produce_warning(self, caplog):
        """Test that unknown options produce a warning message."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsDataSource
        import logging

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
            "unknown_option": "some_value",
        }

        with caplog.at_level(logging.WARNING):
            HistoricalStockBarsDataSource(options)

        assert "Unknown option 'unknown_option' provided" in caplog.text

    def test_multiple_unknown_options_produce_warnings(self, caplog):
        """Test that multiple unknown options each produce a warning."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsDataSource
        import logging

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
            "foo": "bar",
            "baz": "qux",
        }

        with caplog.at_level(logging.WARNING):
            HistoricalStockBarsDataSource(options)

        assert "Unknown option 'foo' provided" in caplog.text
        assert "Unknown option 'baz' provided" in caplog.text

    def test_known_options_no_warning(self, caplog):
        """Test that known options do not produce warnings."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsDataSource
        import logging

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
            "adjustment": "raw",
            "feed": "sip",
        }

        with caplog.at_level(logging.WARNING):
            HistoricalStockBarsDataSource(options)

        assert "Unknown option" not in caplog.text

    def test_optional_params_included_in_api_params(self):
        """Test that optional params are included in api_params when present."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsReader
        import pyarrow as pa

        # Minimal schema for testing
        schema = pa.schema(
            [
                ("symbol", pa.string()),
                ("time", pa.timestamp("us", tz="UTC")),
                ("open", pa.float32()),
                ("high", pa.float32()),
                ("low", pa.float32()),
                ("close", pa.float32()),
                ("volume", pa.int32()),
                ("trade_count", pa.int32()),
                ("vwap", pa.float32()),
            ]
        )

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
            "adjustment": "split",
            "feed": "iex",
        }

        reader = HistoricalStockBarsReader(schema, options)
        partition = SymbolTimeRangePartition("AAPL", dt(2021, 1, 1), dt(2021, 1, 2))

        params = reader.api_params(partition)

        assert params["adjustment"] == "split"
        assert params["feed"] == "iex"

    def test_optional_params_not_included_when_absent(self):
        """Test that optional params are NOT included in api_params when not provided."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsReader
        import pyarrow as pa

        schema = pa.schema(
            [
                ("symbol", pa.string()),
                ("time", pa.timestamp("us", tz="UTC")),
                ("open", pa.float32()),
                ("high", pa.float32()),
                ("low", pa.float32()),
                ("close", pa.float32()),
                ("volume", pa.int32()),
                ("trade_count", pa.int32()),
                ("vwap", pa.float32()),
            ]
        )

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
        }

        reader = HistoricalStockBarsReader(schema, options)
        partition = SymbolTimeRangePartition("AAPL", dt(2021, 1, 1), dt(2021, 1, 2))

        params = reader.api_params(partition)

        assert "adjustment" not in params
        assert "feed" not in params
        assert "currency" not in params

    def test_bars_optional_params_list(self):
        """Test that AbstractBarsDataSource has the expected optional params."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsDataSource

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
        }

        datasource = HistoricalStockBarsDataSource(options)
        optional_params = datasource._optional_api_parameters()

        assert "adjustment" in optional_params
        assert "feed" in optional_params
        assert "currency" in optional_params
        assert "asof" in optional_params
        assert "sort" in optional_params

    def test_trades_optional_params_list(self):
        """Test that StockTradesDataSource has the expected optional params."""
        from alpaca_pyspark.stocks.trades import StockTradesDataSource

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
        }

        datasource = StockTradesDataSource(options)
        optional_params = datasource._optional_api_parameters()

        assert "feed" in optional_params
        assert "currency" in optional_params
        assert "sort" in optional_params
        # Trades should NOT have bars-specific params
        assert "adjustment" not in optional_params
        assert "asof" not in optional_params

    def test_internal_options_not_in_api_params(self):
        """Test that internal options are recognized but not passed to API."""
        from alpaca_pyspark.stocks.bars import HistoricalStockBarsDataSource

        options = {
            "symbols": "['AAPL']",
            "APCA-API-KEY-ID": "test-key",
            "APCA-API-SECRET-KEY": "test-secret",
            "start": "2021-01-01",
            "end": "2021-01-02",
            "timeframe": "1Day",
            "endpoint": "https://custom.endpoint.com",
            "limit": "5000",
            "rate_limit_delay": "0.5",
        }

        # Should not produce any unknown option warnings
        import logging as log_module

        with pytest.MonkeyPatch().context() as mp:
            warnings = []
            original_warning = log_module.Logger.warning

            def capture_warning(self, msg, *args, **kwargs):
                warnings.append(msg)
                return original_warning(self, msg, *args, **kwargs)

            mp.setattr(log_module.Logger, "warning", capture_warning)

            HistoricalStockBarsDataSource(options)

            # Filter for unknown option warnings
            unknown_warnings = [w for w in warnings if "Unknown option" in str(w)]
            assert len(unknown_warnings) == 0
