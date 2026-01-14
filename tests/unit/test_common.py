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
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {"symbol": "AAPL", "limit": 1000}
        )

        assert "https://api.example.com/stocks/bars?" in url
        assert "symbol=AAPL" in url
        assert "limit=1000" in url

    def test_url_with_none_params(self):
        """Test that None parameters are excluded from query string."""
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {"symbol": "AAPL", "page_token": None, "limit": 1000}
        )

        assert "symbol=AAPL" in url
        assert "limit=1000" in url
        assert "page_token" not in url

    def test_url_encoding_special_characters(self):
        """Test that special characters are properly encoded."""
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {"symbol": "BRK.B"}
        )

        # Either encoded or unencoded dot is acceptable
        assert "BRK.B" in url or "BRK%2EB" in url

    def test_url_with_multiple_params(self):
        """Test URL with multiple query parameters."""
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {
                "symbol": "AAPL",
                "start": "2021-01-01",
                "end": "2021-01-31",
                "limit": 5000
            }
        )

        assert "symbol=AAPL" in url
        assert "start=2021-01-01" in url
        assert "end=2021-01-31" in url
        assert "limit=5000" in url

    def test_url_with_empty_params(self):
        """Test URL building with empty parameters dict."""
        url = build_url(
            "https://api.example.com",
            ["stocks", "bars"],
            {}
        )

        assert url == "https://api.example.com/stocks/bars?"

    def test_url_with_nested_path(self):
        """Test URL with multiple path elements."""
        url = build_url(
            "https://api.example.com/v2",
            ["stocks", "bars", "latest"],
            {"symbol": "AAPL"}
        )

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
