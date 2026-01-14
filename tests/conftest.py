"""Root conftest.py with shared fixtures for all tests."""
import pytest
from datetime import datetime as dt
from zoneinfo import ZoneInfo


@pytest.fixture
def sample_symbols():
    """Sample stock symbols for testing."""
    return ["AAPL", "MSFT", "GOOG"]


@pytest.fixture
def sample_date_range():
    """Sample date range for testing."""
    tz = ZoneInfo("America/New_York")
    return {
        "start": dt(2021, 1, 1, tzinfo=tz),
        "end": dt(2021, 1, 5, tzinfo=tz),
    }


@pytest.fixture
def api_credentials():
    """Mock API credentials for testing."""
    return {
        "APCA-API-KEY-ID": "test-key-id",
        "APCA-API-SECRET-KEY": "test-secret-key",
    }


@pytest.fixture
def base_options(sample_symbols, sample_date_range, api_credentials):
    """Base options dict for DataSource testing."""
    return {
        "symbols": sample_symbols,
        "start": sample_date_range["start"].isoformat(),
        "end": sample_date_range["end"].isoformat(),
        **api_credentials,
    }
