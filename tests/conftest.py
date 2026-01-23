"""Root conftest.py with shared fixtures for all tests."""

import pytest
import responses
from datetime import datetime as dt
from zoneinfo import ZoneInfo

from alpaca_pyspark.common import EndpointConfig
from alpaca_pyspark.corp_actions.corporate_actions import CorporateActionsReader


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


@pytest.fixture
def mock_alpaca_api():
    """Mock HTTP adapter for Alpaca API calls."""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def sample_corp_actions_reader(api_credentials, sample_symbols, sample_date_range):
    """Sample corporate actions reader for testing."""
    config = EndpointConfig(
        api_key_id=api_credentials["APCA-API-KEY-ID"],
        api_key_secret=api_credentials["APCA-API-SECRET-KEY"],
        endpoint="https://data.alpaca.markets/v2",
        rate_limit_delay=0.0,
    )
    
    params = {
        "symbols": sample_symbols,
        "start": sample_date_range["start"].isoformat(),
        "end": sample_date_range["end"].isoformat(),
        "limit": "10000",
    }
    
    # Create a dummy DataSource to get the schema
    from alpaca_pyspark.corp_actions.corporate_actions import CorporateActionsDataSource
    datasource = CorporateActionsDataSource({
        "symbols": sample_symbols,
        "start": sample_date_range["start"].isoformat(),
        "end": sample_date_range["end"].isoformat(),
        **api_credentials,
    })
    
    return CorporateActionsReader(config, datasource.pa_schema, params)
