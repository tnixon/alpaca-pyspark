"""Unit test specific fixtures."""
import pytest
import responses


@pytest.fixture
def mock_alpaca_api():
    """Mock Alpaca API responses for unit tests.

    Uses the responses library to intercept HTTP requests and return mock data.
    Callers can add custom responses using the yielded RequestsMock object.

    Example:
        def test_something(mock_alpaca_api):
            mock_alpaca_api.add(
                responses.GET,
                "https://data.alpaca.markets/v2/stocks/bars",
                json={"bars": {...}},
                status=200
            )
            # Test code here
    """
    with responses.RequestsMock() as rsps:
        yield rsps
