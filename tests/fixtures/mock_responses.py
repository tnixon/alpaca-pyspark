"""Mock API response data for testing."""

from typing import Any, Dict

# Single page of bars data
MOCK_BARS_RESPONSE = {
    "bars": {
        "AAPL": [
            {
                "t": "2021-01-01T09:30:00Z",
                "o": 130.0,
                "h": 132.0,
                "l": 129.0,
                "c": 131.5,
                "v": 1000000,
                "n": 5000,
                "vw": 131.0,
            },
            {
                "t": "2021-01-01T10:30:00Z",
                "o": 131.5,
                "h": 133.0,
                "l": 131.0,
                "c": 132.5,
                "v": 1100000,
                "n": 5500,
                "vw": 132.0,
            },
        ]
    },
    "next_page_token": None,
}

# First page of paginated bars data
MOCK_BARS_RESPONSE_PAGE1 = {
    "bars": {
        "AAPL": [
            {
                "t": "2021-01-01T09:30:00Z",
                "o": 130.0,
                "h": 132.0,
                "l": 129.0,
                "c": 131.5,
                "v": 1000000,
                "n": 5000,
                "vw": 131.0,
            }
        ]
    },
    "next_page_token": "token123",
}

# Second page of paginated bars data
MOCK_BARS_RESPONSE_PAGE2 = {
    "bars": {
        "AAPL": [
            {
                "t": "2021-01-01T10:30:00Z",
                "o": 131.5,
                "h": 133.0,
                "l": 131.0,
                "c": 132.5,
                "v": 1100000,
                "n": 5500,
                "vw": 132.0,
            }
        ]
    },
    "next_page_token": None,
}

# Single page of trades data
MOCK_TRADES_RESPONSE = {
    "trades": {
        "AAPL": [
            {"t": "2021-01-01T09:30:00Z", "x": "V", "p": 131.0, "s": 100, "c": [], "i": 12345, "z": "C"},
            {"t": "2021-01-01T09:30:01Z", "x": "V", "p": 131.5, "s": 200, "c": ["@", "I"], "i": 12346, "z": "C"},
        ]
    },
    "next_page_token": None,
}

# Empty response
MOCK_EMPTY_RESPONSE: Dict[str, Any] = {"bars": {}, "next_page_token": None}

# Malformed bar record (missing required fields)
MOCK_MALFORMED_BAR = {
    "bars": {
        "AAPL": [
            {
                "t": "2021-01-01T09:30:00Z",
                "o": 130.0,
                # Missing h, l, c, v, n, vw fields
            }
        ]
    },
    "next_page_token": None,
}

# Single page of corporate actions data
MOCK_CORPORATE_ACTIONS_RESPONSE = {
    "corporate_actions": {
        "AAPL": [
            {
                "ex_date": "2021-02-05T00:00:00Z",
                "record_date": "2021-02-08T00:00:00Z",
                "payable_date": "2021-02-11T00:00:00Z",
                "type": "dividend",
                "amount": 0.205,
                "ratio": 1.0,
                "new_symbol": "",
                "old_symbol": "AAPL",
            },
            {
                "ex_date": "2021-08-30T00:00:00Z",
                "record_date": "2021-08-30T00:00:00Z",
                "payable_date": "2021-08-30T00:00:00Z",
                "type": "split",
                "amount": 0.0,
                "ratio": 4.0,
                "new_symbol": "AAPL",
                "old_symbol": "AAPL",
            },
        ]
    },
    "next_page_token": None,
}
