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

# Single page of options contracts data
MOCK_OPTIONS_CONTRACTS_RESPONSE = {
    "option_contracts": [
        {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            "name": "AAPL Jan 17 2025 150 Call",
            "status": "active",
            "tradable": True,
            "expiration_date": "2025-01-17",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
            "open_interest": 5000,
            "open_interest_date": "2025-01-10",
            "close_price": "5.25",
            "close_price_date": "2025-01-10",
        },
        {
            "id": "f6e5d4c3-b2a1-0987-fedc-ba0987654321",
            "symbol": "AAPL250117P00150000",
            "name": "AAPL Jan 17 2025 150 Put",
            "status": "active",
            "tradable": True,
            "expiration_date": "2025-01-17",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "put",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
            "open_interest": 3000,
            "open_interest_date": "2025-01-10",
            "close_price": "2.10",
            "close_price_date": "2025-01-10",
        },
    ],
    "next_page_token": None,
}

# Options contracts response with missing optional fields
MOCK_OPTIONS_CONTRACTS_MINIMAL = {
    "option_contracts": [
        {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            "name": "AAPL Jan 17 2025 150 Call",
            "status": "active",
            "tradable": True,
            "expiration_date": "2025-01-17",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
            # No open_interest, open_interest_date, close_price, close_price_date
        },
    ],
    "next_page_token": None,
}

# Empty options contracts response
MOCK_OPTIONS_CONTRACTS_EMPTY: Dict[str, Any] = {
    "option_contracts": [],
    "next_page_token": None,
}

# Malformed options contract (missing required fields)
MOCK_OPTIONS_CONTRACTS_MALFORMED = {
    "option_contracts": [
        {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            # Missing name, status, tradable, expiration_date, etc.
        },
    ],
    "next_page_token": None,
}
