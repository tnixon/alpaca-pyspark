import logging
from datetime import datetime as dt
from typing import Any, Dict, Iterable, List, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import StructType

from ..common import (
    ApiParam,
    BaseAlpacaDataSource,
    BaseAlpacaReader,
)

# Set up logger
logger = logging.getLogger(__name__)

# Valid values for enum-like parameters
VALID_SORT_VALUES = ("asc", "desc")
VALID_FEED_VALUES = ("iex", "sip", "delayed_sip", "otc")

# Type alias for trade data tuple: symbol, time, exchange, price, size, conditions, id, tape
TradeTuple = Tuple[str, dt, str, float, int, str, int, str]


class StockTradesDataSource(BaseAlpacaDataSource):
    """PySpark DataSource for Alpaca's historical trades data.

    Required options:
        - symbols: List of stock symbols or string representation of list
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key
        - start: Start date/time (ISO format)
        - end: End date/time (ISO format)

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of trades per API call (default: 10000)
        - sort: Sort order for results ('asc' or 'desc', default: 'asc')
        - feed: Data feed source ('iex', 'sip', 'delayed_sip', 'otc')
        - currency: Currency for prices in ISO 4217 format (default: 'USD')
    """

    @property
    def api_params(self) -> List[ApiParam]:
        """Stock trades API parameters including stock-specific options."""
        return super().api_params + [
            ApiParam("sort", False),
            ApiParam("feed", False),
            ApiParam("currency", False),
        ]

    def _validate_params(self, options: Dict[str, str]) -> Dict[str, str]:
        """Validate stock-specific parameters."""
        # Validate sort parameter
        sort = options.get("sort", "").lower()
        if sort and sort not in VALID_SORT_VALUES:
            raise ValueError(f"Invalid 'sort' value: '{sort}'. Must be one of: {VALID_SORT_VALUES}")

        # Validate feed parameter
        feed = options.get("feed", "").lower()
        if feed and feed not in VALID_FEED_VALUES:
            raise ValueError(f"Invalid 'feed' value: '{feed}'. Must be one of: {VALID_FEED_VALUES}")

        return super()._validate_params(options)

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Stocks_Trades"

    def schema(self) -> Union[StructType, str]:
        return """
            symbol STRING,
            time TIMESTAMP,
            exchange STRING,
            price FLOAT,
            size INT,
            conditions STRING,
            id BIGINT,
            tape STRING
        """

    @property
    def pa_schema(self) -> pa.Schema:
        fields: Iterable[tuple[str, pa.DataType]] = [
            ("symbol", pa.string()),
            ("time", pa.timestamp("us", tz="UTC")),
            ("exchange", pa.string()),
            ("price", pa.float32()),
            ("size", pa.int32()),
            ("conditions", pa.string()),
            ("id", pa.int64()),
            ("tape", pa.string()),
        ]
        return pa.schema(fields)

    def reader(self, schema: StructType) -> "HistoricalStockTradesReader":
        return HistoricalStockTradesReader(self.config, self.pa_schema, self.params)


class HistoricalStockTradesReader(BaseAlpacaReader):
    """Reader implementation for historical trades data source."""

    @property
    def data_key(self) -> str:
        """Trades data is returned under the 'trades' key."""
        return "trades"

    @property
    def path_elements(self) -> List[str]:
        """URL path for trades endpoint."""
        return ["stocks", "trades"]

    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> TradeTuple:
        """Parse a single trade from API response into tuple format.

        Args:
            symbol: Stock symbol
            record: Trade data dictionary from API response

        Returns:
            Tuple containing parsed trade data (symbol, time, exchange, price, size, conditions, id, tape)

        Raises:
            ValueError: If trade data is malformed or missing required fields
        """
        try:
            # Conditions is a list of strings, join them
            conditions = ",".join(record.get("c", []))
            return (
                symbol,
                dt.fromisoformat(record["t"]),
                record["x"],
                float(record["p"]),
                int(record["s"]),
                conditions,
                int(record["i"]),
                record["z"],
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse trade data for symbol {symbol}: {record}. Error: {e}") from e
