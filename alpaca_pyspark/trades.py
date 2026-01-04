import logging
from datetime import datetime as dt
from typing import Any, Dict, List, Tuple, Union, Iterable

import pyarrow as pa
from pyspark.sql.types import StructType

from .common import (
    BaseAlpacaDataSource,
    BaseAlpacaReader,
    DEFAULT_LIMIT,
)

# Set up logger
logger = logging.getLogger(__name__)

# Type alias for trade data tuple: symbol, time, exchange, price, size, conditions, id, tape
TradeTuple = Tuple[str, dt, str, float, int, str, int, str]


class HistoricalTradesDataSource(BaseAlpacaDataSource):
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
    """

    @classmethod
    def name(cls) -> str:
        return "Alpaca_HistoricalTrades"

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

    def reader(self, schema: StructType) -> "HistoricalTradesReader":
        return HistoricalTradesReader(self.pa_schema, self.options)


class HistoricalTradesReader(BaseAlpacaReader):
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
