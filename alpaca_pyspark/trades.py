import logging
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import StructType

from .common import (
    BaseAlpacaDataSource,
    BaseAlpacaReader,
    DEFAULT_LIMIT,
)

# Set up logger
logger = logging.getLogger(__name__)


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

    def reader(self, schema: StructType) -> "HistoricalTradesReader":
        return HistoricalTradesReader(schema, self.options)


class HistoricalTradesReader(BaseAlpacaReader):
    """Reader implementation for historical trades data source."""

    def _api_params(self) -> Dict[str, Any]:
        """Get API parameters for trades requests."""
        return {
            "start": self.options['start'],
            "end": self.options['end'],
            "limit": int(self.options.get('limit', DEFAULT_LIMIT))
        }

    def _get_data_key(self) -> str:
        """Trades data is returned under the 'trades' key."""
        return "trades"

    def _get_path_elements(self) -> List[str]:
        """URL path for trades endpoint."""
        return ["stocks", "trades"]

    @property
    def pyarrow_type(self) -> pa.Schema:
        """Return PyArrow schema for trades data."""
        cols: List[tuple[str, pa.DataType]] = [("symbol", pa.string()), ("time", pa.timestamp('us')),
                                               ("exchange", pa.string()), ("price", pa.float32()), ("size", pa.int32()),
                                               ("conditions", pa.string()), ("id", pa.int64()), ("tape", pa.string())]
        return pa.schema(cols)

    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> \
            Tuple[str, dt, str, float, int, str, int, str]:
        """Parse a single trade from API response into tuple format.

        Args:
            symbol: Stock symbol
            record: Trade data dictionary from API response

        Returns:
            Tuple containing parsed trade data

        Raises:
            ValueError: If trade data is malformed or missing required fields
        """
        try:
            # Conditions is a list of strings, join them
            conditions = ','.join(record.get("c", []))
            return (
                symbol, dt.fromisoformat(record["t"]), record["x"], float(record["p"]), int(record["s"]), conditions,
                int(record["i"]), record["z"]
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse trade data for symbol {symbol}: {record}. Error: {e}") from e

    def _create_record_batch(
        self, symbols: List[str], times: List[dt], exchanges: List[str], prices: List[float], sizes: List[int],
        conditions: List[str], ids: List[int], tapes: List[str]
    ) -> pa.RecordBatch:
        """Create a PyArrow RecordBatch from accumulated trade data."""
        return pa.RecordBatch.from_arrays([
            pa.array(symbols, type=pa.string()),
            pa.array(times, type=pa.timestamp('us')),
            pa.array(exchanges, type=pa.string()),
            pa.array(prices, type=pa.float32()),
            pa.array(sizes, type=pa.int32()),
            pa.array(conditions, type=pa.string()),
            pa.array(ids, type=pa.int64()),
            pa.array(tapes, type=pa.string())
        ],
                                          schema=self.pyarrow_type)

    def _parse_page_to_batch(self, data: Dict[str, List[Dict[str, Any]]], symbol: str) -> Optional[pa.RecordBatch]:
        """Parse a page of trades data into a PyArrow RecordBatch."""
        symbols: List[str] = []
        times: List[dt] = []
        exchanges: List[str] = []
        prices: List[float] = []
        sizes: List[int] = []
        conditions: List[str] = []
        ids: List[int] = []
        tapes: List[str] = []

        for sym in data.keys():
            for trade in data[sym]:
                try:
                    parsed = self._parse_record(sym, trade)
                    symbols.append(parsed[0])
                    times.append(parsed[1])
                    exchanges.append(parsed[2])
                    prices.append(parsed[3])
                    sizes.append(parsed[4])
                    conditions.append(parsed[5])
                    ids.append(parsed[6])
                    tapes.append(parsed[7])
                except ValueError as e:
                    logger.warning(f"Skipping malformed trade for {sym}: {e}")
                    continue

        if symbols:
            return self._create_record_batch(symbols, times, exchanges, prices, sizes, conditions, ids, tapes)
        return None
