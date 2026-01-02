import logging
from datetime import datetime as dt
from typing import Any, Dict, List, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import StructType

from .common import (
    BaseAlpacaDataSource,
    BaseAlpacaReader,
    DEFAULT_LIMIT,
)

# Set up logger
logger = logging.getLogger(__name__)


class HistoricalBarsDataSource(BaseAlpacaDataSource):
    """PySpark DataSource for Alpaca's historical bars data.

    Required options:
        - symbols: List of stock symbols or string representation of list
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key
        - timeframe: Time frame for bars (e.g., '1Day', '1Hour')
        - start: Start date/time (ISO format)
        - end: End date/time (ISO format)

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of bars per API call (default: 10000)
    """

    def _additional_required_options(self) -> List[str]:
        """Bars require the 'timeframe' option."""
        return ['timeframe']

    @classmethod
    def name(cls) -> str:
        return "Alpaca_HistoricalBars"

    def schema(self) -> Union[StructType, str]:
        return """
            symbol STRING,
            time TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            trade_count INT,
            vwap FLOAT
        """

    @property
    def pa_schema(self) -> pa.Schema:
        return pa.schema([
            ("symbol", pa.string()),
            ("time", pa.timestamp("us", tz="UTC")),
            ("open", pa.float32()),
            ("high", pa.float32()),
            ("low", pa.float32()),
            ("close", pa.float32()),
            ("volume", pa.int32()),
            ("trade_count", pa.int32()),
            ("vwap", pa.float32())
        ])

    def reader(self, schema: StructType) -> "HistoricalBarsReader":
        return HistoricalBarsReader(self.pa_schema, self.options)


class HistoricalBarsReader(BaseAlpacaReader):
    """Reader implementation for historical bars data source."""

    @property
    def api_params(self) -> Dict[str, Any]:
        """Get API parameters for bars requests."""
        return {
            "timeframe": self.options['timeframe'],
            "start": self.options['start'],
            "end": self.options['end'],
            "limit": int(self.options.get('limit', DEFAULT_LIMIT))
        }

    @property
    def data_key(self) -> str:
        """Bars data is returned under the 'bars' key."""
        return "bars"

    @property
    def path_elements(self) -> List[str]:
        """URL path for bars endpoint."""
        return ["stocks", "bars"]

    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> \
            Tuple[str, dt, float, float, float, float, int, int, float]:
        """Parse a single bar from API response into tuple format.

        Args:
            symbol: Stock symbol
            record: Bar data dictionary from API response

        Returns:
            Tuple containing parsed bar data

        Raises:
            ValueError: If bar data is malformed or missing required fields
        """
        try:
            return (
                symbol,
                dt.fromisoformat(record["t"]),
                float(record["o"]),
                float(record["h"]),
                float(record["l"]),
                float(record["c"]),
                int(record["v"]),
                int(record["n"]),
                float(record["vw"])
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse bar data for symbol {symbol}: {record}. Error: {e}") from e
