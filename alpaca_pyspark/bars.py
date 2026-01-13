import logging
import math
import re
from datetime import datetime as dt, timedelta as td
from enum import Enum
from functools import cached_property
from typing import Any, Dict, Iterable, List, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import StructType

from .common import (
    BaseAlpacaDataSource,
    BaseAlpacaReader,
    SymbolTimeRangePartition,
)

# Set up logger
logger = logging.getLogger(__name__)

# Constants
PAGES_PER_PARTITION = 5

# Type alias for bar data tuple: symbol, time, open, high, low, close, volume, trade_count, vwap
BarTuple = Tuple[str, dt, float, float, float, float, int, int, float]


class TimeUnit(Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    @classmethod
    def _missing_(cls, value):
        # expecting a string
        if not isinstance(value, str):
            value = str(value)
        # treat as case-invariant
        value = value.lower()
        # Remove trailing 's' for plural forms
        if value.endswith('s'):
            value = value[:-1]
        # Map alternate representations
        alt_map = {
            "min": cls.MINUTE,
            "minute": cls.MINUTE,
            "t": cls.MINUTE,
            "h": cls.HOUR,
            "hour": cls.HOUR,
            "d": cls.DAY,
            "day": cls.DAY,
            "w": cls.WEEK,
            "week": cls.WEEK,
            "m": cls.MONTH,
            "month": cls.MONTH,
        }
        if value in alt_map:
            return alt_map[value]
        raise ValueError(f"Unknown time unit: {value}")


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
        return ["timeframe"]

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
        fields: Iterable[tuple[str, pa.DataType]] = [
            ("symbol", pa.string()),
            ("time", pa.timestamp("us", tz="UTC")),
            ("open", pa.float32()),
            ("high", pa.float32()),
            ("low", pa.float32()),
            ("close", pa.float32()),
            ("volume", pa.int32()),
            ("trade_count", pa.int32()),
            ("vwap", pa.float32()),
        ]
        return pa.schema(fields)

    def reader(self, schema: StructType) -> "HistoricalBarsReader":
        return HistoricalBarsReader(self.pa_schema, self.options)


class HistoricalBarsReader(BaseAlpacaReader):
    """Reader implementation for historical bars data source."""

    def api_params(self, partition: SymbolTimeRangePartition) -> Dict[str, Any]:
        params = super().api_params(partition)
        params["timeframe"] = self.options["timeframe"]
        return params

    @property
    def data_key(self) -> str:
        """Bars data is returned under the 'bars' key."""
        return "bars"

    @property
    def path_elements(self) -> List[str]:
        """URL path for bars endpoint."""
        return ["stocks", "bars"]

    @cached_property
    def timeframe(self) -> td:
        tf = self.options.get("timeframe", "")
        match = re.match(r"^(\d+)([A-Za-z]+)(s?)$", tf)
        if not match:
            raise ValueError(f"Invalid timeframe format: {tf}")
        number = int(match.group(1))
        unit = TimeUnit(match.group(2))

        if unit == TimeUnit.MINUTE:
            return td(minutes=number)
        elif unit == TimeUnit.HOUR:
            return td(hours=number)
        elif unit == TimeUnit.DAY:
            return td(days=number)
        elif unit == TimeUnit.WEEK:
            # approximate a trading week as 5 days
            return td(days=(5 * number))
        elif unit == TimeUnit.MONTH:
            # Approximate a trading month as 20 days
            return td(days=(20 * number))
        else:
            raise ValueError(f"Unsupported TimeUnit: {unit}")

    @property
    def partition_interval(self) -> td:
        range_td = self.end - self.start
        num_intervals = max(1, math.ceil((range_td/self.timeframe) / (self.limit * PAGES_PER_PARTITION)))
        return range_td / num_intervals

    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> BarTuple:
        """Parse a single bar from API response into tuple format.

        Args:
            symbol: Stock symbol
            record: Bar data dictionary from API response

        Returns:
            Tuple containing parsed bar data (symbol, time, open, high, low, close, volume, trade_count, vwap)

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
                float(record["vw"]),
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse bar data for symbol {symbol}: {record}. Error: {e}") from e
