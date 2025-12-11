import ast
import logging
from datetime import datetime as dt
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from .common import build_page_fetcher, SymbolPartition

# Constants
DEFAULT_DATA_ENDPOINT = "https://data.alpaca.markets/v2"
DEFAULT_LIMIT = 1000
MAX_RETRIES = 3
RETRY_DELAY = 1.0

# Set up logger
logger = logging.getLogger(__name__)

#
# Historical Bars
#

Symbols_Option_Type = Union[str, List[str], Tuple[str, ...]]


class HistoricalBarsDataSource(DataSource):
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
        - limit: Maximum number of bars per API call (default: 1000)
    """

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)
        self._validate_options()

    def _validate_options(self) -> None:
        """Validate that all required options are present and valid."""
        required_options = ['symbols', 'APCA-API-KEY-ID', 'APCA-API-SECRET-KEY',
                            'timeframe', 'start', 'end']
        missing = [opt for opt in required_options if opt not in self.options or not self.options[opt]]
        if missing:
            raise ValueError(f"Missing required options: {missing}")

        # Validate symbols format
        symbols: Symbols_Option_Type = self.options.get('symbols', [])
        if isinstance(symbols, str):
            try:
                parsed_symbols = ast.literal_eval(symbols)
                if not isinstance(parsed_symbols, (list, tuple)) or not parsed_symbols:
                    raise ValueError("Symbols must be a non-empty list or tuple")
            except (ValueError, SyntaxError) as e:
                raise ValueError(f"Invalid symbols format '{symbols}'. "
                                 f"Must be a valid Python list/tuple string.") from e
        elif isinstance(symbols, (list, tuple)):
            if not symbols:
                raise ValueError("Symbols list cannot be empty")
        else:
            raise ValueError(f"Symbols must be a list, tuple, "
                             f"or string representation, got {type(symbols)}")

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

    def reader(self, schema: StructType) -> "DataSourceReader":
        return HistoricalBarsReader(schema, self.options)


class HistoricalBarsReader(DataSourceReader):
    """Reader implementation for historical bars data source."""

    def __init__(self, schema: StructType, options: Dict[str, str]) -> None:
        super().__init__()
        self.schema = schema
        self.options = options

    @property
    def _headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            'Content-Type': 'application/json',
            'APCA-API-KEY-ID': self.options['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.options['APCA-API-SECRET-KEY']
        }

    def _api_params(self) -> Dict[str, Any]:
        """Get base API parameters for requests."""
        return {
            "timeframe": self.options['timeframe'],
            "start": self.options['start'],
            "end": self.options['end'],
            "limit": int(self.options.get('limit', DEFAULT_LIMIT))
        }

    @property
    def endpoint(self) -> str:
        """Get API endpoint URL."""
        return self.options.get("endpoint", DEFAULT_DATA_ENDPOINT)

    @property
    def symbols(self) -> List[str]:
        """Get the list of symbols to fetch data for.

        Note: Symbol validation occurs in HistoricalBarsDataSource._validate_options()
        """
        symbols: Symbols_Option_Type = self.options.get("symbols", [])
        if isinstance(symbols, str):
            # Parse string representation (already validated in DataSource)
            return list(ast.literal_eval(symbols))
        else:
            # Already a list/tuple (already validated in DataSource)
            return list(symbols)

    def partitions(self) -> Sequence[SymbolPartition]:
        """Create partitions for parallel processing, one per symbol."""
        symbol_list = self.symbols
        if not symbol_list:
            raise ValueError("No symbols provided for data fetching")
        return [SymbolPartition(sym) for sym in symbol_list]

    def __parse_bar(self, sym: str, bar: Dict[str, Any]) -> \
            Tuple[str, dt, float, float, float, float, int, int, float]:
        """Parse a single bar from API response into tuple format.

        Args:
            sym: Stock symbol
            bar: Bar data dictionary from API response

        Returns:
            Tuple containing parsed bar data

        Raises:
            ValueError: If bar data is malformed or missing required fields
        """
        try:
            return (
                sym,
                dt.fromisoformat(bar["t"]),
                float(bar["o"]),
                float(bar["h"]),
                float(bar["l"]),
                float(bar["c"]),
                int(bar["v"]),
                int(bar["n"]),
                float(bar["vw"])
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse bar data for symbol {sym}: {bar}. Error: {e}") from e

    def read(self, partition: InputPartition) -> Iterator[Tuple[str, dt, float, float, float, float, int, int, float]]:
        """Read historical bars data for a single symbol partition.

        Args:
            partition: Symbol partition to read data for

        Yields:
            Tuples containing parsed bar data
        """
        # Ensure partition is SymbolPartition
        if not isinstance(partition, SymbolPartition):
            raise ValueError(f"Expected SymbolPartition, got {type(partition)}")

        # Set up the page fetcher function with enhanced error handling
        get_bars_page = build_page_fetcher(self.endpoint, self._headers, ["stocks", "bars"])
        # Our base params
        params = self._api_params()
        # Set the symbol from the partition
        params['symbols'] = partition.symbol

        # Configure session
        with requests.Session() as sess:
            # Tracking pages
            num_pages = 0
            next_page_token: Optional[str] = None

            # Cycle through pages
            while next_page_token or num_pages < 1:
                retry_count = 0
                while retry_count < MAX_RETRIES:
                    try:
                        # Get the page with retry logic
                        pg = get_bars_page(sess, params, next_page_token)
                        break  # Success, exit retry loop
                    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                        retry_count += 1
                        if retry_count >= MAX_RETRIES:
                            logger.error(
                                f"Failed to fetch data for symbol {partition.symbol} after {MAX_RETRIES} retries: {e}")
                            raise ValueError(
                                f"API request failed for symbol {partition.symbol} after {MAX_RETRIES} retries") from e

                        logger.warning(f"Retry {retry_count} for symbol {partition.symbol}: {e}")
                        sleep(RETRY_DELAY * retry_count)  # Exponential backoff

                # Process each bar
                if "bars" in pg and pg["bars"]:
                    bars = pg["bars"]
                    for sym in bars.keys():
                        for bar in bars[sym]:
                            try:
                                yield self.__parse_bar(sym, bar)
                            except ValueError as e:
                                logger.warning(f"Skipping malformed bar for {sym}: {e}")
                                continue

                # Go to next page
                num_pages += 1
                next_page_token = pg.get("next_page_token", None)
