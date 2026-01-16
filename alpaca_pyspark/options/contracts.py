"""PySpark DataSource for Alpaca's options contracts listing API."""

import ast
import logging
from datetime import date
from functools import cached_property
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from common import SymbolPartition
from ..common import (
    DEFAULT_DATA_ENDPOINT,
    DEFAULT_LIMIT,
    Symbols_Option_Type,
    build_page_fetcher,
    fetch_all_pages,
)

logger = logging.getLogger(__name__)

# default endpoint for options contracts
DEFAULT_OPTS_CNTRCTS_ENDPOINT = "https://paper-api.alpaca.markets/v2"

# Type alias for contract data tuple
ContractTuple = Tuple[
    str,  # id
    str,  # symbol
    str,  # name
    str,  # status
    bool,  # tradable
    date,  # expiration_date
    str,  # root_symbol
    str,  # underlying_symbol
    str,  # underlying_asset_id
    str,  # type
    str,  # style
    float,  # strike_price
    int,  # size
    Optional[int],  # open_interest
    Optional[date],  # open_interest_date
    Optional[float],  # close_price
    Optional[date],  # close_price_date
]

# Filter options that can be passed to the API
FILTER_OPTIONS = [
    "type",
    "strike_price_gte",
    "strike_price_lte",
    "expiration_date",
    "expiration_date_gte",
    "expiration_date_lte",
    "root_symbol",
    "status",
]


class OptionsContractsDataSource(DataSource):
    """PySpark DataSource for Alpaca's options contracts listing API.

    This data source fetches available option contracts filtered by underlying
    symbols and other criteria. Unlike bars/trades, this is a listing endpoint
    that returns contract metadata, not time-series data.

    Required options:
        - underlying_symbols: List of underlying symbols or string representation
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of contracts per API call (default: 10000)
        - type: Filter by contract type ("call" or "put")
        - strike_price_gte: Minimum strike price
        - strike_price_lte: Maximum strike price
        - expiration_date: Exact expiration date (YYYY-MM-DD)
        - expiration_date_gte: Minimum expiration date
        - expiration_date_lte: Maximum expiration date
        - root_symbol: Filter by root symbol
        - status: Contract status filter
    """

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)
        self._validate_options()

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Options_Contracts"

    def schema(self) -> Union[StructType, str]:
        return """
            id STRING,
            symbol STRING,
            name STRING,
            status STRING,
            tradable BOOLEAN,
            expiration_date DATE,
            root_symbol STRING,
            underlying_symbol STRING,
            underlying_asset_id STRING,
            type STRING,
            style STRING,
            strike_price DOUBLE,
            size INT,
            open_interest INT,
            open_interest_date DATE,
            close_price DOUBLE,
            close_price_date DATE
        """

    @property
    def pa_schema(self) -> pa.Schema:
        fields: Iterable[tuple[str, pa.DataType]] = [
            ("id", pa.string()),
            ("symbol", pa.string()),
            ("name", pa.string()),
            ("status", pa.string()),
            ("tradable", pa.bool_()),
            ("expiration_date", pa.date32()),
            ("root_symbol", pa.string()),
            ("underlying_symbol", pa.string()),
            ("underlying_asset_id", pa.string()),
            ("type", pa.string()),
            ("style", pa.string()),
            ("strike_price", pa.float64()),
            ("size", pa.int32()),
            ("open_interest", pa.int32()),
            ("open_interest_date", pa.date32()),
            ("close_price", pa.float64()),
            ("close_price_date", pa.date32()),
        ]
        return pa.schema(fields)

    def reader(self, schema: StructType) -> "OptionsContractsReader":
        return OptionsContractsReader(self.pa_schema, self.options)

    def _validate_options(self) -> None:
        """Validate that all required options are present and valid."""
        required_options = [
            "underlying_symbols",
            "APCA-API-KEY-ID",
            "APCA-API-SECRET-KEY",
        ]

        missing = [
            opt
            for opt in required_options
            if opt not in self.options or not self.options[opt]
        ]
        if missing:
            raise ValueError(f"Missing required options: {missing}")

        # Validate underlying_symbols format
        symbols: Symbols_Option_Type = self.options.get("underlying_symbols", [])
        if isinstance(symbols, str):
            try:
                parsed_symbols = ast.literal_eval(symbols)
                if not isinstance(parsed_symbols, (list, tuple)) or not parsed_symbols:
                    raise ValueError(
                        "underlying_symbols must be a non-empty list or tuple"
                    )
            except (ValueError, SyntaxError) as e:
                raise ValueError(
                    f"Invalid underlying_symbols format '{symbols}'. "
                    f"Must be a valid Python list/tuple string."
                ) from e
        elif isinstance(symbols, (list, tuple)):
            if not symbols:
                raise ValueError("underlying_symbols list cannot be empty")
        else:
            raise ValueError(
                f"underlying_symbols must be a list, tuple, "
                f"or string representation, got {type(symbols)}"
            )

        # Validate type option if provided
        type_opt = self.options.get("type")
        if type_opt is not None and type_opt not in ("call", "put"):
            raise ValueError(f"type must be 'call' or 'put', got '{type_opt}'")


class OptionsContractsReader(DataSourceReader):
    """Reader implementation for options contracts listing data source."""

    def __init__(self, pa_schema: pa.Schema, options: Dict[str, str]) -> None:
        super().__init__()
        self.pa_schema = pa_schema
        self.options = options

    @property
    def headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            "Content-Type": "application/json",
            "APCA-API-KEY-ID": self.options["APCA-API-KEY-ID"],
            "APCA-API-SECRET-KEY": self.options["APCA-API-SECRET-KEY"],
        }

    @property
    def endpoint(self) -> str:
        """Get API endpoint URL."""
        return self.options.get("endpoint", DEFAULT_OPTS_CNTRCTS_ENDPOINT)

    @cached_property
    def underlying_symbols(self) -> List[str]:
        """Get the list of underlying symbols to fetch contracts for."""
        symbols: Symbols_Option_Type = self.options.get("underlying_symbols", [])
        if isinstance(symbols, str):
            return list(ast.literal_eval(symbols))
        else:
            return list(symbols)

    @cached_property
    def limit(self) -> int:
        return int(self.options.get("limit", DEFAULT_LIMIT))

    @property
    def path_elements(self) -> List[str]:
        """URL path for options contracts endpoint."""
        return ["options", "contracts"]

    def partitions(self) -> Sequence[SymbolPartition]:
        """Create partitions for parallel processing, one per underlying symbol."""
        symbol_list = self.underlying_symbols
        if not symbol_list:
            raise ValueError("No underlying symbols provided for data fetching")
        return [SymbolPartition(symbol=sym) for sym in symbol_list]

    def api_params(self, partition: SymbolPartition) -> Dict[str, Any]:
        """Get API parameters for requests.

        Args:
            partition: the current partition

        Returns:
            API parameters for the current partition
        """
        params: Dict[str, Any] = {
            "underlying_symbols": partition.symbol,
            "limit": self.limit,
        }
        # Add optional filter parameters if provided
        for opt in FILTER_OPTIONS:
            if opt in self.options:
                params[opt] = self.options[opt]
        return params

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Read contracts for a single underlying symbol partition.

        Each API page is yielded as a single PyArrow RecordBatch.

        Args:
            partition: Symbol partition to read data for

        Yields:
            PyArrow RecordBatch objects, one per API page
        """
        if not isinstance(partition, SymbolPartition):
            raise ValueError(f"Expected SymbolPartition, got {type(partition)}")

        # Set up the page fetcher function
        get_page = build_page_fetcher(self.endpoint, self.headers, self.path_elements)

        # Get API parameters for this partition
        params = self.api_params(partition)

        # Get rate limit delay from options (default: 0.0, disabled)
        rate_limit_delay = float(self.options.get("rate_limit_delay", 0.0))

        # Process all pages of results
        for pg in fetch_all_pages(get_page, params, rate_limit_delay=rate_limit_delay):
            # Contracts are returned as a flat array under "option_contracts"
            if "option_contracts" in pg and pg["option_contracts"]:
                batch = self._parse_page_to_batch(pg["option_contracts"])
                if batch is not None:
                    yield batch

    def _parse_page_to_batch(
        self, contracts: List[Dict[str, Any]]
    ) -> Optional[pa.RecordBatch]:
        """Parse a page of contracts into a PyArrow RecordBatch.

        Args:
            contracts: List of contract dictionaries from API response

        Returns:
            PyArrow RecordBatch or None if no valid data
        """
        num_cols = len(self.pa_schema)
        buffer_size = 0
        col_buffer: List[List[Any]] = [[] for _ in range(num_cols)]

        for record in contracts:
            try:
                parsed = self._parse_record(record)
                for i in range(num_cols):
                    col_buffer[i].append(parsed[i])
                buffer_size += 1
            except ValueError as e:
                logger.warning(f"Skipping malformed contract record: {e}")
                continue

        if buffer_size > 0:
            parrays = [
                pa.array(col_buffer[i], type=self.pa_schema.field(i).type)
                for i in range(num_cols)
            ]
            return pa.RecordBatch.from_arrays(parrays, schema=self.pa_schema)
        return None

    def _parse_record(self, record: Dict[str, Any]) -> ContractTuple:
        """Parse a single contract from API response into tuple format.

        Args:
            record: Contract data dictionary from API response

        Returns:
            Tuple containing parsed contract data

        Raises:
            ValueError: If contract data is malformed or missing required fields
        """
        try:
            # Parse required date field
            expiration_date = date.fromisoformat(record["expiration_date"])

            # Parse optional date fields
            open_interest_date_str = record.get("open_interest_date")
            open_interest_date = (
                date.fromisoformat(open_interest_date_str)
                if open_interest_date_str
                else None
            )

            close_price_date_str = record.get("close_price_date")
            close_price_date = (
                date.fromisoformat(close_price_date_str)
                if close_price_date_str
                else None
            )

            # Parse optional numeric fields
            open_interest = record.get("open_interest")
            if open_interest is not None:
                open_interest = int(open_interest)

            close_price = record.get("close_price")
            if close_price is not None:
                close_price = float(close_price)

            return (
                str(record["id"]),
                record["symbol"],
                record["name"],
                record["status"],
                bool(record["tradable"]),
                expiration_date,
                record["root_symbol"],
                record["underlying_symbol"],
                str(record["underlying_asset_id"]),
                record["type"],
                record["style"],
                float(record["strike_price"]),
                int(record["size"]),
                open_interest,
                open_interest_date,
                close_price,
                close_price_date,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(
                f"Failed to parse contract data: {record}. Error: {e}"
            ) from e
