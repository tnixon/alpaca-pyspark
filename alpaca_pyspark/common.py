import ast
import logging
import urllib.parse as urlp
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pyarrow as pa
import requests
from requests import HTTPError, RequestException, Session

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from pyspark.sql.pandas.types import to_arrow_schema

logger = logging.getLogger(__name__)

# Common constants
DEFAULT_DATA_ENDPOINT = "https://data.alpaca.markets/v2"
DEFAULT_LIMIT = 10000
MAX_RETRIES = 3
RETRY_DELAY = 1.0

# Type alias for symbols option
Symbols_Option_Type = Union[str, List[str], Tuple[str, ...]]


@dataclass
class SymbolPartition(InputPartition):
    """Partition representing a single stock symbol for parallel processing."""
    symbol: str


def build_url(endpoint: str, path_elements: List[str], params: Dict[str, Any]) -> str:
    """Build a properly encoded URL from components.

    Args:
        endpoint: Base API endpoint URL
        path_elements: List of path segments to join
        params: Query parameters dictionary

    Returns:
        Complete URL with properly encoded parameters
    """
    # Build URL path
    path = "/".join(path_elements)

    # Convert all param values to strings and handle None values
    param_pairs = []
    for k, v in params.items():
        if v is not None:
            # Optimize common type conversions
            if isinstance(v, (int, float, bool)):
                str_v = str(v)
            else:
                str_v = str(v)
            # URL encode the value
            quoted_v = urlp.quote(str_v)
            param_pairs.append(f"{k}={quoted_v}")

    param_str = "&".join(param_pairs)

    # Assemble the final URL
    return f"{endpoint}/{path}?{param_str}"


def build_page_fetcher(endpoint: str, headers: Dict[str, str], path_elements: List[str]):
    """Build a page fetcher function with enhanced error handling.

    Args:
        endpoint: Base API endpoint
        headers: HTTP headers for requests
        path_elements: URL path segments

    Returns:
        Function that fetches a single page of data
    """

    def get_page(sess: Session, params: Dict[str, Any], page_token: Optional[str]) -> Dict[str, Any]:
        """Fetch a single page of data from the API.

        Args:
            sess: HTTP session to use
            params: Query parameters
            page_token: Optional pagination token

        Returns:
            JSON response as dictionary

        Raises:
            HTTPError: For HTTP error responses
            RequestException: For network/connection errors
        """
        # Make a copy to avoid modifying the original params
        request_params = params.copy()

        # Append page token if it exists
        if page_token:
            request_params['page_token'] = page_token

        try:
            url = build_url(endpoint, path_elements, request_params)
            response = sess.get(url, headers=headers, timeout=(10.0, 30.0))

            if not response.ok:
                raise HTTPError(f"HTTP error {response.status_code} for {response.url}: {response.text}")
            return response.json()

        except RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            raise RequestException(f"Network request failed: {e}") from e

    return get_page


class BaseAlpacaDataSource(DataSource, ABC):
    """Abstract base class for Alpaca DataSource implementations.

    Provides common validation and functionality for all Alpaca data sources.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)
        self._validate_options()

    def _validate_options(self) -> None:
        """Validate that all required options are present and valid."""
        # Check common required options
        common_required = ['symbols', 'APCA-API-KEY-ID', 'APCA-API-SECRET-KEY', 'start', 'end']
        # Allow subclasses to add additional required options
        required_options = common_required + self._additional_required_options()

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
                raise ValueError(
                    f"Invalid symbols format '{symbols}'. "
                    f"Must be a valid Python list/tuple string."
                ) from e
        elif isinstance(symbols, (list, tuple)):
            if not symbols:
                raise ValueError("Symbols list cannot be empty")
        else:
            raise ValueError(f"Symbols must be a list, tuple, "
                             f"or string representation, got {type(symbols)}")

        # Allow subclasses to perform additional validation
        self._validate_additional_options()

    def _additional_required_options(self) -> List[str]:
        """Return list of additional required options beyond the common ones.

        Subclasses can override this to add data-source-specific required options.
        """
        return []

    def _validate_additional_options(self) -> None:
        """Perform additional validation specific to the data source.

        Subclasses can override this to add custom validation logic.
        """
        pass

    @abstractmethod
    def reader(self, schema: StructType) -> "DataSourceReader":
        """Create and return a reader instance for this data source."""
        pass


class BaseAlpacaReader(DataSourceReader, ABC):
    """Abstract base class for Alpaca DataSourceReader implementations.

    Provides common functionality for reading data from Alpaca APIs with
    PyArrow batching support.
    """

    def __init__(self, schema: StructType, options: Dict[str, str]) -> None:
        super().__init__()
        self.schema = schema
        self.options = options

    @property
    def headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            'Content-Type': 'application/json',
            'APCA-API-KEY-ID': self.options['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.options['APCA-API-SECRET-KEY']
        }

    @property
    def endpoint(self) -> str:
        """Get API endpoint URL."""
        return self.options.get("endpoint", DEFAULT_DATA_ENDPOINT)

    @property
    def symbols(self) -> List[str]:
        """Get the list of symbols to fetch data for.

        Note: Symbol validation occurs in BaseAlpacaDataSource._validate_options()
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

    @cached_property
    def pyarrow_type(self) -> pa.Schema:
        """Return PyArrow schema for this data type."""
        return to_arrow_schema(self.schema)

    @property
    @abstractmethod
    def api_params(self) -> Dict[str, Any]:
        """Get API parameters for requests.

        Subclasses should implement this to return data-source-specific parameters.
        """
        pass

    @property
    @abstractmethod
    def data_key(self) -> str:
        """Get the key used to extract data from API response.

        For example: 'bars' for bars data, 'trades' for trades data.
        """
        pass

    @property
    @abstractmethod
    def path_elements(self) -> List[str]:
        """Get the URL path elements for the API endpoint.

        For example: ['stocks', 'bars'] or ['stocks', 'trades']
        """
        pass

    @abstractmethod
    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> Tuple:
        """Parse a single record from API response into a tuple.

        Args:
            symbol: Stock symbol
            record: Record data dictionary from API response

        Returns:
            Tuple containing parsed record data
        """
        pass

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Read data for a single symbol partition.

        Each API page is yielded as a single PyArrow RecordBatch. The batch size
        corresponds to the 'limit' parameter used for API requests.

        Args:
            partition: Symbol partition to read data for

        Yields:
            PyArrow RecordBatch objects, one per API page
        """
        # Ensure partition is SymbolPartition
        if not isinstance(partition, SymbolPartition):
            raise ValueError(f"Expected SymbolPartition, got {type(partition)}")

        # Set up the page fetcher function
        get_page = build_page_fetcher(self.endpoint, self.headers, self.path_elements)

        # Get base params and set symbol
        params = self.api_params
        params['symbols'] = partition.symbol

        # Configure session
        with requests.Session() as sess:
            num_pages = 0
            next_page_token: Optional[str] = None

            # Cycle through pages
            while next_page_token or num_pages < 1:
                retry_count = 0

                # Retry loop
                while retry_count < MAX_RETRIES:
                    try:
                        pg = get_page(sess, params, next_page_token)
                        break  # Success, exit retry loop
                    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                        retry_count += 1
                        if retry_count >= MAX_RETRIES:
                            logger.error(
                                f"Failed to fetch data for symbol {partition.symbol} after {MAX_RETRIES} retries: {e}"
                            )
                            raise ValueError(
                                f"API request failed for symbol {partition.symbol} after {MAX_RETRIES} retries"
                            ) from e

                        logger.warning(f"Retry {retry_count} for symbol {partition.symbol}: {e}")
                        sleep(RETRY_DELAY * retry_count)

                # Process page as a single batch
                if self.data_key in pg and pg[self.data_key]:
                    # Let subclass parse the page into a batch
                    batch = self._parse_page_to_batch(pg[self.data_key], partition.symbol)
                    if batch is not None:
                        yield batch

                # Go to next page
                num_pages += 1
                next_page_token = pg.get("next_page_token", None)

    def _parse_page_to_batch(self, data: Dict[str, List[Dict[str, Any]]], symbol: str) -> Optional[pa.RecordBatch]:
        """Parse a page of data into a PyArrow RecordBatch.

        Args:
            data: Dictionary mapping symbols to lists of records
            symbol: The symbol being processed

        Returns:
            PyArrow RecordBatch or None if no valid data
        """
        # initialize an in-memory buffer for each column
        num_cols = len(self.pyarrow_type)
        buffer_size = 0
        col_buffer: List[List[Any]] = [[] for _ in range(num_cols)]

        # results come as lists of records per symbol
        for sym in data.keys():
            for record in data[sym]:
                try:
                    # parse the record and append it to the column buffer
                    parsed = self._parse_record(sym, record)
                    for i in range(num_cols):
                        col_buffer[i].append(parsed[i])
                    buffer_size += 1
                except ValueError as e:
                    logger.warning(f"Skipping malformed record for {sym}: {e}")
                    continue

        if buffer_size > 0:
            # convert buffers to PyArrow Arrays
            parrays = [pa.array(col_buffer[i], type=self.pyarrow_type.field(i).type) for i in range(num_cols)]
            # return as a batch
            return pa.RecordBatch.from_arrays(parrays, schema=self.pyarrow_type)
        return None
