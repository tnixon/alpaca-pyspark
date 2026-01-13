import ast
import logging
import math
import time
import urllib.parse as urlp
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime as dt, timedelta as td
from functools import cached_property
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pyarrow as pa
import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from requests import HTTPError, RequestException, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger(__name__)

# Common constants
DEFAULT_DATA_ENDPOINT = "https://data.alpaca.markets/v2"
DEFAULT_LIMIT = 10000
MAX_RETRIES = 3
RETRY_DELAY = 1.0

# Type alias for symbols option
Symbols_Option_Type = Union[str, List[str], Tuple[str, ...]]

# Type alias for page fetcher function signature
Page_Fetcher_SigType = Callable[[Session, Dict[str, Any], Optional[str]], Dict[str, Any]]


@dataclass
class SymbolTimeRangePartition(InputPartition):
    """Partition representing a single stock symbol on a single date for parallel processing."""

    symbol: str
    start: dt
    end: dt


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


def build_page_fetcher(endpoint: str, headers: Dict[str, str], path_elements: List[str]) -> Page_Fetcher_SigType:
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
            request_params["page_token"] = page_token

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


def retriable_session(num_retries: int = MAX_RETRIES) -> Session:
    """
    Creates a session configured to automatically retry failed requests.

    Args:
        num_retries: the maximum number of retry attempts (default: MAX_RETRIES)

    Returns: a session configured to automatically retry failed requests
    """
    session = requests.Session()

    # Define the retry strategy
    retry_strategy = Retry(
        total=num_retries,  # Total number of retries
        backoff_factor=1,  # Wait [0.5s, 1s, 2s, 4s, 8s...] between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Only retry safe/idempotent methods
    )

    # Create the adapter and mount it to the session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_all_pages(
    page_fetcher_fn: Page_Fetcher_SigType,
    params: Dict[str, Any],
    num_retries: int = MAX_RETRIES,
    rate_limit_delay: float = 0.0,
) -> Iterator[Dict[str, Any]]:
    """Fetch all pages of data from the API with retry logic and optional rate limiting.

    Args:
        page_fetcher_fn: Function to fetch a single page of data
        params: Base query parameters for API requests
        num_retries: Maximum number of retry attempts (default: MAX_RETRIES)
        rate_limit_delay: Seconds to sleep between page fetches (default: 0.0, disabled)

    Yields:
        Dict[str, Any]: JSON response from each API page

    Raises:
        ValueError: If all retry attempts fail
    """
    # Configure session
    with retriable_session(num_retries) as sess:
        num_pages = 0
        next_page_token: Optional[str] = None

        # Cycle through pages
        while next_page_token or num_pages < 1:
            # fetch the next page of results
            pg = page_fetcher_fn(sess, params, next_page_token)

            # yield up the page of results
            yield pg

            # Go to next page
            num_pages += 1
            next_page_token = pg.get("next_page_token", None)

            # Apply rate limiting if configured and there are more pages
            if rate_limit_delay > 0 and next_page_token:
                time.sleep(rate_limit_delay)


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
        common_required = ["symbols", "APCA-API-KEY-ID", "APCA-API-SECRET-KEY", "start", "end"]
        # Allow subclasses to add additional required options
        required_options = common_required + self._additional_required_options()

        missing = [opt for opt in required_options if opt not in self.options or not self.options[opt]]
        if missing:
            raise ValueError(f"Missing required options: {missing}")

        # Validate symbols format
        symbols: Symbols_Option_Type = self.options.get("symbols", [])
        if isinstance(symbols, str):
            try:
                parsed_symbols = ast.literal_eval(symbols)
                if not isinstance(parsed_symbols, (list, tuple)) or not parsed_symbols:
                    raise ValueError("Symbols must be a non-empty list or tuple")
            except (ValueError, SyntaxError) as e:
                raise ValueError(
                    f"Invalid symbols format '{symbols}'. " f"Must be a valid Python list/tuple string."
                ) from e
        elif isinstance(symbols, (list, tuple)):
            if not symbols:
                raise ValueError("Symbols list cannot be empty")
        else:
            raise ValueError(f"Symbols must be a list, tuple, " f"or string representation, got {type(symbols)}")

        # Validate start and end datetime formats
        start_str = self.options.get("start", "")
        end_str = self.options.get("end", "")

        try:
            start_t = dt.fromisoformat(start_str)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid 'start' option: '{start_str}' is not a valid ISO format datetime") from e

        try:
            end_t = dt.fromisoformat(end_str)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid 'end' option: '{end_str}' is not a valid ISO format datetime") from e

        # make sure that start is before end
        if start_t > end_t:
            raise ValueError(f"start time is after end time: {start_t} > {end_t}")

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

    @property
    @abstractmethod
    def pa_schema(self) -> pa.Schema:
        """Return PyArrow schema for this data type."""
        pass


class BaseAlpacaReader(DataSourceReader, ABC):
    """Abstract base class for Alpaca DataSourceReader implementations.

    Provides common functionality for reading data from Alpaca APIs with
    PyArrow batching support.
    """

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
        return self.options.get("endpoint", DEFAULT_DATA_ENDPOINT)

    @cached_property
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

    @cached_property
    def start(self) -> dt:
        return dt.fromisoformat(self.options.get("start", ""))

    @cached_property
    def end(self) -> dt:
        return dt.fromisoformat(self.options.get("end", ""))

    @cached_property
    def limit(self) -> int:
        return int(self.options.get("limit", DEFAULT_LIMIT))

    @property
    def partition_interval(self) -> td:
        return td(days=1)

    def partitions(self) -> Sequence[SymbolTimeRangePartition]:
        """Create partitions for parallel processing, one per symbol per date."""
        symbol_list = self.symbols
        if not symbol_list:
            raise ValueError("No symbols provided for data fetching")
        # calculate the date range
        range_td = self.end - self.start
        interval_td = self.partition_interval
        num_intervals = math.ceil(range_td / interval_td)
        # don't bother partitioning by datetime if only 1 interval
        if num_intervals < 2:
            return [SymbolTimeRangePartition(sym, start=self.start, end=self.end) for sym in symbol_list]
        # otherwise, build a set of time intervals
        interval_bounds = [(self.start + i*interval_td,
                            min([self.start + (i+1)*interval_td, self.end]))
                           for i in range(num_intervals)]
        # partitions for all symbols and intervals
        return [SymbolTimeRangePartition(sym, s, e) for sym in symbol_list for s, e in interval_bounds]

    def api_params(self, partition: SymbolTimeRangePartition) -> Dict[str, Any]:
        """Get API parameters for requests.

        Args:
            partition: the current partition

        Returns: API parameters for the current partition
        """
        return {
            "symbols": partition.symbol,
            "start": partition.start.isoformat(),
            "end": partition.end.isoformat(),
            "limit": self.limit,
        }

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
        if not isinstance(partition, SymbolTimeRangePartition):
            raise ValueError(f"Expected SymbolPartition, got {type(partition)}")

        # Set up the page fetcher function
        get_page = build_page_fetcher(self.endpoint, self.headers, self.path_elements)

        # Get base params and set symbol
        params = self.api_params(partition)

        # Get rate limit delay from options (default: 0.0, disabled)
        rate_limit_delay = float(self.options.get("rate_limit_delay", 0.0))

        # process all pages of results
        for pg in fetch_all_pages(get_page, params, rate_limit_delay=rate_limit_delay):
            # Process page as a single batch
            if self.data_key in pg and pg[self.data_key]:
                # Let subclass parse the page into a batch
                batch = self._parse_page_to_batch(pg[self.data_key])
                if batch is not None:
                    yield batch

    def _parse_page_to_batch(self, data: Dict[str, List[Dict[str, Any]]]) -> Optional[pa.RecordBatch]:
        """Parse a page of data into a PyArrow RecordBatch.

        Args:
            data: Dictionary mapping symbols to lists of records

        Returns:
            PyArrow RecordBatch or None if no valid data
        """
        # initialize an in-memory buffer for each column
        num_cols = len(self.pa_schema)
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
            parrays = [pa.array(col_buffer[i], type=self.pa_schema.field(i).type) for i in range(num_cols)]
            # return as a batch
            return pa.RecordBatch.from_arrays(parrays, schema=self.pa_schema)
        return None
