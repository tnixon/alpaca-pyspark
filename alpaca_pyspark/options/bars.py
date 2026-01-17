"""PySpark DataSource for Alpaca's option historical bars data."""

import logging
from typing import Any, Dict, Iterator, List, Sequence, Union

import pyarrow as pa
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType

from ..bars import (
    AbstractBarsDataSource,
    AbstractBarsReader,
)
from ..common import (
    SymbolPartition,
    SymbolTimeRangePartition,
    build_page_fetcher,
    fetch_all_pages,
)

# Set up logger
logger = logging.getLogger(__name__)

DEFAULT_OPTS_BARS_ENDPOINT = "https://data.alpaca.markets/v1beta1"


class OptionBarsDataSource(AbstractBarsDataSource):
    """PySpark DataSource for Alpaca's historical option bars data.

    Required options:
        - symbols: List of option symbols or string representation of list (OCC format)
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key
        - timeframe: Time frame for bars (e.g., '1Day', '1Hour')

    Optional options:
        - start: Start date/time (ISO format)
        - end: End date/time (ISO format)
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of bars per API call (default: 10000)
    """

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Options_Bars"

    def _common_required_options(self) -> List[str]:
        """Options bars do not require start/end dates."""
        return ["symbols", "APCA-API-KEY-ID", "APCA-API-SECRET-KEY"]

    def reader(self, schema: StructType) -> "HistoricalOptionBarsReader":
        return HistoricalOptionBarsReader(self.pa_schema, self.options)


class HistoricalOptionBarsReader(AbstractBarsReader):
    """Reader implementation for historical option bars data source."""

    @property
    def endpoint(self) -> str:
        """Get API endpoint URL."""
        return self.options.get("endpoint", DEFAULT_OPTS_BARS_ENDPOINT)

    @property
    def path_elements(self) -> List[str]:
        """URL path for option bars endpoint."""
        return ["options", "bars"]

    @property
    def has_date_range(self) -> bool:
        """Check if start and end dates are provided."""
        return bool(self.options.get("start")) and bool(self.options.get("end"))

    def partitions(self) -> Sequence[Union[SymbolPartition, SymbolTimeRangePartition]]:
        """Create partitions for parallel processing.

        Uses symbol-only partitions if no date range provided,
        otherwise uses time-range partitions from parent class.
        """
        if self.has_date_range:
            return super().partitions()
        # No date range - use symbol-only partitions
        return [SymbolPartition(symbol=sym) for sym in self.symbols]

    def api_params(
        self, partition: Union[SymbolPartition, SymbolTimeRangePartition]
    ) -> Dict[str, Any]:
        """Get API parameters for requests."""
        params: Dict[str, Any] = {
            "symbols": partition.symbol,
            "timeframe": self.options["timeframe"],
            "limit": self.limit,
        }
        # Add date range if partition has it
        if isinstance(partition, SymbolTimeRangePartition):
            params["start"] = partition.start.isoformat()
            params["end"] = partition.end.isoformat()
        return params

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Read data for a single symbol partition.

        Accepts both SymbolPartition (no dates) and SymbolTimeRangePartition.
        """
        if not isinstance(partition, (SymbolPartition, SymbolTimeRangePartition)):
            raise ValueError(f"Expected SymbolPartition, got {type(partition)}")

        get_page = build_page_fetcher(self.endpoint, self.headers, self.path_elements)
        params = self.api_params(partition)
        rate_limit_delay = float(self.options.get("rate_limit_delay", 0.0))

        for pg in fetch_all_pages(get_page, params, rate_limit_delay=rate_limit_delay):
            if self.data_key in pg and pg[self.data_key]:
                batch = self._parse_page_to_batch(pg[self.data_key])
                if batch is not None:
                    yield batch
