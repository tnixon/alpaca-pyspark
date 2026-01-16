"""PySpark DataSource for Alpaca's option historical bars data."""

import logging
from typing import List

from pyspark.sql.types import StructType

from ..bars import (
    AbstractBarsDataSource,
    AbstractBarsReader,
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
        - start: Start date/time (ISO format)
        - end: End date/time (ISO format)

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of bars per API call (default: 10000)
    """

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Options_Bars"

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
