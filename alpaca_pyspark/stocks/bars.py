"""PySpark DataSource for Alpaca's stock historical bars data."""

import logging
from typing import List

from pyspark.sql.types import StructType

from ..bars import (
    AbstractBarsDataSource,
    AbstractBarsReader,
)

# Set up logger
logger = logging.getLogger(__name__)


class StockBarsDataSource(AbstractBarsDataSource):
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

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Stocks_Bars"

    def reader(self, schema: StructType) -> "HistoricalStockBarsReader":
        return HistoricalStockBarsReader(self.config, self.pa_schema, self.params)


class HistoricalStockBarsReader(AbstractBarsReader):
    """Reader implementation for historical bars data source."""

    @property
    def path_elements(self) -> List[str]:
        """URL path for bars endpoint."""
        return ["stocks", "bars"]
