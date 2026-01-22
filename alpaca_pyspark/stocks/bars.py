"""PySpark DataSource for Alpaca's stock historical bars data."""

import logging
import re
from typing import Dict, List

from pyspark.sql.types import StructType

from ..bars import (
    AbstractBarsDataSource,
    AbstractBarsReader,
)
from ..common import ApiParam

# Set up logger
logger = logging.getLogger(__name__)

# Valid values for enum-like parameters (sort is validated in base class)
VALID_ADJUSTMENT_VALUES = ("raw", "split", "dividend", "all")
VALID_FEED_VALUES = ("iex", "sip", "delayed_sip", "otc")


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
        - sort: Sort order for results ('asc' or 'desc', default: 'asc')
        - adjustment: Corporate action adjustment ('raw', 'split', 'dividend', 'all')
        - feed: Data feed source ('iex', 'sip', 'delayed_sip', 'otc')
        - currency: Currency for prices in ISO 4217 format (default: 'USD')
        - asof: Point-in-time view date in YYYY-MM-DD format
    """

    @property
    def api_params(self) -> List[ApiParam]:
        """Stock bars API parameters including stock-specific options."""
        return super().api_params + [
            ApiParam("adjustment", False),
            ApiParam("feed", False),
            ApiParam("currency", False),
            ApiParam("asof", False),
        ]

    def _validate_params(self, options: Dict[str, str]) -> Dict[str, str]:
        """Validate stock-specific parameters."""
        # Validate adjustment parameter
        adjustment = options.get("adjustment", "").lower()
        if adjustment and adjustment not in VALID_ADJUSTMENT_VALUES:
            raise ValueError(
                f"Invalid 'adjustment' value: '{adjustment}'. "
                f"Must be one of: {VALID_ADJUSTMENT_VALUES}"
            )

        # Validate feed parameter
        feed = options.get("feed", "").lower()
        if feed and feed not in VALID_FEED_VALUES:
            raise ValueError(
                f"Invalid 'feed' value: '{feed}'. Must be one of: {VALID_FEED_VALUES}"
            )

        # Validate asof date format (YYYY-MM-DD)
        asof = options.get("asof", "")
        if asof and not re.match(r"^\d{4}-\d{2}-\d{2}$", asof):
            raise ValueError(
                f"Invalid 'asof' format: '{asof}'. Must be in YYYY-MM-DD format"
            )

        return super()._validate_params(options)

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
