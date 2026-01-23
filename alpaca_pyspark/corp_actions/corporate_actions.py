"""PySpark DataSource for Alpaca's corporate actions data."""

import logging
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import StructType

from ..common import (
    ApiParam,
    BaseAlpacaDataSource,
    BaseAlpacaReader,
)

# Set up logger
logger = logging.getLogger(__name__)

# Valid values for enum-like parameters
VALID_SORT_VALUES = ("asc", "desc")
VALID_TYPE_VALUES = ("dividend", "split", "merger", "spinoff", "stock_dividend", "all")

# Type alias for corporate action data tuple: symbol, ex_date, record_date, payable_date, type, amount, ratio, new_symbol, old_symbol
CorporateActionTuple = Tuple[
    str,                # symbol
    Optional[dt],       # ex_date (can be None)
    Optional[dt],       # record_date (can be None)  
    Optional[dt],       # payable_date (can be None)
    str,                # type
    float,              # amount
    float,              # ratio
    str,                # new_symbol
    str,                # old_symbol
]


class CorporateActionsDataSource(BaseAlpacaDataSource):
    """PySpark DataSource for Alpaca's corporate actions data.

    Required options:
        - symbols: List of stock symbols or string representation of list
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key
        - start: Start date/time (ISO format)
        - end: End date/time (ISO format)

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint)
        - limit: Maximum number of corporate actions per API call (default: 10000)
        - sort: Sort order for results ('asc' or 'desc', default: 'asc')
        - types: Corporate action types filter ('dividend', 'split', 'merger', 'spinoff', 'stock_dividend', 'all')
        - date_type: Date field to filter on ('ex_date', 'record_date', 'payable_date', default: 'ex_date')
    """

    @property
    def api_params(self) -> List[ApiParam]:
        """Corporate actions API parameters including corporate action-specific options."""
        return super().api_params + [
            ApiParam("sort", False),
            ApiParam("types", False),
            ApiParam("date_type", False),
        ]

    def _validate_params(self, options: Dict[str, str]) -> Dict[str, str]:
        """Validate corporate action-specific parameters."""
        # Validate sort parameter
        sort = options.get("sort", "").lower()
        if sort and sort not in VALID_SORT_VALUES:
            raise ValueError(f"Invalid 'sort' value: '{sort}'. Must be one of: {VALID_SORT_VALUES}")

        # Validate types parameter
        types = options.get("types", "")
        if types:
            # Types can be a comma-separated list or single value
            type_list = [t.strip().lower() for t in types.split(",") if t.strip()]
            invalid_types = [t for t in type_list if t not in VALID_TYPE_VALUES]
            if invalid_types:
                raise ValueError(f"Invalid 'types' values: {invalid_types}. Must be one of: {VALID_TYPE_VALUES}")

        # Validate date_type parameter
        date_type = options.get("date_type", "ex_date")
        valid_date_types = ("ex_date", "record_date", "payable_date")
        if date_type not in valid_date_types:
            raise ValueError(f"Invalid 'date_type' value: '{date_type}'. Must be one of: {valid_date_types}")

        return super()._validate_params(options)

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Corporate_Actions"

    def schema(self) -> Union[StructType, str]:
        return """
            symbol STRING,             -- Stock symbol
            ex_date TIMESTAMP,         -- Ex-dividend date (when stock trades without dividend)
            record_date TIMESTAMP,     -- Record date (determines dividend eligibility)  
            payable_date TIMESTAMP,    -- Payment date (when dividend is paid)
            type STRING,               -- Corporate action type (dividend, split, etc.)
            amount DOUBLE,             -- Cash amount (for dividends) or 0.0 (for splits)
            ratio DOUBLE,              -- Split ratio (for splits) or 1.0 (for dividends)
            new_symbol STRING,         -- New symbol after action (for mergers/spinoffs)
            old_symbol STRING          -- Original symbol before action
        """

    @property
    def pa_schema(self) -> pa.Schema:
        fields: List[Tuple[str, pa.DataType]] = [
            ("symbol", pa.string()),
            ("ex_date", pa.timestamp("us", tz="UTC")),
            ("record_date", pa.timestamp("us", tz="UTC")),
            ("payable_date", pa.timestamp("us", tz="UTC")),
            ("type", pa.string()),
            ("amount", pa.float64()),
            ("ratio", pa.float64()),
            ("new_symbol", pa.string()),
            ("old_symbol", pa.string()),
        ]
        return pa.schema(fields)

    def reader(self, schema: StructType) -> "CorporateActionsReader":
        return CorporateActionsReader(self.config, self.pa_schema, self.params)


class CorporateActionsReader(BaseAlpacaReader):
    """Reader implementation for corporate actions data source."""

    @property
    def data_key(self) -> str:
        """Corporate actions data is returned under the 'corporate_actions' key."""
        return "corporate_actions"

    @property
    def path_elements(self) -> List[str]:
        """URL path for corporate actions endpoint."""
        return ["stocks", "corporate_actions"]

    def _parse_record(self, symbol: str, record: Dict[str, Any]) -> CorporateActionTuple:
        """Parse a single corporate action from API response into tuple format.

        Args:
            symbol: Stock symbol
            record: Corporate action data dictionary from API response

        Returns:
            Tuple containing parsed corporate action data

        Raises:
            ValueError: If corporate action data is malformed or missing required fields
        """
        try:
            # Parse dates, handling potential None values
            ex_date = dt.fromisoformat(record["ex_date"]) if record.get("ex_date") else None
            record_date = dt.fromisoformat(record["record_date"]) if record.get("record_date") else None
            payable_date = dt.fromisoformat(record["payable_date"]) if record.get("payable_date") else None

            return (
                symbol,
                ex_date,
                record_date,
                payable_date,
                record.get("type", ""),
                float(record.get("amount", 0.0)),
                float(record.get("ratio", 0.0)),
                record.get("new_symbol", ""),
                record.get("old_symbol", ""),
            )
        except (KeyError, ValueError, TypeError) as e:
            # Truncate record data for readability in error messages
            record_summary = {k: v for k, v in list(record.items())[:3]}
            if len(record) > 3:
                record_summary["..."] = f"and {len(record) - 3} more fields"
            raise ValueError(f"Failed to parse corporate action data for symbol {symbol}: {record_summary}. Error: {e}") from e