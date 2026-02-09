"""PySpark DataSource for Alpaca's corporate actions data."""

import logging
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from pyspark.sql.types import MapType, StringType, StructType

from ..common import (
    ApiParam,
    BaseAlpacaDataSource,
    BaseAlpacaReader,
    SymbolTimeRangePartition,
)

# Set up logger
logger = logging.getLogger(__name__)

# Valid values for enum-like parameters
VALID_SORT_VALUES = ("asc", "desc")
VALID_TYPE_VALUES = ("dividend", "split", "merger", "spinoff", "stock_dividend", "all")

# Type alias for corporate action data tuple:
#   symbol, ex_date, record_date, payable_date, type, details
CorporateActionTuple = Tuple[
    str,  # symbol
    Optional[dt],  # ex_date (can be None)
    Optional[dt],  # record_date (can be None)
    Optional[dt],  # payable_date (can be None)
    str,  # type
    Dict[str, Any],  # details (action-specific fields)
]


class CorporateActionsDataSource(BaseAlpacaDataSource):
    """PySpark DataSource for Alpaca's corporate actions data.

    Required options:
        - symbols: List of stock symbols or string representation of list
        - APCA-API-KEY-ID: Alpaca API key ID
        - APCA-API-SECRET-KEY: Alpaca API secret key
        - start: Start date in YYYY-MM-DD format
        - end: End date in YYYY-MM-DD format

    Optional options:
        - endpoint: API endpoint URL (defaults to Alpaca's data endpoint, but uses v1 for corporate actions)
        - limit: Maximum number of corporate actions per API call (default: 10000)
        - sort: Sort order for results ('asc' or 'desc', default: 'asc')
        - types: Corporate action types filter ('dividend', 'split', 'merger', 'spinoff', 'stock_dividend', 'all')
    """

    @property
    def api_params(self) -> List[ApiParam]:
        """Corporate actions API parameters including corporate action-specific options."""
        return super().api_params + [
            ApiParam("sort", False),
            ApiParam("types", False),
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

        return super()._validate_params(options)

    @classmethod
    def name(cls) -> str:
        return "Alpaca_Corporate_Actions"

    def schema(self) -> Union[StructType, str]:
        return """
            symbol STRING,             -- Stock symbol
            ex_date DATE,              -- Ex-dividend date (when stock trades without dividend)
            record_date DATE,          -- Record date (determines dividend eligibility)
            payable_date DATE,         -- Payment date (when dividend is paid)
            type STRING,               -- Corporate action type (dividend, split, etc.)
            details MAP<STRING, STRING> -- Action-specific fields (amount, ratio, symbols, etc.)
        """

    @property
    def pa_schema(self) -> pa.Schema:
        fields: List[Tuple[str, pa.DataType]] = [
            ("symbol", pa.string()),
            ("ex_date", pa.date32()),
            ("record_date", pa.date32()),
            ("payable_date", pa.date32()),
            ("type", pa.string()),
            ("details", pa.map_(pa.string(), pa.string())),
        ]
        return pa.schema(fields)

    def reader(self, schema: StructType) -> "CorporateActionsReader":
        return CorporateActionsReader(self.config, self.pa_schema, self.params)


class CorporateActionsReader(BaseAlpacaReader):
    """Reader implementation for corporate actions data source."""

    @property
    def endpoint(self) -> str:
        """Corporate actions uses v1 API instead of default v2."""
        return self._config.endpoint.replace("/v2", "/v1")

    def api_params(self, partition: SymbolTimeRangePartition) -> Dict[str, str]:
        """Get API parameters with YYYY-MM-DD date format for corporate actions."""
        partition_params: Dict[str, str] = self._params.copy()
        partition_params["symbols"] = partition.symbol
        # Corporate actions API requires YYYY-MM-DD format, not ISO timestamps
        partition_params["start"] = partition.start.strftime("%Y-%m-%d")
        partition_params["end"] = partition.end.strftime("%Y-%m-%d")
        partition_params["limit"] = str(self.limit)
        return partition_params

    @property
    def data_key(self) -> str:
        """Corporate actions data is returned under the 'corporate_actions' key."""
        return "corporate_actions"

    @property
    def path_elements(self) -> List[str]:
        """URL path for corporate actions endpoint."""
        return ["corporate-actions"]

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
            # Corporate actions API returns dates in YYYY-MM-DD format, not timestamps
            def parse_date(date_str: Optional[str]) -> Optional[dt]:
                if not date_str:
                    return None
                # Handle both YYYY-MM-DD and ISO timestamp formats for flexibility
                if "T" in date_str:
                    return dt.fromisoformat(date_str).replace(tzinfo=None)
                else:
                    return dt.strptime(date_str, "%Y-%m-%d")

            ex_date = parse_date(record.get("ex_date"))
            record_date = parse_date(record.get("record_date"))
            payable_date = parse_date(record.get("payable_date"))

            # Extract common fields
            action_type = record.get("type", "")
            
            # Pack all other fields into details map
            common_fields = {"ex_date", "record_date", "payable_date", "type"}
            details = {k: str(v) if v is not None else "" for k, v in record.items() if k not in common_fields}

            return (
                symbol,
                ex_date,
                record_date,
                payable_date,
                action_type,
                details,
            )
        except (KeyError, ValueError, TypeError) as e:
            # Truncate record data for readability in error messages
            record_summary = {k: v for k, v in list(record.items())[:3]}
            if len(record) > 3:
                record_summary["..."] = f"and {len(record) - 3} more fields"
            raise ValueError(
                f"Failed to parse corporate action data for symbol {symbol}: " f"{record_summary}. Error: {e}"
            ) from e
