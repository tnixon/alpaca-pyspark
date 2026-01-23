"""Corporate Actions data sources for Alpaca PySpark connector.

This sub-module provides DataSource implementations for Alpaca's corporate actions data,
including stock splits, dividends, mergers, and other corporate events.
"""

from .corporate_actions import CorporateActionsDataSource

__all__ = ["CorporateActionsDataSource"]
