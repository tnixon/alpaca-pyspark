# alpaca-pyspark

A high-performance PySpark connector for importing market data from the Alpaca Markets API in a distributed fashion.

> **⚠️ Important**: This library requires **Apache Spark 4.0+** and **Python 3.11+**

## Overview

**alpaca-pyspark** provides custom PySpark DataSource implementations that enable efficient, parallel retrieval of market data from Alpaca Markets. The library leverages PySpark's distributed computing capabilities to fetch data across multiple stock symbols concurrently, with built-in retry logic, error handling, and PyArrow batch processing for optimal performance.

### Key Features

- **Distributed Data Fetching**: Automatically parallelizes API requests across stock symbols and time ranges
- **Intelligent Partitioning**: Dynamically sizes partitions based on data volume for optimal load balancing
- **PyArrow Batch Processing**: Uses Apache Arrow for high-performance data transfer (up to 10x faster than row-by-row processing)
- **Resilient**: Built-in retry logic with exponential backoff for network failures
- **Type-Safe**: Strict schema definitions ensure data consistency
- **Easy Integration**: Works seamlessly with PySpark DataFrames and Spark SQL

## Quick Start

### Installation

This project uses [Poetry](https://python-poetry.org/) for dependency management.

**Prerequisites:**
- Python 3.11 or higher
- Apache Spark 4.0+ / PySpark 4.0+
- Poetry installed ([installation guide](https://python-poetry.org/docs/#installation))

**Setup:**
```bash
git clone https://github.com/tnixon/alpaca-pyspark.git
cd alpaca-pyspark
poetry install
poetry shell
```

### Basic Example

```python
import datetime as dt
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from alpaca_pyspark.stocks import HistoricalBarsDataSource

# Initialize Spark session (requires Spark 4.0+)
spark = SparkSession.builder.appName("AlpacaExample").getOrCreate()

# Register the data source
spark.dataSource.register(HistoricalBarsDataSource)

# Configure the data source options
tz = ZoneInfo("America/New_York")
options = {
    "symbols": ["AAPL", "MSFT", "GOOG"],
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key", 
    "timeframe": "1Day",
    "start": dt.datetime(2021, 1, 1, tzinfo=tz).isoformat(),
    "end": dt.datetime(2022, 1, 1, tzinfo=tz).isoformat()
}

# Load data as a DataFrame
df = (spark.read
      .format("Alpaca_Stocks_Bars")
      .options(**options)
      .load())

# Use the DataFrame
df.show()
df.createOrReplaceTempView("bars")
spark.sql("SELECT symbol, time, close FROM bars WHERE symbol = 'AAPL'").show()
```

### Available Data Sources

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Stocks_Bars` | `HistoricalBarsDataSource` | Historical OHLCV bars/candles for stocks |
| `Alpaca_Stocks_Trades` | `HistoricalTradesDataSource` | Historical tick-by-tick trades for stocks |
| `Alpaca_Options_Bars` | `HistoricalOptionBarsDataSource` | Historical OHLCV bars/candles for options |
| `Alpaca_Corporate_Actions` | `CorporateActionsDataSource` | Corporate actions (splits, dividends, etc.) |

### Example: Stock Trades
```python
from alpaca_pyspark.stocks import HistoricalTradesDataSource
spark.dataSource.register(HistoricalTradesDataSource)

df = (spark.read
      .format("Alpaca_Stocks_Trades")
      .options(**options)  # No timeframe needed for trades
      .load())
```

### Example: Options Data
```python
from alpaca_pyspark.options import HistoricalOptionBarsDataSource
spark.dataSource.register(HistoricalOptionBarsDataSource)

options = {
    "symbols": ["AAPL241220C00150000"],  # Options use specific format
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "timeframe": "1Hour",
    "start": dt.datetime(2024, 12, 1, tzinfo=tz).isoformat(),
    "end": dt.datetime(2024, 12, 20, tzinfo=tz).isoformat()
}

df = (spark.read
      .format("Alpaca_Options_Bars") 
      .options(**options)
      .load())
```

## Documentation

For detailed information about using and contributing to alpaca-pyspark:

- **[Usage Guide](USAGE.md)**: Comprehensive configuration options, data schemas, and advanced usage patterns
- **[Contributing Guide](CONTRIBUTING.md)**: Development environment setup, testing procedures, and contribution workflow

## Security

**⚠️ Important**: Never commit API credentials to version control. Use secure methods like:

- Environment variables
- Spark secrets management (e.g., Databricks secrets)  
- Cloud secret managers (AWS Secrets Manager, Azure Key Vault, etc.)

```python
import os
options = {
    "APCA-API-KEY-ID": os.getenv("APCA_API_KEY_ID"),
    "APCA-API-SECRET-KEY": os.getenv("APCA_API_SECRET_KEY"),
    # ... other options
}
```

## External Documentation

- **[Alpaca Market Data API](https://docs.alpaca.markets/docs/about-market-data-api)**: API specification and endpoints
- **[PySpark DataSource API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)**: PySpark custom data source implementation
- **[PyArrow Documentation](https://arrow.apache.org/docs/python/)**: Arrow batch processing
- **[Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)**: Spark SQL programming

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on:

- Development environment setup
- Code quality standards  
- Testing procedures
- Contribution workflow
