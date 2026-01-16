# alpaca-pyspark

A high-performance PySpark connector for importing market data from the Alpaca Markets API in a distributed fashion.

## Overview

**alpaca-pyspark** provides custom PySpark DataSource implementations that enable efficient, parallel retrieval of market data from Alpaca Markets. The library leverages PySpark's distributed computing capabilities to fetch data across multiple symbols concurrently, with built-in retry logic, error handling, and PyArrow batch processing for optimal performance.

### Key Features

- **Distributed Data Fetching**: Automatically parallelizes API requests across symbols and time ranges
- **Intelligent Partitioning**: Dynamically sizes partitions based on data volume for optimal load balancing
- **PyArrow Batch Processing**: Uses Apache Arrow for high-performance data transfer (up to 10x faster than row-by-row processing)
- **Resilient**: Built-in retry logic with exponential backoff for network failures
- **Type-Safe**: Strict schema definitions ensure data consistency
- **Easy Integration**: Works seamlessly with PySpark DataFrames and Spark SQL

## Quick Start

```python
import datetime as dt
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from alpaca_pyspark.stocks import StockBarsDataSource

# Initialize Spark session
spark = SparkSession.builder.appName("AlpacaExample").getOrCreate()

# Register the data source
spark.dataSource.register(StockBarsDataSource)

# Configure and load data
tz = ZoneInfo("America/New_York")
df = spark.read.format("Alpaca_Stocks_Bars").options(**{
    "symbols": ["AAPL", "MSFT", "GOOG"],
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "timeframe": "1Day",
    "start": dt.datetime(2024, 1, 1, tzinfo=tz).isoformat(),
    "end": dt.datetime(2024, 12, 31, tzinfo=tz).isoformat(),
}).load()

df.show()
```

## Available Data Sources

The library provides data sources following the naming pattern: `Alpaca_<AssetType>_<DataType>`

### Stocks

| DataSource Name | Description | Documentation |
|----------------|-------------|---------------|
| `Alpaca_Stocks_Bars` | Historical OHLCV bars/candles | [Details](alpaca_pyspark/stocks/README.md#alpaca_stocks_bars) |
| `Alpaca_Stocks_Trades` | Historical tick-by-tick trades | [Details](alpaca_pyspark/stocks/README.md#alpaca_stocks_trades) |

### Options

| DataSource Name | Description | Documentation |
|----------------|-------------|---------------|
| `Alpaca_Options_Bars` | Historical OHLCV bars for option contracts | [Details](alpaca_pyspark/options/README.md#alpaca_options_bars) |
| `Alpaca_Options_Contracts` | Options contract listings and metadata | [Details](alpaca_pyspark/options/README.md#alpaca_options_contracts) |

For detailed configuration options, schemas, and examples, see the module-specific documentation:
- **[Stocks Documentation](alpaca_pyspark/stocks/README.md)**
- **[Options Documentation](alpaca_pyspark/options/README.md)**

## Installation

This project uses [Poetry](https://python-poetry.org/) for dependency management.

### Prerequisites

- Python 3.11 or higher
- Poetry installed ([installation guide](https://python-poetry.org/docs/#installation))
- Apache Spark / PySpark environment

### Setup

```bash
git clone <repository-url>
cd alpaca-pyspark
poetry install
poetry shell
```

## Project Structure

| Directory | Description |
|-----------|-------------|
| `alpaca_pyspark/` | Main package with base classes and shared utilities |
| `alpaca_pyspark/stocks/` | Stock market data sources ([docs](alpaca_pyspark/stocks/README.md)) |
| `alpaca_pyspark/options/` | Options data sources ([docs](alpaca_pyspark/options/README.md)) |
| `alpaca_pyspark/crypto/` | Crypto data sources (placeholder) |
| `tests/` | Unit, integration, and Spark tests |

## Architecture

### Parallel Processing

Data requests are partitioned by **symbol and time range**, enabling Spark to execute API requests in parallel across multiple executors. For bars data, partition intervals are calculated dynamically based on timeframe and expected data volume.

### PyArrow Batching

API pages are converted to PyArrow RecordBatch objects for efficient data transfer, providing significant performance improvements over row-by-row processing.

### Error Handling

Built-in resilience includes:
- Automatic retry with exponential backoff (3 retries by default)
- Graceful handling of malformed data (logged as warnings)
- Connection timeout management

### Class Hierarchy

```
BaseAlpacaDataSource (common.py)
├── AbstractBarsDataSource (bars.py)
│   ├── StockBarsDataSource (stocks/bars.py)
│   └── OptionBarsDataSource (options/bars.py)
├── StockTradesDataSource (stocks/trades.py)
└── OptionsContractsDataSource (options/contracts.py)
```

## Development

### Code Quality

```bash
# Format code
poetry run ruff format alpaca_pyspark/

# Lint
poetry run flake8 alpaca_pyspark/

# Type check
poetry run mypy alpaca_pyspark/
```

### Testing

```bash
# Run all tests
poetry run pytest

# Run unit tests only
poetry run pytest -m unit

# Run specific test file
poetry run pytest tests/unit/test_common.py
```

### Building

```bash
poetry build
```

## API Credentials

**Security Note**: Never commit API credentials to version control. Use environment variables, Spark secrets management, or cloud secret managers.

## External Documentation

- [Alpaca Market Data API](https://docs.alpaca.markets/docs/about-market-data-api)
- [PySpark DataSource API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please ensure code follows the existing style and includes appropriate tests.
