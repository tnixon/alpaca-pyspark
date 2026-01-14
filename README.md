# alpaca-pyspark

A high-performance PySpark connector for importing market data from the Alpaca Markets API in a distributed fashion.

## Overview

**alpaca-pyspark** provides custom PySpark DataSource implementations that enable efficient, parallel retrieval of market data from Alpaca Markets. The library leverages PySpark's distributed computing capabilities to fetch data across multiple stock symbols concurrently, with built-in retry logic, error handling, and PyArrow batch processing for optimal performance.

### Key Features

- **Distributed Data Fetching**: Automatically parallelizes API requests across stock symbols and time ranges
- **Intelligent Partitioning**: Dynamically sizes partitions based on data volume for optimal load balancing
- **PyArrow Batch Processing**: Uses Apache Arrow for high-performance data transfer (up to 10x faster than row-by-row processing)
- **Resilient**: Built-in retry logic with exponential backoff for network failures
- **Type-Safe**: Strict schema definitions ensure data consistency
- **Easy Integration**: Works seamlessly with PySpark DataFrames and Spark SQL

### Documentation Links

- **Alpaca Market Data API**: https://docs.alpaca.markets/docs/about-market-data-api
- **PySpark DataSource API**: https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
- **PyArrow Documentation**: https://arrow.apache.org/docs/python/

## Usage

### Basic Example

```python
import datetime as dt
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from alpaca_pyspark.stocks import HistoricalBarsDataSource

# Initialize Spark session
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
    "end": dt.datetime(2022, 1, 1, tzinfo=tz).isoformat(),
    "limit": 1000
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

### Configuration Options

#### Required Options

- **symbols**: List of stock symbols (e.g., `["AAPL", "MSFT"]`)
- **APCA-API-KEY-ID**: Your Alpaca API key ID
- **APCA-API-SECRET-KEY**: Your Alpaca API secret key
- **timeframe**: Bar timeframe (e.g., `"1Min"`, `"1Hour"`, `"1Day"`)
- **start**: Start date/time in ISO format
- **end**: End date/time in ISO format

#### Optional Options

- **endpoint**: API endpoint URL (defaults to Alpaca's production endpoint)
- **limit**: Maximum bars per API request (default: 10000)

### Schema

The Historical Bars data source returns DataFrames with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Stock symbol |
| time | TIMESTAMP | Bar timestamp |
| open | FLOAT | Opening price |
| high | FLOAT | High price |
| low | FLOAT | Low price |
| close | FLOAT | Closing price |
| volume | INT | Trading volume |
| trade_count | INT | Number of trades |
| vwap | FLOAT | Volume-weighted average price |

## Available Data Sources

The library provides data sources following a consistent naming pattern: `Alpaca_<AssetType>_<DataType>`

### Stocks

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Stocks_Bars` | `HistoricalBarsDataSource` | Historical OHLCV bars/candles for stocks |
| `Alpaca_Stocks_Trades` | `HistoricalTradesDataSource` | Historical tick-by-tick trades for stocks |

### Options

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Options_Bars` | `HistoricalOptionBarsDataSource` | Historical OHLCV bars/candles for options contracts |

### Usage Examples

**Stock Bars:**
```python
from alpaca_pyspark.stocks import HistoricalBarsDataSource
spark.dataSource.register(HistoricalBarsDataSource)
df = spark.read.format("Alpaca_Stocks_Bars").options(**options).load()
```

**Stock Trades:**
```python
from alpaca_pyspark.stocks import HistoricalTradesDataSource
spark.dataSource.register(HistoricalTradesDataSource)
df = spark.read.format("Alpaca_Stocks_Trades").options(**options).load()
```

**Option Bars:**
```python
from alpaca_pyspark.options import HistoricalOptionBarsDataSource
spark.dataSource.register(HistoricalOptionBarsDataSource)
df = spark.read.format("Alpaca_Options_Bars").options(**options).load()
```

## Installation

This project uses [Poetry](https://python-poetry.org/) for dependency management.

### Prerequisites

- Python 3.11 or higher
- Poetry installed ([installation guide](https://python-poetry.org/docs/#installation))
- Apache Spark / PySpark environment

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd alpaca-pyspark
```

2. Install dependencies:
```bash
poetry install
```

3. Activate the virtual environment:
```bash
poetry shell
```

## Project Structure

The project is organized as follows:

```
alpaca-pyspark/
├── alpaca_pyspark/          # Main package directory
│   ├── common.py            # Shared base classes, utilities, and partitioning logic
│   ├── bars.py              # Abstract base classes for bars data sources
│   ├── stocks/              # Stock market data sources
│   │   ├── bars.py          # Stock historical bars data source implementation
│   │   ├── trades.py        # Stock historical trades data source implementation
│   │   └── __init__.py      # Stocks sub-module exports
│   ├── options/             # Options data sources
│   │   ├── bars.py          # Options historical bars data source implementation
│   │   └── __init__.py      # Options sub-module exports
│   ├── crypto/              # Crypto data sources (placeholder)
│   │   └── __init__.py
│   └── __init__.py          # Package root
├── .github/
│   └── workflows/
│       └── lint.yml         # CI/CD workflow for code quality checks
├── pyproject.toml           # Poetry configuration and dependencies
├── README.md                # This file
├── CLAUDE.md                # Development guidelines for AI assistants
└── Test Historical API.ipynb  # Example notebook
```

### Key Components

- **Base Classes** (`common.py`): Universal abstract base classes (`BaseAlpacaDataSource`, `BaseAlpacaReader`) providing common functionality for all asset types and data types
- **Bars Abstract Classes** (`bars.py`): Abstract base classes (`AbstractBarsDataSource`, `AbstractBarsReader`) providing shared functionality for bars (OHLCV) data across all asset types
- **DataSource Implementations** (`stocks/bars.py`, `options/bars.py`, `stocks/trades.py`): Asset-specific implementations that extend the abstract base classes
- **DataSourceReader Implementations**: Implement the data fetching logic with PyArrow batch support for each data type
- **Partition Classes** (`common.py`): Enable parallel processing by distributing work across symbols and time ranges
- **Utility Functions** (`common.py`): Common functionality for URL building and API requests

## Development

### Building the Package

To build the package for distribution:

```bash
poetry build
```

This creates distribution packages in the `dist/` directory.

### Linting and Code Quality

The project uses several tools to maintain code quality. Run these checks before committing:

#### Code Formatting with Ruff

Format all Python files:

```bash
poetry run ruff format alpaca_pyspark/
```

Check formatting without making changes:

```bash
poetry run ruff format --check alpaca_pyspark/
```

#### Linting with Flake8

Check for code style issues:

```bash
poetry run flake8 alpaca_pyspark/
```

#### Type Checking with MyPy

Verify type hints and type safety:

```bash
poetry run mypy alpaca_pyspark/
```

#### Running All Checks

Run all quality checks at once:

```bash
poetry run ruff format --check alpaca_pyspark/ && \
poetry run flake8 alpaca_pyspark/ && \
poetry run mypy alpaca_pyspark/
```

#### Running Tests

Execute the test suite:

```bash
poetry run pytest
```

## Architecture

### Parallel Processing and Partitioning Strategies

The library uses intelligent partitioning to maximize parallel processing efficiency. Data requests are partitioned by **both stock symbol and time range**, allowing Spark to:

- Execute API requests in parallel across multiple executors
- Scale horizontally by adding more Spark workers
- Handle large numbers of symbols efficiently
- Distribute large time ranges across multiple partitions for better load balancing

#### Default Time-Range Partitioning

By default, all data sources partition requests into 1-day intervals. For example, if you request data for 3 symbols over a 7-day period, the library creates 21 partitions (3 symbols × 7 days), enabling highly parallel data fetching.

**Key behaviors:**
- If the time range is less than the partition interval (e.g., less than 1 day), no temporal partitioning occurs—only symbol-based partitioning is used
- Each partition fetches data for a single symbol within a specific time range
- Time range intervals are calculated automatically based on the start and end times

#### Bars-Specific Intelligent Partitioning

For historical bars data, the partitioning strategy is more sophisticated. The partition interval is calculated dynamically based on:

- **Timeframe**: The bar granularity (e.g., `1Min`, `1Hour`, `1Day`)
- **Limit**: Maximum bars per API request (default: 10,000)
- **Expected Pages**: Target number of API pages per partition (default: 5 pages)

This approach estimates how many bars to expect within a given time period and sizes partitions accordingly. For example:
- **Fine-grained data** (e.g., `1Min` bars over weeks/months): Creates smaller time ranges per partition to avoid overwhelming individual partitions with too many API pages
- **Coarse-grained data** (e.g., `1Day` bars over years): Creates larger time ranges per partition since fewer data points exist per unit of time

**Formula:**
```
partition_interval = total_time_range / max(1, ceil((total_time_range / timeframe) / (limit × pages_per_partition)))
```

This ensures that each partition handles approximately the same amount of data, regardless of the bar timeframe, leading to better load distribution across Spark executors.

### PyArrow Batching

The data sources use PyArrow RecordBatch objects for efficient data transfer between the API and Spark. This approach:

- Batches API pages into Arrow RecordBatch objects (default: up to 10,000 rows per API page)
- Reduces I/O overhead compared to row-by-row processing
- Provides significant performance improvements for large datasets
- Leverages Arrow's zero-copy capabilities for fast data transfer

### Error Handling

Built-in resilience features include:

- Automatic retry logic with exponential backoff (3 retries by default)
- Graceful handling of malformed data (logged as warnings, not failures)
- Connection timeout management
- Comprehensive error logging

### Class Hierarchy and Code Reuse

The library uses a layered inheritance architecture to maximize code reuse and maintain consistency across asset types:

```
BaseAlpacaDataSource (common.py)
├── AbstractBarsDataSource (bars.py)
│   ├── HistoricalBarsDataSource (stocks/bars.py)
│   └── HistoricalOptionBarsDataSource (options/bars.py)
└── HistoricalTradesDataSource (stocks/trades.py)

BaseAlpacaReader (common.py)
├── AbstractBarsReader (bars.py)
│   ├── HistoricalBarsReader (stocks/bars.py)
│   └── HistoricalOptionBarsReader (options/bars.py)
└── HistoricalTradesReader (stocks/trades.py)
```

**Benefits of this architecture:**

1. **Universal Base Classes** (`common.py`): Common functionality for all data sources including:
   - API authentication and request handling
   - Retry logic with exponential backoff
   - Symbol parsing and validation
   - Partition generation
   - PyArrow batch conversion

2. **Data-Type Abstract Classes** (`bars.py`): Shared functionality for specific data types across all asset types:
   - Bars (OHLCV) data: Common schema, timeframe parsing, partition sizing
   - Eliminates duplication between stocks bars and options bars implementations
   - Makes adding new asset types (e.g., crypto bars) require minimal code

3. **Asset-Specific Implementations**: Each implementation only needs to define:
   - DataSource name (following the `Alpaca_<AssetType>_<DataType>` pattern)
   - API endpoint path (e.g., `["stocks", "bars"]` vs `["options", "bars"]`)
   - Any asset-specific parsing logic (if needed)

This design follows the **DRY (Don't Repeat Yourself)** principle and makes the codebase highly maintainable and extensible.

## API Credentials

**Security Note**: Never commit API credentials to version control. Use one of these approaches:

- Environment variables
- Spark secrets management (e.g., Databricks secrets)
- Configuration files (add to `.gitignore`)
- Cloud secret managers (AWS Secrets Manager, Azure Key Vault, etc.)

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please ensure code follows the existing style and includes appropriate tests.
