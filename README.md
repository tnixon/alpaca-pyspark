# alpaca-pyspark

A high-performance PySpark connector for importing market data from the Alpaca Markets API in a distributed fashion.

## Overview

**alpaca-pyspark** provides custom PySpark DataSource implementations that enable efficient, parallel retrieval of market data from Alpaca Markets. The library leverages PySpark's distributed computing capabilities to fetch data across multiple stock symbols concurrently, with built-in retry logic, error handling, and PyArrow batch processing for optimal performance.

### Key Features

- **Distributed Data Fetching**: Automatically parallelizes API requests across stock symbols
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
from alpaca_pyspark import HistoricalBarsDataSource

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
      .format("Alpaca_HistoricalBars")
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
- **limit**: Maximum bars per API request (default: 1000)

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
│   ├── bars.py              # Historical bars data source implementation
│   ├── common.py            # Shared utilities and partitioning logic
│   └── __init__.py          # Package exports
├── pyproject.toml           # Poetry configuration and dependencies
├── README.md                # This file
└── Test Historical Bars DS.ipynb  # Example notebook
```

### Key Components

- **DataSource Classes**: Define schema and validate options for each data type
- **DataSourceReader Classes**: Implement the data fetching logic with PyArrow batch support
- **Partition Classes**: Enable parallel processing by distributing work across symbols
- **Utility Functions**: Common functionality for URL building and API requests

## Development

### Building the Package

To build the package for distribution:

```bash
poetry build
```

This creates distribution packages in the `dist/` directory.

### Linting and Code Quality

The project uses several tools to maintain code quality. Run these checks before committing:

#### Code Formatting with YAPF

Format all Python files:

```bash
poetry run yapf -ir alpaca_pyspark/
```

Check formatting without making changes:

```bash
poetry run yapf --diff --recursive alpaca_pyspark/
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
poetry run yapf --diff --recursive alpaca_pyspark/ && \
poetry run flake8 alpaca_pyspark/ && \
poetry run mypy alpaca_pyspark/
```

#### Running Tests

Execute the test suite:

```bash
poetry run pytest
```

## Architecture

### Parallel Processing

The library partitions data requests by stock symbol, allowing Spark to:

- Execute API requests in parallel across multiple executors
- Scale horizontally by adding more Spark workers
- Handle large numbers of symbols efficiently

### PyArrow Batching

The data sources use PyArrow RecordBatch objects for efficient data transfer between the API and Spark. This approach:

- Batches rows into groups (default: 10,000 rows per batch)
- Reduces I/O overhead compared to row-by-row processing
- Provides significant performance improvements for large datasets

### Error Handling

Built-in resilience features include:

- Automatic retry logic with exponential backoff (3 retries by default)
- Graceful handling of malformed data (logged as warnings, not failures)
- Connection timeout management
- Comprehensive error logging

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
