# Usage Guide

This guide provides detailed information about using alpaca-pyspark data sources, including comprehensive configuration options, data schemas, and advanced usage patterns.

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

### Corporate Actions

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Corporate_Actions` | `CorporateActionsDataSource` | Corporate actions events (splits, dividends, etc.) |

## Detailed Configuration Options

### Required Options

All data sources require these base configuration options:

- **symbols**: List of symbols (e.g., `["AAPL", "MSFT"]`)
- **APCA-API-KEY-ID**: Your Alpaca API key ID
- **APCA-API-SECRET-KEY**: Your Alpaca API secret key
- **start**: Start date/time in ISO format with timezone
- **end**: End date/time in ISO format with timezone

### Data Source Specific Options

#### Historical Bars (Stocks & Options)

Additional required options:
- **timeframe**: Bar timeframe (e.g., `"1Min"`, `"5Min"`, `"15Min"`, `"30Min"`, `"1Hour"`, `"1Day"`)

Optional options:
- **limit**: Maximum bars per API request (default: 10000, max: 10000)
- **page_limit**: Maximum API pages to fetch per partition (default: 100)
- **feed**: Data feed to use (default: `"sip"` for stocks, `"indicative"` for options)
- **asof**: Point-in-time query timestamp for historical data reconstruction
- **adjustment**: Price adjustment method (`"raw"`, `"split"`, `"dividend"`, `"all"` - default: `"raw"`)

#### Historical Trades (Stocks)

Optional options:
- **limit**: Maximum trades per API request (default: 10000, max: 10000)
- **page_limit**: Maximum API pages to fetch per partition (default: 100)
- **feed**: Data feed to use (default: `"sip"`)
- **asof**: Point-in-time query timestamp for historical data reconstruction

#### Corporate Actions

Optional options:
- **ca_types**: Comma-separated list of corporate action types to include (default: all types)
- **since**: Filter actions that occurred on or after this date
- **until**: Filter actions that occurred on or before this date

### Global Options

These options apply to all data sources:

- **endpoint**: API endpoint URL (defaults to production: `"https://data.alpaca.markets"`)
- **timeout**: Request timeout in seconds (default: 30)
- **retries**: Number of retry attempts for failed requests (default: 3)

## Data Schemas

### Historical Bars Schema

Both stocks and options bars use the same schema:

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Stock/option symbol |
| time | TIMESTAMP | Bar timestamp (timezone-aware) |
| open | FLOAT | Opening price |
| high | FLOAT | High price |
| low | FLOAT | Low price |
| close | FLOAT | Closing price |
| volume | INT | Trading volume |
| trade_count | INT | Number of trades within the bar |
| vwap | FLOAT | Volume-weighted average price |

### Historical Trades Schema

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Stock symbol |
| time | TIMESTAMP | Trade timestamp (timezone-aware) |
| price | FLOAT | Trade price |
| size | INT | Trade size (number of shares) |
| conditions | STRING | Trade conditions/qualifiers |
| exchange | STRING | Exchange where trade occurred |
| tape | STRING | Tape identifier |

### Corporate Actions Schema

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Stock symbol |
| ca_type | STRING | Type of corporate action |
| ca_sub_type | STRING | Sub-type of corporate action |
| initiating_symbol | STRING | Symbol that initiated the action |
| target_symbol | STRING | Target symbol of the action |
| date | DATE | Date of the corporate action |
| rate | FLOAT | Rate or ratio of the action |
| special_date | DATE | Special date related to the action |
| payable_date | DATE | Date when action is payable |
| record_date | DATE | Record date for the action |
| declared_date | DATE | Date action was declared |
| ex_date | DATE | Ex-dividend/ex-rights date |

## Usage Examples

### Basic Stock Bars Example

```python
import datetime as dt
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from alpaca_pyspark.stocks import HistoricalBarsDataSource

# Initialize Spark session (requires Spark 4.0+)
spark = SparkSession.builder.appName("AlpacaExample").getOrCreate()

# Register the data source
spark.dataSource.register(HistoricalBarsDataSource)

# Configure timezone
tz = ZoneInfo("America/New_York")

# Configure options
options = {
    "symbols": ["AAPL", "MSFT", "GOOG"],
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "timeframe": "1Day",
    "start": dt.datetime(2021, 1, 1, tzinfo=tz).isoformat(),
    "end": dt.datetime(2022, 1, 1, tzinfo=tz).isoformat(),
    "limit": 1000
}

# Load data as DataFrame
df = (spark.read
      .format("Alpaca_Stocks_Bars")
      .options(**options)
      .load())

# Use the DataFrame
df.show()
df.createOrReplaceTempView("bars")
spark.sql("SELECT symbol, time, close FROM bars WHERE symbol = 'AAPL'").show()
```

### Stock Trades Example

```python
from alpaca_pyspark.stocks import HistoricalTradesDataSource

spark.dataSource.register(HistoricalTradesDataSource)

# Configure for trades (no timeframe required)
trades_options = {
    "symbols": ["AAPL"],
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "start": dt.datetime(2024, 1, 15, 9, 30, tzinfo=tz).isoformat(),
    "end": dt.datetime(2024, 1, 15, 16, 0, tzinfo=tz).isoformat(),
    "limit": 5000
}

trades_df = (spark.read
             .format("Alpaca_Stocks_Trades")
             .options(**trades_options)
             .load())

trades_df.show()
```

### Options Bars Example

```python
from alpaca_pyspark.options import HistoricalOptionBarsDataSource

spark.dataSource.register(HistoricalOptionBarsDataSource)

# Note: options symbols use specific format
options_symbols = [
    "AAPL241220C00150000",  # AAPL call option
    "MSFT241220P00300000"   # MSFT put option
]

options_bars_options = {
    "symbols": options_symbols,
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "timeframe": "1Hour",
    "start": dt.datetime(2024, 12, 1, tzinfo=tz).isoformat(),
    "end": dt.datetime(2024, 12, 20, tzinfo=tz).isoformat()
}

options_df = (spark.read
              .format("Alpaca_Options_Bars")
              .options(**options_bars_options)
              .load())

options_df.show()
```

### Corporate Actions Example

```python
from alpaca_pyspark.corp_actions import CorporateActionsDataSource

spark.dataSource.register(CorporateActionsDataSource)

corp_actions_options = {
    "symbols": ["AAPL", "MSFT"],
    "APCA-API-KEY-ID": "your-api-key-id",
    "APCA-API-SECRET-KEY": "your-api-secret-key",
    "start": "2020-01-01",
    "end": "2024-12-31",
    "ca_types": "dividend,split"  # Only get dividends and splits
}

corp_actions_df = (spark.read
                   .format("Alpaca_Corporate_Actions")
                   .options(**corp_actions_options)
                   .load())

corp_actions_df.show()
```

### Advanced: Using Multiple Data Sources

```python
# Load both bars and trades for analysis
bars_df = (spark.read
           .format("Alpaca_Stocks_Bars")
           .options(**bars_options)
           .load())

trades_df = (spark.read
             .format("Alpaca_Stocks_Trades")  
             .options(**trades_options)
             .load())

# Create views for SQL analysis
bars_df.createOrReplaceTempView("bars")
trades_df.createOrReplaceTempView("trades")

# Analyze average trade size per bar
result = spark.sql("""
    SELECT 
        b.symbol,
        b.time as bar_time,
        b.volume as bar_volume,
        COUNT(t.size) as trade_count,
        AVG(t.size) as avg_trade_size
    FROM bars b
    JOIN trades t ON b.symbol = t.symbol 
        AND t.time >= b.time 
        AND t.time < b.time + INTERVAL 1 DAY
    GROUP BY b.symbol, b.time, b.volume
    ORDER BY b.symbol, b.time
""")

result.show()
```

## Performance Optimization

### Partitioning Strategy

The library automatically partitions data requests by symbol and time range for optimal parallel processing:

- **Symbol partitioning**: Each symbol is processed independently
- **Time range partitioning**: Large date ranges are split into smaller chunks
- **Dynamic sizing**: Partition size adapts to timeframe and expected data volume

### Tuning Parameters

For better performance with large datasets:

```python
# Optimize for high-frequency data (1-minute bars)
high_freq_options = {
    "timeframe": "1Min",
    "limit": 10000,        # Maximum bars per API request
    "page_limit": 10,      # Limit pages per partition for memory
    # ... other options
}

# Optimize for daily data over long periods
daily_options = {
    "timeframe": "1Day", 
    "limit": 10000,
    "page_limit": 100,     # Allow more pages for sparse daily data
    # ... other options
}
```

### Memory Management

For very large datasets:

1. **Increase Spark executor memory**: Configure `spark.executor.memory`
2. **Limit concurrent partitions**: Use `spark.sql.adaptive.coalescePartitions.enabled=true`
3. **Use checkpointing**: Call `df.checkpoint()` for iterative processing

## Error Handling and Troubleshooting

### Common Issues

#### Authentication Errors
```
Error: 401 Unauthorized
```
- Verify your API key ID and secret key
- Ensure your Alpaca account has market data permissions
- Check if you're using the correct endpoint (sandbox vs. live)

#### Rate Limiting
```
Error: 429 Too Many Requests  
```
- The library includes automatic retry with exponential backoff
- Consider reducing the number of symbols or time range per job
- For high-frequency requests, implement delays between jobs

#### Data Availability
```
Warning: No data returned for symbol XYZ
```
- Check if the symbol is valid and exists for the requested time period
- Verify market hours for your requested time range
- Some symbols may not have data for all timeframes

### Debugging Tips

Enable verbose logging:

```python
# Enable Spark SQL debugging
spark.sparkContext.setLogLevel("INFO")

# Check partition information
df.rdd.getNumPartitions()  # Number of partitions
df.explain(True)           # Execution plan
```

Monitor API requests:
- Check the logs for API URLs and response codes
- Verify request parameters are correctly formatted
- Monitor rate limit headers in responses

## Security Best Practices

### API Credentials Management

**Never hardcode credentials in your code.** Use one of these secure approaches:

#### Environment Variables
```python
import os

options = {
    "APCA-API-KEY-ID": os.getenv("APCA_API_KEY_ID"),
    "APCA-API-SECRET-KEY": os.getenv("APCA_API_SECRET_KEY"),
    # ... other options
}
```

#### Spark Configuration
```python
# Set in Spark configuration
spark.conf.set("spark.alpaca.api.key", "your-key-id")
spark.conf.set("spark.alpaca.api.secret", "your-secret-key")

options = {
    "APCA-API-KEY-ID": spark.conf.get("spark.alpaca.api.key"),
    "APCA-API-SECRET-KEY": spark.conf.get("spark.alpaca.api.secret"),
    # ... other options
}
```

#### Cloud Secret Managers
```python
# Example with AWS Secrets Manager
import boto3

secrets_client = boto3.client('secretsmanager')
secret = secrets_client.get_secret_value(SecretId='alpaca-api-credentials')
credentials = json.loads(secret['SecretString'])

options = {
    "APCA-API-KEY-ID": credentials["key_id"],
    "APCA-API-SECRET-KEY": credentials["secret_key"],
    # ... other options
}
```

### Databricks Integration

For Databricks users:

```python
# Use Databricks secrets
options = {
    "APCA-API-KEY-ID": dbutils.secrets.get("alpaca", "api-key-id"),
    "APCA-API-SECRET-KEY": dbutils.secrets.get("alpaca", "api-secret-key"),
    # ... other options
}
```

## API Reference Links

- [Alpaca Market Data API Documentation](https://docs.alpaca.markets/docs/about-market-data-api) - Complete API specification, endpoints, and data formats
- [PySpark DataSource API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) - Tutorial and reference for implementing custom DataSources
- [PyArrow Documentation](https://arrow.apache.org/docs/python/) - High-performance columnar data processing library used for batching
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) - Comprehensive guide to Spark SQL features and syntax