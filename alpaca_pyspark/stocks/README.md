# Stocks Data Sources

This module provides PySpark DataSources for retrieving historical stock market data from the Alpaca Markets API.

## Available Data Sources

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Stocks_Bars` | `StockBarsDataSource` | Historical OHLCV bars/candles |
| `Alpaca_Stocks_Trades` | `StockTradesDataSource` | Historical tick-by-tick trades |

---

## Alpaca_Stocks_Bars

Historical OHLCV (Open, High, Low, Close, Volume) bar/candle data for stocks.

### Usage

```python
from alpaca_pyspark.stocks import StockBarsDataSource

spark.dataSource.register(StockBarsDataSource)

df = spark.read.format("Alpaca_Stocks_Bars").options(**{
    "symbols": ["AAPL", "MSFT", "GOOG"],
    "APCA-API-KEY-ID": "your-api-key",
    "APCA-API-SECRET-KEY": "your-secret-key",
    "timeframe": "1Day",
    "start": "2024-01-01T00:00:00-05:00",
    "end": "2024-12-31T23:59:59-05:00",
}).load()
```

### Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `symbols` | Yes | - | List of stock symbols (e.g., `["AAPL", "MSFT"]`) |
| `APCA-API-KEY-ID` | Yes | - | Alpaca API key ID |
| `APCA-API-SECRET-KEY` | Yes | - | Alpaca API secret key |
| `timeframe` | Yes | - | Bar timeframe (see values below) |
| `start` | Yes | - | Start date/time in ISO format |
| `end` | Yes | - | End date/time in ISO format |
| `endpoint` | No | `https://data.alpaca.markets/v2` | API endpoint URL |
| `limit` | No | `10000` | Maximum bars per API request |

#### Timeframe Values

| Value | Description |
|-------|-------------|
| `1Min`, `5Min`, `15Min`, `30Min` | Minute bars |
| `1Hour`, `4Hour` | Hourly bars |
| `1Day` | Daily bars |
| `1Week` | Weekly bars |
| `1Month` | Monthly bars |

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | STRING | Stock symbol |
| `time` | TIMESTAMP | Bar timestamp (UTC) |
| `open` | FLOAT | Opening price |
| `high` | FLOAT | High price |
| `low` | FLOAT | Low price |
| `close` | FLOAT | Closing price |
| `volume` | INT | Trading volume |
| `trade_count` | INT | Number of trades in bar |
| `vwap` | FLOAT | Volume-weighted average price |

---

## Alpaca_Stocks_Trades

Historical tick-by-tick trade data for stocks.

### Usage

```python
from alpaca_pyspark.stocks import StockTradesDataSource

spark.dataSource.register(StockTradesDataSource)

df = spark.read.format("Alpaca_Stocks_Trades").options(**{
    "symbols": ["AAPL", "MSFT"],
    "APCA-API-KEY-ID": "your-api-key",
    "APCA-API-SECRET-KEY": "your-secret-key",
    "start": "2024-01-15T09:30:00-05:00",
    "end": "2024-01-15T16:00:00-05:00",
}).load()
```

### Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `symbols` | Yes | - | List of stock symbols (e.g., `["AAPL", "MSFT"]`) |
| `APCA-API-KEY-ID` | Yes | - | Alpaca API key ID |
| `APCA-API-SECRET-KEY` | Yes | - | Alpaca API secret key |
| `start` | Yes | - | Start date/time in ISO format |
| `end` | Yes | - | End date/time in ISO format |
| `endpoint` | No | `https://data.alpaca.markets/v2` | API endpoint URL |
| `limit` | No | `10000` | Maximum trades per API request |

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | STRING | Stock symbol |
| `time` | TIMESTAMP | Trade timestamp (UTC) |
| `exchange` | STRING | Exchange code |
| `price` | FLOAT | Trade price |
| `size` | INT | Trade size (shares) |
| `conditions` | STRING | Trade conditions (comma-separated) |
| `id` | BIGINT | Trade ID |
| `tape` | STRING | Tape (A, B, or C) |

---

## API Documentation

For more details on the underlying API:
- [Alpaca Stock Bars](https://docs.alpaca.markets/docs/historical-stock-pricing-data)
- [Alpaca Stock Trades](https://docs.alpaca.markets/docs/historical-stock-trades)
