# Options Data Sources

This module provides PySpark DataSources for retrieving options market data from the Alpaca Markets API.

## Available Data Sources

| DataSource Name | Python Class | Description |
|----------------|--------------|-------------|
| `Alpaca_Options_Bars` | `OptionBarsDataSource` | Historical OHLCV bars/candles for option contracts |
| `Alpaca_Options_Contracts` | `OptionsContractsDataSource` | Options contract listings and metadata |

---

## Alpaca_Options_Bars

Historical OHLCV (Open, High, Low, Close, Volume) bar/candle data for option contracts.

### Usage

```python
from alpaca_pyspark.options import OptionBarsDataSource

spark.dataSource.register(OptionBarsDataSource)

df = spark.read.format("Alpaca_Options_Bars").options(**{
    "symbols": ["AAPL250117C00150000", "AAPL250117P00150000"],
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
| `symbols` | Yes | - | List of option symbols in OCC format |
| `APCA-API-KEY-ID` | Yes | - | Alpaca API key ID |
| `APCA-API-SECRET-KEY` | Yes | - | Alpaca API secret key |
| `timeframe` | Yes | - | Bar timeframe (see values below) |
| `start` | Yes | - | Start date/time in ISO format |
| `end` | Yes | - | End date/time in ISO format |
| `endpoint` | No | `https://data.alpaca.markets/v1beta1` | API endpoint URL |
| `limit` | No | `10000` | Maximum bars per API request |

#### OCC Symbol Format

Option symbols follow the OCC (Options Clearing Corporation) format:
```
AAPL250117C00150000
│   │     ││       │
│   │     ││       └── Strike price ($150.00 = 00150000)
│   │     │└────────── Option type (C=Call, P=Put)
│   │     └─────────── Expiration date (YYMMDD)
│   └───────────────── Root symbol
```

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
| `symbol` | STRING | Option symbol (OCC format) |
| `time` | TIMESTAMP | Bar timestamp (UTC) |
| `open` | FLOAT | Opening price |
| `high` | FLOAT | High price |
| `low` | FLOAT | Low price |
| `close` | FLOAT | Closing price |
| `volume` | INT | Trading volume |
| `trade_count` | INT | Number of trades in bar |
| `vwap` | FLOAT | Volume-weighted average price |

---

## Alpaca_Options_Contracts

Lists available option contracts filtered by underlying symbol and other criteria. This is a discovery endpoint for finding contracts, not time-series data.

### Usage

```python
from alpaca_pyspark.options import OptionsContractsDataSource

spark.dataSource.register(OptionsContractsDataSource)

df = spark.read.format("Alpaca_Options_Contracts").options(**{
    "underlying_symbols": ["AAPL", "MSFT"],
    "APCA-API-KEY-ID": "your-api-key",
    "APCA-API-SECRET-KEY": "your-secret-key",
    "type": "call",
    "expiration_date_gte": "2025-01-01",
    "strike_price_gte": "100",
}).load()
```

### Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `underlying_symbols` | Yes | - | List of underlying stock symbols |
| `APCA-API-KEY-ID` | Yes | - | Alpaca API key ID |
| `APCA-API-SECRET-KEY` | Yes | - | Alpaca API secret key |
| `endpoint` | No | `https://paper-api.alpaca.markets/v2` | API endpoint URL |
| `limit` | No | `10000` | Maximum contracts per API request |
| `type` | No | - | Filter by option type: `"call"` or `"put"` |
| `strike_price_gte` | No | - | Minimum strike price |
| `strike_price_lte` | No | - | Maximum strike price |
| `expiration_date` | No | - | Exact expiration date (YYYY-MM-DD) |
| `expiration_date_gte` | No | - | Minimum expiration date |
| `expiration_date_lte` | No | - | Maximum expiration date |
| `root_symbol` | No | - | Filter by root symbol |
| `status` | No | - | Contract status filter |

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | Contract ID |
| `symbol` | STRING | OCC symbol (e.g., "AAPL250117C00150000") |
| `name` | STRING | Human-readable name (e.g., "AAPL Jan 17 2025 150 Call") |
| `status` | STRING | Contract status |
| `tradable` | BOOLEAN | Whether the contract is tradable |
| `expiration_date` | DATE | Expiration date |
| `root_symbol` | STRING | Root symbol |
| `underlying_symbol` | STRING | Underlying asset symbol |
| `underlying_asset_id` | STRING | Underlying asset ID |
| `type` | STRING | Option type ("call" or "put") |
| `style` | STRING | Option style ("american") |
| `strike_price` | DOUBLE | Strike price |
| `size` | INT | Contract size (typically 100) |
| `open_interest` | INT | Open interest (nullable) |
| `open_interest_date` | DATE | Date of open interest data (nullable) |
| `close_price` | DOUBLE | Last close price (nullable) |
| `close_price_date` | DATE | Date of close price (nullable) |

---

## API Documentation

For more details on the underlying APIs:
- [Alpaca Options Bars](https://docs.alpaca.markets/docs/historical-option-pricing-data)
- [Alpaca Options Contracts](https://docs.alpaca.markets/reference/get-options-contracts)
