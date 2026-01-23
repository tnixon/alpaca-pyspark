"""Tests for schema changes to handle large values and high precision.

Tests verify that the migration from INT32 to INT64 and FLOAT32 to FLOAT64
properly handles edge cases that would overflow the original data types.
"""

import pytest
import pyarrow as pa
from datetime import datetime as dt
from typing import Dict, Any

from alpaca_pyspark.bars import AbstractBarsReader, AbstractBarsDataSource
from alpaca_pyspark.stocks.trades import HistoricalStockTradesReader, StockTradesDataSource


@pytest.mark.unit
class TestLargeValueHandling:
    """Tests for handling values that exceed INT32 and FLOAT32 limits."""

    def test_large_volume_parsing_bars(self, base_options):
        """Test that bars can handle volumes exceeding INT32 max value (2^31 - 1 = 2,147,483,647)."""
        # Create a concrete implementation for testing
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        # Test volume just above INT32 limit
        large_volume = 3_000_000_000  # 3 billion, exceeds INT32 max
        bar_data = {
            "t": "2021-01-01T09:30:00Z",
            "o": 131.5,
            "h": 133.0,
            "l": 131.0,
            "c": 132.5,
            "v": large_volume,  # Large volume
            "n": 5500,
            "vw": 132.0,
        }

        # Create reader instance
        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        reader = datasource.reader(None)

        # Test that parsing succeeds with large volume
        result = reader._parse_record("AAPL", bar_data)
        
        assert result[0] == "AAPL"  # symbol
        assert result[6] == large_volume  # volume (index 6 in BarTuple)
        assert isinstance(result[6], int)  # Ensure it's still an int

    def test_large_trade_count_parsing_bars(self, base_options):
        """Test that bars can handle trade counts exceeding INT32 max value."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        # Test trade count just above INT32 limit
        large_trade_count = 2_500_000_000  # 2.5 billion
        bar_data = {
            "t": "2021-01-01T09:30:00Z",
            "o": 131.5,
            "h": 133.0,
            "l": 131.0,
            "c": 132.5,
            "v": 1000000,
            "n": large_trade_count,  # Large trade count
            "vw": 132.0,
        }

        # Create reader instance
        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        reader = datasource.reader(None)

        # Test that parsing succeeds with large trade count
        result = reader._parse_record("AAPL", bar_data)
        
        assert result[0] == "AAPL"  # symbol
        assert result[7] == large_trade_count  # trade_count (index 7 in BarTuple)
        assert isinstance(result[7], int)

    def test_high_precision_prices_bars(self, base_options):
        """Test that bars maintain precision for high-value prices that would lose precision in FLOAT32."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        # Test high-precision price that would lose precision in FLOAT32
        # FLOAT32 has ~7 decimal digits of precision
        high_precision_price = 123456.789012345  # 15 significant digits
        bar_data = {
            "t": "2021-01-01T09:30:00Z",
            "o": high_precision_price,
            "h": high_precision_price + 1.0,
            "l": high_precision_price - 1.0,
            "c": high_precision_price + 0.5,
            "v": 1000000,
            "n": 5500,
            "vw": high_precision_price,
        }

        # Create reader instance
        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        reader = datasource.reader(None)

        # Test that parsing maintains precision
        result = reader._parse_record("AAPL", bar_data)
        
        assert result[0] == "AAPL"  # symbol
        assert result[2] == high_precision_price  # open price (index 2)
        assert result[8] == high_precision_price  # vwap price (index 8)
        assert isinstance(result[2], float)

    def test_large_trade_size_trades(self, base_options):
        """Test that trades can handle sizes exceeding INT32 max value."""
        # Test trade size just above INT32 limit
        large_size = 3_000_000_000  # 3 billion shares
        trade_data = {
            "t": "2021-01-01T09:30:00Z",
            "x": "V",  # exchange
            "p": 131.0,  # price
            "s": large_size,  # Large trade size
            "c": ["@", "I"],  # conditions
            "i": 12345,  # id
            "z": "C",  # tape
        }

        # Create reader instance
        reader = HistoricalStockTradesReader({}, StockTradesDataSource(base_options).pa_schema, base_options)

        # Test that parsing succeeds with large trade size
        result = reader._parse_record("AAPL", trade_data)
        
        assert result[0] == "AAPL"  # symbol
        assert result[4] == large_size  # size (index 4 in TradeTuple)
        assert isinstance(result[4], int)

    def test_high_precision_trade_price(self, base_options):
        """Test that trades maintain precision for high-value prices."""
        # Test high-precision trade price
        high_precision_price = 987654.321098765  # 15 significant digits
        trade_data = {
            "t": "2021-01-01T09:30:00Z",
            "x": "V",
            "p": high_precision_price,  # High precision price
            "s": 100,
            "c": [],
            "i": 12345,
            "z": "C",
        }

        # Create reader instance  
        reader = HistoricalStockTradesReader({}, StockTradesDataSource(base_options).pa_schema, base_options)

        # Test that parsing maintains precision
        result = reader._parse_record("AAPL", trade_data)
        
        assert result[0] == "AAPL"  # symbol
        assert result[3] == high_precision_price  # price (index 3 in TradeTuple)
        assert isinstance(result[3], float)


@pytest.mark.unit
class TestPyArrowSchemaTypes:
    """Tests that PyArrow schemas use the correct 64-bit types."""

    def test_bars_pyarrow_schema_uses_64bit_types(self, base_options):
        """Verify that bars PyArrow schema uses int64 and float64 types."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        schema = datasource.pa_schema

        # Check that price fields use float64
        assert schema.field("open").type == pa.float64()
        assert schema.field("high").type == pa.float64()
        assert schema.field("low").type == pa.float64()
        assert schema.field("close").type == pa.float64()
        assert schema.field("vwap").type == pa.float64()

        # Check that count fields use int64
        assert schema.field("volume").type == pa.int64()
        assert schema.field("trade_count").type == pa.int64()

    def test_trades_pyarrow_schema_uses_64bit_types(self, base_options):
        """Verify that trades PyArrow schema uses int64 and float64 types."""
        datasource = StockTradesDataSource(base_options)
        schema = datasource.pa_schema

        # Check that price field uses float64
        assert schema.field("price").type == pa.float64()

        # Check that size field uses int64
        assert schema.field("size").type == pa.int64()
        assert schema.field("id").type == pa.int64()  # Trade ID should also be int64


@pytest.mark.unit  
class TestSparkSQLSchemaTypes:
    """Tests that Spark SQL schemas use the correct 64-bit types."""

    def test_bars_spark_schema_uses_64bit_types(self, base_options):
        """Verify that bars Spark SQL schema uses BIGINT and DOUBLE types."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        schema_str = datasource.schema()

        # Check that the schema string contains DOUBLE for price fields
        assert "open DOUBLE" in schema_str
        assert "high DOUBLE" in schema_str
        assert "low DOUBLE" in schema_str
        assert "close DOUBLE" in schema_str
        assert "vwap DOUBLE" in schema_str

        # Check that the schema string contains BIGINT for count fields
        assert "volume BIGINT" in schema_str
        assert "trade_count BIGINT" in schema_str

    def test_trades_spark_schema_uses_64bit_types(self, base_options):
        """Verify that trades Spark SQL schema uses BIGINT and DOUBLE types."""
        datasource = StockTradesDataSource(base_options)
        schema_str = datasource.schema()

        # Check that the schema string contains DOUBLE for price field
        assert "price DOUBLE" in schema_str

        # Check that the schema string contains BIGINT for size field
        assert "size BIGINT" in schema_str
        assert "id BIGINT" in schema_str


@pytest.mark.unit
class TestEdgeCaseValues:
    """Tests for specific edge case values near type boundaries."""

    def test_int32_boundary_values(self, base_options):
        """Test values at INT32 boundaries (2^31 - 1 and 2^31)."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        reader = datasource.reader(None)

        # Test exactly at INT32 max value
        int32_max = 2147483647  # 2^31 - 1
        bar_data = {
            "t": "2021-01-01T09:30:00Z",
            "o": 131.5,
            "h": 133.0,
            "l": 131.0,
            "c": 132.5,
            "v": int32_max,  # Exactly at INT32 boundary
            "n": 5500,
            "vw": 132.0,
        }

        result = reader._parse_record("AAPL", bar_data)
        assert result[6] == int32_max

        # Test just above INT32 max value (would overflow INT32)
        int32_overflow = 2147483648  # 2^31 (one more than INT32 max)
        bar_data["v"] = int32_overflow
        
        result = reader._parse_record("AAPL", bar_data)
        assert result[6] == int32_overflow  # Should work with INT64

    def test_float32_precision_loss_prevention(self, base_options):
        """Test that FLOAT64 prevents precision loss that would occur with FLOAT32."""
        class TestBarsDataSource(AbstractBarsDataSource):
            @classmethod
            def name(cls) -> str:
                return "Test_Bars"
            
            def reader(self, schema):
                return TestBarsReader(self.config, self.pa_schema, self.params)

        class TestBarsReader(AbstractBarsReader):
            @property
            def path_elements(self):
                return ["test", "bars"]

        test_options = {**base_options, "timeframe": "1Day"}
        datasource = TestBarsDataSource(test_options)
        reader = datasource.reader(None)

        # This value would lose precision in FLOAT32 but should be preserved in FLOAT64
        precise_value = 16777217.0  # 2^24 + 1 (where FLOAT32 starts losing integer precision)
        
        bar_data = {
            "t": "2021-01-01T09:30:00Z",
            "o": precise_value,
            "h": precise_value,
            "l": precise_value,
            "c": precise_value,
            "v": 1000000,
            "n": 5500,
            "vw": precise_value,
        }

        result = reader._parse_record("AAPL", bar_data)
        
        # All price fields should maintain exact precision
        assert result[2] == precise_value  # open
        assert result[3] == precise_value  # high
        assert result[4] == precise_value  # low
        assert result[5] == precise_value  # close
        assert result[8] == precise_value  # vwap