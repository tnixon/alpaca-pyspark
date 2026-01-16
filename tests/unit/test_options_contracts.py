"""Tests for alpaca_pyspark/options/contracts.py - Options contracts data source."""

import pytest
from datetime import date

from alpaca_pyspark.options.contracts import (
    OptionsContractsDataSource,
    OptionsContractsReader,
)
from common import SymbolPartition


@pytest.fixture
def contracts_options(api_credentials):
    """Options dict for OptionsContractsDataSource testing."""
    return {
        "underlying_symbols": "['AAPL', 'MSFT']",
        **api_credentials,
    }


@pytest.fixture
def contracts_options_list(api_credentials):
    """Options dict with symbols as list for testing."""
    return {
        "underlying_symbols": ["AAPL", "MSFT"],
        **api_credentials,
    }


@pytest.mark.unit
class TestSymbolPartition:
    """Tests for SymbolPartition dataclass."""

    def test_partition_creation(self):
        """Test creating a partition with symbol."""
        partition = SymbolPartition(symbol="AAPL")

        assert partition.symbol == "AAPL"

    def test_partition_equality(self):
        """Test that partitions with same values are equal."""
        partition1 = SymbolPartition(symbol="AAPL")
        partition2 = SymbolPartition(symbol="AAPL")

        assert partition1 == partition2

    def test_partition_inequality(self):
        """Test that partitions with different values are not equal."""
        partition1 = SymbolPartition(symbol="AAPL")
        partition2 = SymbolPartition(symbol="MSFT")

        assert partition1 != partition2


@pytest.mark.unit
class TestOptionsContractsDataSourceValidation:
    """Tests for OptionsContractsDataSource validation."""

    def test_valid_options_string_symbols(self, contracts_options):
        """Test DataSource creation with valid string symbols."""
        datasource = OptionsContractsDataSource(contracts_options)
        assert datasource is not None

    def test_valid_options_list_symbols(self, contracts_options_list):
        """Test DataSource creation with list symbols."""
        datasource = OptionsContractsDataSource(contracts_options_list)
        assert datasource is not None

    def test_missing_underlying_symbols(self, api_credentials):
        """Test that missing underlying_symbols raises ValueError."""
        options = {**api_credentials}

        with pytest.raises(ValueError) as exc_info:
            OptionsContractsDataSource(options)

        assert "underlying_symbols" in str(exc_info.value)

    def test_missing_api_key(self):
        """Test that missing API key raises ValueError."""
        options = {
            "underlying_symbols": "['AAPL']",
            "APCA-API-SECRET-KEY": "test-secret",
        }

        with pytest.raises(ValueError) as exc_info:
            OptionsContractsDataSource(options)

        assert "APCA-API-KEY-ID" in str(exc_info.value)

    def test_empty_symbols_list(self, api_credentials):
        """Test that empty symbols list raises ValueError."""
        options = {
            "underlying_symbols": "[]",
            **api_credentials,
        }

        with pytest.raises(ValueError) as exc_info:
            OptionsContractsDataSource(options)

        assert "non-empty" in str(exc_info.value)

    def test_invalid_symbols_format(self, api_credentials):
        """Test that invalid symbols format raises ValueError."""
        options = {
            "underlying_symbols": "not a valid list",
            **api_credentials,
        }

        with pytest.raises(ValueError) as exc_info:
            OptionsContractsDataSource(options)

        assert "Invalid underlying_symbols format" in str(exc_info.value)

    def test_invalid_type_option(self, contracts_options):
        """Test that invalid type option raises ValueError."""
        options = {
            **contracts_options,
            "type": "invalid",
        }

        with pytest.raises(ValueError) as exc_info:
            OptionsContractsDataSource(options)

        assert "type must be 'call' or 'put'" in str(exc_info.value)

    def test_valid_type_option_call(self, contracts_options):
        """Test that type='call' is valid."""
        options = {**contracts_options, "type": "call"}
        datasource = OptionsContractsDataSource(options)
        assert datasource is not None

    def test_valid_type_option_put(self, contracts_options):
        """Test that type='put' is valid."""
        options = {**contracts_options, "type": "put"}
        datasource = OptionsContractsDataSource(options)
        assert datasource is not None


@pytest.mark.unit
class TestOptionsContractsDataSourceSchema:
    """Tests for OptionsContractsDataSource schema."""

    def test_schema_returns_string(self, contracts_options):
        """Test that schema() returns a DDL string."""
        datasource = OptionsContractsDataSource(contracts_options)
        schema = datasource.schema()

        assert isinstance(schema, str)
        assert "id STRING" in schema
        assert "symbol STRING" in schema
        assert "strike_price DOUBLE" in schema
        assert "expiration_date DATE" in schema

    def test_pa_schema_field_count(self, contracts_options):
        """Test that PyArrow schema has correct number of fields."""
        datasource = OptionsContractsDataSource(contracts_options)
        pa_schema = datasource.pa_schema

        # 17 fields as per the plan
        assert len(pa_schema) == 17

    def test_pa_schema_field_names(self, contracts_options):
        """Test that PyArrow schema has expected field names."""
        datasource = OptionsContractsDataSource(contracts_options)
        pa_schema = datasource.pa_schema

        expected_fields = [
            "id",
            "symbol",
            "name",
            "status",
            "tradable",
            "expiration_date",
            "root_symbol",
            "underlying_symbol",
            "underlying_asset_id",
            "type",
            "style",
            "strike_price",
            "size",
            "open_interest",
            "open_interest_date",
            "close_price",
            "close_price_date",
        ]

        actual_fields = [field.name for field in pa_schema]
        assert actual_fields == expected_fields


@pytest.mark.unit
class TestOptionsContractsReader:
    """Tests for OptionsContractsReader."""

    def test_underlying_symbols_parsing_string(self, contracts_options):
        """Test that string symbols are parsed correctly."""
        datasource = OptionsContractsDataSource(contracts_options)
        reader = datasource.reader(None)

        assert reader.underlying_symbols == ["AAPL", "MSFT"]

    def test_underlying_symbols_parsing_list(self, contracts_options_list):
        """Test that list symbols are handled correctly."""
        datasource = OptionsContractsDataSource(contracts_options_list)
        reader = datasource.reader(None)

        assert reader.underlying_symbols == ["AAPL", "MSFT"]

    def test_partitions_creation(self, contracts_options):
        """Test that partitions are created for each symbol."""
        datasource = OptionsContractsDataSource(contracts_options)
        reader = datasource.reader(None)
        partitions = reader.partitions()

        assert len(partitions) == 2
        assert partitions[0].symbol == "AAPL"
        assert partitions[1].symbol == "MSFT"

    def test_path_elements(self, contracts_options):
        """Test that path_elements returns correct path."""
        datasource = OptionsContractsDataSource(contracts_options)
        reader = datasource.reader(None)

        assert reader.path_elements == ["options", "contracts"]

    def test_api_params_basic(self, contracts_options):
        """Test api_params with basic options."""
        datasource = OptionsContractsDataSource(contracts_options)
        reader = datasource.reader(None)
        partition = SymbolPartition(symbol="AAPL")

        params = reader.api_params(partition)

        assert params["underlying_symbols"] == "AAPL"
        assert params["limit"] == 10000

    def test_api_params_with_filters(self, contracts_options):
        """Test api_params includes filter options."""
        options = {
            **contracts_options,
            "type": "call",
            "strike_price_gte": "100",
            "expiration_date_gte": "2025-01-01",
        }
        datasource = OptionsContractsDataSource(options)
        reader = datasource.reader(None)
        partition = SymbolPartition(symbol="AAPL")

        params = reader.api_params(partition)

        assert params["type"] == "call"
        assert params["strike_price_gte"] == "100"
        assert params["expiration_date_gte"] == "2025-01-01"

    def test_default_limit(self, contracts_options):
        """Test that default limit is 10000."""
        datasource = OptionsContractsDataSource(contracts_options)
        reader = datasource.reader(None)

        assert reader.limit == 10000

    def test_custom_limit(self, contracts_options):
        """Test that custom limit is used."""
        options = {**contracts_options, "limit": "5000"}
        datasource = OptionsContractsDataSource(options)
        reader = datasource.reader(None)

        assert reader.limit == 5000


@pytest.mark.unit
class TestParseRecord:
    """Tests for _parse_record method."""

    @pytest.fixture
    def reader(self, contracts_options):
        """Create a reader instance for testing."""
        datasource = OptionsContractsDataSource(contracts_options)
        return datasource.reader(None)

    def test_parse_complete_record(self, reader):
        """Test parsing a complete contract record."""
        record = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            "name": "AAPL Jan 17 2025 150 Call",
            "status": "active",
            "tradable": True,
            "expiration_date": "2025-01-17",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
            "open_interest": 5000,
            "open_interest_date": "2025-01-10",
            "close_price": "5.25",
            "close_price_date": "2025-01-10",
        }

        result = reader._parse_record(record)

        assert result[0] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"  # id
        assert result[1] == "AAPL250117C00150000"  # symbol
        assert result[2] == "AAPL Jan 17 2025 150 Call"  # name
        assert result[3] == "active"  # status
        assert result[4] is True  # tradable
        assert result[5] == date(2025, 1, 17)  # expiration_date
        assert result[6] == "AAPL"  # root_symbol
        assert result[7] == "AAPL"  # underlying_symbol
        assert result[9] == "call"  # type
        assert result[10] == "american"  # style
        assert result[11] == 150.0  # strike_price
        assert result[12] == 100  # size
        assert result[13] == 5000  # open_interest
        assert result[14] == date(2025, 1, 10)  # open_interest_date
        assert result[15] == 5.25  # close_price
        assert result[16] == date(2025, 1, 10)  # close_price_date

    def test_parse_record_with_missing_optional_fields(self, reader):
        """Test parsing a record with missing optional fields."""
        record = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            "name": "AAPL Jan 17 2025 150 Call",
            "status": "active",
            "tradable": True,
            "expiration_date": "2025-01-17",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
            # No open_interest, open_interest_date, close_price, close_price_date
        }

        result = reader._parse_record(record)

        assert result[13] is None  # open_interest
        assert result[14] is None  # open_interest_date
        assert result[15] is None  # close_price
        assert result[16] is None  # close_price_date

    def test_parse_record_missing_required_field(self, reader):
        """Test that missing required field raises ValueError."""
        record = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            # Missing name, status, tradable, etc.
        }

        with pytest.raises(ValueError) as exc_info:
            reader._parse_record(record)

        assert "Failed to parse contract data" in str(exc_info.value)

    def test_parse_record_invalid_date(self, reader):
        """Test that invalid date raises ValueError."""
        record = {
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "symbol": "AAPL250117C00150000",
            "name": "AAPL Jan 17 2025 150 Call",
            "status": "active",
            "tradable": True,
            "expiration_date": "invalid-date",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": 100,
        }

        with pytest.raises(ValueError) as exc_info:
            reader._parse_record(record)

        assert "Failed to parse contract data" in str(exc_info.value)


@pytest.mark.unit
class TestParseBatch:
    """Tests for _parse_page_to_batch method."""

    @pytest.fixture
    def reader(self, contracts_options):
        """Create a reader instance for testing."""
        datasource = OptionsContractsDataSource(contracts_options)
        return datasource.reader(None)

    def test_parse_batch_with_valid_contracts(self, reader):
        """Test parsing a batch of valid contracts."""
        contracts = [
            {
                "id": "id1",
                "symbol": "AAPL250117C00150000",
                "name": "AAPL Call",
                "status": "active",
                "tradable": True,
                "expiration_date": "2025-01-17",
                "root_symbol": "AAPL",
                "underlying_symbol": "AAPL",
                "underlying_asset_id": "asset-id",
                "type": "call",
                "style": "american",
                "strike_price": "150.00",
                "size": 100,
            },
            {
                "id": "id2",
                "symbol": "AAPL250117P00150000",
                "name": "AAPL Put",
                "status": "active",
                "tradable": True,
                "expiration_date": "2025-01-17",
                "root_symbol": "AAPL",
                "underlying_symbol": "AAPL",
                "underlying_asset_id": "asset-id",
                "type": "put",
                "style": "american",
                "strike_price": "150.00",
                "size": 100,
            },
        ]

        batch = reader._parse_page_to_batch(contracts)

        assert batch is not None
        assert batch.num_rows == 2
        assert batch.num_columns == 17

    def test_parse_batch_with_empty_list(self, reader):
        """Test parsing an empty list returns None."""
        batch = reader._parse_page_to_batch([])

        assert batch is None

    def test_parse_batch_skips_malformed_records(self, reader):
        """Test that malformed records are skipped."""
        contracts = [
            {
                "id": "id1",
                "symbol": "AAPL250117C00150000",
                "name": "AAPL Call",
                "status": "active",
                "tradable": True,
                "expiration_date": "2025-01-17",
                "root_symbol": "AAPL",
                "underlying_symbol": "AAPL",
                "underlying_asset_id": "asset-id",
                "type": "call",
                "style": "american",
                "strike_price": "150.00",
                "size": 100,
            },
            {
                "id": "id2",
                # Missing required fields - should be skipped
            },
        ]

        batch = reader._parse_page_to_batch(contracts)

        assert batch is not None
        assert batch.num_rows == 1  # Only valid record
