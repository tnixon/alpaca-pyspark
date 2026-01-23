"""Unit tests for corporate actions DataSource and Reader."""

import pytest
import responses
from datetime import datetime as dt

from alpaca_pyspark.corp_actions.corporate_actions import CorporateActionsDataSource, CorporateActionsReader
from alpaca_pyspark.common import SymbolTimeRangePartition
from tests.fixtures.mock_responses import MOCK_CORPORATE_ACTIONS_RESPONSE


class TestCorporateActionsDataSource:
    """Test suite for CorporateActionsDataSource."""

    def test_name(self):
        """Test DataSource name."""
        assert CorporateActionsDataSource.name() == "Alpaca_Corporate_Actions"

    def test_schema_type(self, base_options):
        """Test schema returns correct type."""
        datasource = CorporateActionsDataSource(base_options)
        schema = datasource.schema()
        assert isinstance(schema, str)
        assert "symbol" in schema
        assert "ex_date" in schema
        assert "type" in schema

    def test_pa_schema(self, base_options):
        """Test PyArrow schema structure."""
        datasource = CorporateActionsDataSource(base_options)
        schema = datasource.pa_schema
        field_names = [field.name for field in schema]
        expected_fields = ["symbol", "ex_date", "record_date", "payable_date", "type", "amount", "ratio", "new_symbol", "old_symbol"]
        assert field_names == expected_fields

    def test_api_params(self, base_options):
        """Test API parameters include corporate action-specific options."""
        datasource = CorporateActionsDataSource(base_options)
        param_names = [p.name for p in datasource.api_params]
        
        # Check base params are included
        assert "symbols" in param_names
        assert "start" in param_names
        assert "end" in param_names
        
        # Check corporate action-specific params
        assert "sort" in param_names
        assert "types" in param_names
        assert "date_type" in param_names

    def test_valid_sort_parameter(self, base_options):
        """Test valid sort parameter values."""
        for sort_val in ["asc", "desc"]:
            options = {**base_options, "sort": sort_val}
            datasource = CorporateActionsDataSource(options)
            assert sort_val in datasource.params.get("sort", "")

    def test_invalid_sort_parameter(self, base_options):
        """Test invalid sort parameter raises ValueError."""
        options = {**base_options, "sort": "invalid"}
        with pytest.raises(ValueError, match="Invalid 'sort' value"):
            CorporateActionsDataSource(options)

    def test_valid_types_parameter(self, base_options):
        """Test valid types parameter values."""
        valid_types = ["dividend", "split", "merger", "spinoff", "stock_dividend", "all"]
        for type_val in valid_types:
            options = {**base_options, "types": type_val}
            datasource = CorporateActionsDataSource(options)
            assert type_val in datasource.params.get("types", "")

    def test_valid_types_parameter_multiple(self, base_options):
        """Test multiple types parameter values."""
        options = {**base_options, "types": "dividend,split,merger"}
        datasource = CorporateActionsDataSource(options)
        assert "dividend,split,merger" in datasource.params.get("types", "")

    def test_invalid_types_parameter(self, base_options):
        """Test invalid types parameter raises ValueError."""
        options = {**base_options, "types": "invalid"}
        with pytest.raises(ValueError, match="Invalid 'types' values"):
            CorporateActionsDataSource(options)

    def test_empty_types_parameter_after_split(self, base_options):
        """Test types parameter with empty strings after splitting."""
        options = {**base_options, "types": "dividend,,split,"}
        datasource = CorporateActionsDataSource(options)
        # Should filter out empty strings and work correctly
        assert "dividend" in datasource.params.get("types", "")
        assert "split" in datasource.params.get("types", "")

    def test_valid_date_type_parameter(self, base_options):
        """Test valid date_type parameter values."""
        for date_type in ["ex_date", "record_date", "payable_date"]:
            options = {**base_options, "date_type": date_type}
            datasource = CorporateActionsDataSource(options)
            assert date_type in datasource.params.get("date_type", "")

    def test_invalid_date_type_parameter(self, base_options):
        """Test invalid date_type parameter raises ValueError."""
        options = {**base_options, "date_type": "invalid"}
        with pytest.raises(ValueError, match="Invalid 'date_type' value"):
            CorporateActionsDataSource(options)


class TestCorporateActionsReader:
    """Test suite for CorporateActionsReader."""

    def test_data_key(self, sample_corp_actions_reader):
        """Test data key is correct."""
        assert sample_corp_actions_reader.data_key == "corporate_actions"

    def test_path_elements(self, sample_corp_actions_reader):
        """Test path elements are correct."""
        assert sample_corp_actions_reader.path_elements == ["stocks", "corporate_actions"]

    def test_parse_record_dividend(self, sample_corp_actions_reader):
        """Test parsing a dividend corporate action record."""
        record = {
            "ex_date": "2021-02-05T00:00:00Z",
            "record_date": "2021-02-08T00:00:00Z",
            "payable_date": "2021-02-11T00:00:00Z",
            "type": "dividend",
            "amount": 0.205,
            "ratio": 1.0,
            "new_symbol": "",
            "old_symbol": "AAPL",
        }
        
        result = sample_corp_actions_reader._parse_record("AAPL", record)
        
        expected = (
            "AAPL",
            dt.fromisoformat("2021-02-05T00:00:00+00:00"),
            dt.fromisoformat("2021-02-08T00:00:00+00:00"),
            dt.fromisoformat("2021-02-11T00:00:00+00:00"),
            "dividend",
            0.205,
            1.0,
            "",
            "AAPL",
        )
        
        assert result == expected

    def test_parse_record_split(self, sample_corp_actions_reader):
        """Test parsing a split corporate action record."""
        record = {
            "ex_date": "2021-08-30T00:00:00Z",
            "record_date": "2021-08-30T00:00:00Z",
            "payable_date": "2021-08-30T00:00:00Z",
            "type": "split",
            "amount": 0.0,
            "ratio": 4.0,
            "new_symbol": "AAPL",
            "old_symbol": "AAPL",
        }
        
        result = sample_corp_actions_reader._parse_record("AAPL", record)
        
        expected = (
            "AAPL",
            dt.fromisoformat("2021-08-30T00:00:00+00:00"),
            dt.fromisoformat("2021-08-30T00:00:00+00:00"),
            dt.fromisoformat("2021-08-30T00:00:00+00:00"),
            "split",
            0.0,
            4.0,
            "AAPL",
            "AAPL",
        )
        
        assert result == expected

    def test_parse_record_with_none_dates(self, sample_corp_actions_reader):
        """Test parsing record with None date values."""
        record = {
            "ex_date": "2021-02-05T00:00:00Z",
            "record_date": None,
            "payable_date": None,
            "type": "dividend",
            "amount": 0.205,
            "ratio": 1.0,
            "new_symbol": "",
            "old_symbol": "AAPL",
        }
        
        result = sample_corp_actions_reader._parse_record("AAPL", record)
        
        expected = (
            "AAPL",
            dt.fromisoformat("2021-02-05T00:00:00+00:00"),
            None,
            None,
            "dividend",
            0.205,
            1.0,
            "",
            "AAPL",
        )
        
        assert result == expected

    def test_parse_record_malformed_date(self, sample_corp_actions_reader):
        """Test parsing record with malformed date format raises ValueError."""
        record = {
            "ex_date": "invalid-date-format",
            "record_date": "2021-02-08T00:00:00Z",
            "payable_date": "2021-02-11T00:00:00Z",
            "type": "dividend",
            "amount": 0.205,
            "ratio": 1.0,
            "new_symbol": "",
            "old_symbol": "AAPL",
        }
        
        with pytest.raises(ValueError, match="Failed to parse corporate action data"):
            sample_corp_actions_reader._parse_record("AAPL", record)

    def test_parse_record_missing_required_fields(self, sample_corp_actions_reader):
        """Test parsing record with missing required fields raises ValueError."""
        record = {
            "ex_date": "2021-02-05T00:00:00Z",
            # Missing type field
            "amount": 0.205,
        }
        
        with pytest.raises(ValueError, match="Failed to parse corporate action data"):
            sample_corp_actions_reader._parse_record("AAPL", record)

    def test_parse_record_invalid_json_structure(self, sample_corp_actions_reader):
        """Test parsing record with invalid structure raises ValueError."""
        record = "not_a_dict"
        
        with pytest.raises(ValueError, match="Failed to parse corporate action data"):
            sample_corp_actions_reader._parse_record("AAPL", record)

    @pytest.mark.integration
    def test_read_with_mock_api(self, mock_alpaca_api, sample_corp_actions_reader):
        """Test reading data with mocked API response."""
        mock_alpaca_api.add(
            responses.GET,
            "https://data.alpaca.markets/v2/stocks/corporate_actions",
            json=MOCK_CORPORATE_ACTIONS_RESPONSE,
            status=200
        )

        # Create a partition for testing
        partition = SymbolTimeRangePartition(
            "AAPL",
            dt(2021, 1, 1),
            dt(2021, 12, 31)
        )

        # Read data
        batches = list(sample_corp_actions_reader.read(partition))
        
        # Verify we got data
        assert len(batches) > 0
        batch = batches[0]
        
        # Verify batch structure
        assert batch.num_rows == 2  # Two corporate actions in mock data
        assert batch.num_columns == 9  # All schema fields
        
        # Verify column data
        symbols = batch.column("symbol").to_pylist()
        assert symbols == ["AAPL", "AAPL"]
        
        types = batch.column("type").to_pylist()
        assert "dividend" in types
        assert "split" in types

    @pytest.mark.integration 
    def test_read_with_http_error(self, mock_alpaca_api, sample_corp_actions_reader):
        """Test reading data with HTTP error response."""
        mock_alpaca_api.add(
            responses.GET,
            "https://data.alpaca.markets/v2/stocks/corporate_actions",
            json={"message": "Unauthorized"},
            status=401
        )

        # Create a partition for testing
        partition = SymbolTimeRangePartition(
            "AAPL",
            dt(2021, 1, 1),
            dt(2021, 12, 31)
        )

        # Reading should handle HTTP errors gracefully
        # The specific behavior depends on the retry logic in BaseAlpacaReader
        batches = list(sample_corp_actions_reader.read(partition))
        
        # Should return empty list or raise appropriate exception
        # This test verifies the error handling doesn't crash the reader
        assert isinstance(batches, list)