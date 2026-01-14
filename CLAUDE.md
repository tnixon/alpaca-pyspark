# Claude Code Instructions for alpaca-pyspark

This document provides project-specific guidance for working on the alpaca-pyspark codebase. For general project information, see [README.md](README.md) and [pyproject.toml](pyproject.toml).

## Core Principles

### 1. Clean Code Standards
**All code changes must follow Clean Code principles** as outlined in [How to Write Clean Code](https://zencoder.ai/blog/how-to-write-clean-code). Key principles:
- Write self-documenting code with clear variable and function names
- Keep functions small and focused on a single responsibility
- DRY: Don't Repeat Yourself (don't duplicate any non-trivial code)
- Minimize complexity and nesting
- Avoid premature optimization
- Write code for readability first

### 2. Focused Changes Only
**Focus exclusively on the requested changes or functionality.** Do NOT:
- Propose code changes unrelated to the current query
- Refactor code that isn't part of the current task
- Add "improvements" or "enhancements" not explicitly requested
- Reformat or cleanup code outside the scope of work

### 3. PR Reviews Are For Feedback
Suggestions for cleanup, refactoring, and broader improvements belong in PR reviews. 
During implementation:
- Implement only what's requested
- Save suggestions for the PR review process
- Avoid scope creep

## Project-Specific Context

### Architecture Overview
This project implements PySpark DataSource connectors for the Alpaca Markets API. Key architectural patterns:

1. **Distributed Processing**: Data fetching is partitioned by stock symbol to enable parallel execution across Spark executors
2. **PyArrow Batching**: Uses Apache Arrow RecordBatch objects for high-performance data transfer (10x faster than row-by-row)
3. **Resilient API Calls**: Built-in retry logic with exponential backoff

### Key Components
- **DataSource Classes** (`stocks/bars.py`, `stocks/trades.py`): Define schema and validate options for stock data types
- **DataSourceReader Classes** (`stocks/bars.py`, `stocks/trades.py`): Implement data fetching with PyArrow batch support
- **Base Classes** (`common.py`): Abstract base classes (`BaseAlpacaDataSource`, `BaseAlpacaReader`) providing common functionality for all asset types
- **Partition Classes** (`common.py`): Enable parallel processing by distributing work across symbols and time ranges
- **Utility Functions** (`common.py`): Shared functionality for URL building and API requests

### Important Documentation

#### External APIs and Technologies
- [Alpaca Market Data API](https://docs.alpaca.markets/docs/about-market-data-api) - API specification and endpoints
- [PySpark DataSource API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) - PySpark custom data source implementation
- [PyArrow Documentation](https://arrow.apache.org/docs/python/) - Arrow batch processing

#### Dependencies
Dependencies are managed via Poetry and defined in `pyproject.toml`:
- Runtime: Python 3.11+, PySpark 4.0+, pandas, requests, pyarrow
- Development: pytest, ruff, flake8, mypy

## Development Guidelines

### Testing
- Use `poetry shell` to activate the virtual environment before testing
- Test with actual PySpark sessions when possible
- Verify distributed behavior across multiple symbols
- Test error handling and retry logic

### Performance Considerations
When making changes that affect data processing:
- **Preserve PyArrow batching** - Don't introduce row-by-row processing
- **Maintain partitioning** - Keep symbol-based partitioning for parallel execution
- **Batch size** - Default is 10,000 rows per batch; changes should be justified
- **API rate limiting** - Consider retry logic and backoff strategies

### Schema Changes
The schema is strictly defined (see README.md for current schema). Any schema changes:
- Must maintain backward compatibility when possible
- Should align with Alpaca API response structure
- Need to preserve PySpark type mappings

### Error Handling Patterns
Follow existing patterns in the codebase:
- Use retry logic with exponential backoff for network failures
- Log warnings for malformed data without failing the entire job
- Handle connection timeouts gracefully
- Provide comprehensive error messages

### Security
- **NEVER hardcode API credentials** in code or tests
- **NEVER commit credentials** to version control
- Use environment variables or Spark secrets management
- Document secure credential handling in examples

### Code Style
- Follow existing code formatting (ruff format, flake8)
- Use type hints consistently (mypy)
- Maintain existing naming conventions
- Keep docstrings updated when modifying functions
- Use type aliases for complex type hints to improve readability

## Working with PySpark DataSource API

When modifying data source implementations:
1. **Schema Definition**: Must be defined in the DataSource class
2. **Reader Implementation**: Must implement the reader method returning a DataSourceReader
3. **Partitioning**: Implement `partitions()` method in the reader for parallelization
4. **Batch Processing**: Use `readFromIterator()` and yield PyArrow RecordBatch objects

## Common Tasks

### Adding New Data Source Types

#### For Stock Data Types
If implementing additional stock data types (quotes, snapshots, etc.):
1. Create new file in `stocks/` directory (e.g., `stocks/quotes.py`)
2. Extend `BaseAlpacaDataSource` and `BaseAlpacaReader` from `common.py`
3. Follow the pattern established in `stocks/bars.py` or `stocks/trades.py`
4. Define schema matching Alpaca API response
5. Implement required abstract methods (`api_params`, `data_key`, `path_elements`, `_parse_record`)
6. Use type aliases for complex return types (e.g., `BarTuple`, `TradeTuple`)
7. PyArrow batching and partitioning are handled by base classes
8. Add to `stocks/__init__.py` for auto-import

#### For New Asset Types (Options, Crypto)
If implementing data sources for options or crypto:
1. Create implementation files in the appropriate sub-module directory (e.g., `options/chains.py`, `crypto/bars.py`)
2. Extend `BaseAlpacaDataSource` and `BaseAlpacaReader` from `common.py`
3. Follow the pattern established in `stocks/bars.py` or `stocks/trades.py`
4. Define asset-type-specific schema matching Alpaca API response
5. Implement required abstract methods
6. Update the sub-module's `__init__.py` for auto-imports

### Modifying API Requests
When changing how API requests are made:
- Maintain the retry logic pattern in `common.py`
- Preserve connection timeout settings
- Keep URL construction centralized
- Update error handling as needed

### Performance Optimization
Before optimizing:
1. Profile to identify actual bottlenecks
2. Ensure proposed changes maintain distributed processing
3. Verify PyArrow batching is preserved
4. Test with realistic data volumes

## Environment Setup

Development environment setup is covered in README.md. Key points:
- Use `poetry install` to install dependencies
- Use `poetry shell` to activate the virtual environment
- Use `poetry build` to create distribution packages

## Code Quality Tools

The project uses three tools for code quality:

1. **Ruff** - Code formatting (replaces Black/YAPF)
   - Run: `poetry run ruff format alpaca_pyspark/`
   - Check: `poetry run ruff format --check alpaca_pyspark/`
   - Configuration in `pyproject.toml` under `[tool.ruff]`

2. **Flake8** - Linting for code style issues
   - Run: `poetry run flake8 alpaca_pyspark/`
   - Configuration in `.flake8` file

3. **MyPy** - Static type checking
   - Run: `poetry run mypy alpaca_pyspark/`
   - Configuration in `pyproject.toml` under `[tool.mypy]`

All three run independently in CI/CD (see `.github/workflows/lint.yml`)

## Testing Guidelines

### When to Write Tests

**Always write tests for:**
- New features and functionality
- Bug fixes (test should fail before fix, pass after)
- Changes to core utilities or base classes
- New DataSource implementations
- API interaction code

**Tests are optional for:**
- Documentation-only changes
- Trivial formatting changes
- Experimental code clearly marked as such

### Test Organization

Tests mirror the source code structure:
- `tests/unit/test_common.py` → tests for `alpaca_pyspark/common.py`
- `tests/unit/test_bars.py` → tests for `alpaca_pyspark/bars.py`
- `tests/unit/test_stocks_bars.py` → tests for `alpaca_pyspark/stocks/bars.py`

### Using Test Fixtures

Common fixtures are available in `tests/conftest.py`:

```python
def test_something(sample_symbols, base_options):
    """sample_symbols and base_options are auto-injected by pytest."""
    datasource = MyDataSource(base_options)
    assert datasource.symbols == sample_symbols
```

For mocking HTTP requests, use the `mock_alpaca_api` fixture:

```python
def test_api_call(mock_alpaca_api):
    """Mock Alpaca API responses."""
    from tests.fixtures.mock_responses import MOCK_BARS_RESPONSE

    mock_alpaca_api.add(
        responses.GET,
        "https://data.alpaca.markets/v2/stocks/bars",
        json=MOCK_BARS_RESPONSE,
        status=200
    )

    # Your test code here
```

### Mocking Strategy

**HTTP Requests:**
- Use `responses` library via `mock_alpaca_api` fixture
- Define mock responses in `tests/fixtures/mock_responses.py` for reuse
- Never make real API calls in unit tests

**Datetime/Time:**
- Use `freezegun` for deterministic date/time testing
- Example: `@freeze_time("2021-01-15 12:00:00")`

**Spark:**
- Unit tests should NOT create real Spark sessions
- Test DataSource/Reader logic independently
- Save full Spark integration for `tests/spark/` directory

### Test Naming and Structure

**File naming:**
- Test files: `test_*.py`
- Match source file name: `test_common.py` tests `common.py`

**Class naming:**
- Group related tests: `class TestBuildUrl:`
- Use descriptive names: `class TestSymbolTimeRangePartition:`

**Function naming:**
- Use `test_` prefix: `def test_simple_url():`
- Descriptive: `def test_url_with_none_params():`
- Include expected behavior: `def test_missing_required_option_raises_error():`

**Test structure (Arrange-Act-Assert):**
```python
def test_something():
    """Test that something works correctly."""
    # Arrange: Set up test data
    url = "https://api.example.com"
    params = {"key": "value"}

    # Act: Execute the code being tested
    result = build_url(url, ["path"], params)

    # Assert: Verify expected behavior
    assert "key=value" in result
```

### Test Markers

Mark tests appropriately for selective execution:

```python
@pytest.mark.unit
def test_pure_unit_test():
    """Fast test, no external dependencies."""
    pass

@pytest.mark.integration
def test_with_mocked_api():
    """Integration test with mocked HTTP."""
    pass

@pytest.mark.spark
def test_with_spark_session():
    """Test requiring Spark."""
    pass
```

### Parameterized Tests

Use `@pytest.mark.parametrize` for testing multiple inputs:

```python
@pytest.mark.parametrize("timeframe,expected", [
    ("1Min", timedelta(minutes=1)),
    ("1Hour", timedelta(hours=1)),
    ("1Day", timedelta(days=1)),
])
def test_timeframe_parsing(timeframe, expected):
    result = parse_timeframe(timeframe)
    assert result == expected
```

### Coverage Expectations

**Target coverage by module:**
- Core utilities (`common.py`): 80%+
- Abstract base classes (`bars.py`): 85%+
- Concrete implementations (`stocks/bars.py`, etc.): 70%+
- Overall project: 70-80%

**Acceptable coverage gaps:**
- Error paths requiring real Spark runtime errors
- Complex network retry edge cases
- Performance-critical paths better tested via integration

### What NOT to Test

- **External library functionality**: Don't test that requests.get() works
- **Third-party integrations**: Don't test PySpark internals
- **Obvious getters/setters**: Don't test trivial property access
- **Python language features**: Don't test that dataclasses work

### Running Tests During Development

Before committing code:

1. Run tests for the module you changed:
   ```bash
   poetry run pytest tests/unit/test_common.py
   ```

2. Run all unit tests (fast):
   ```bash
   poetry run pytest -m unit
   ```

3. Check coverage:
   ```bash
   poetry run pytest --cov=alpaca_pyspark
   ```

4. Run code quality checks:
   ```bash
   poetry run ruff format alpaca_pyspark/ tests/
   poetry run flake8 alpaca_pyspark/ tests/
   poetry run mypy alpaca_pyspark/
   ```

## Questions or Clarifications

When implementation details are unclear:
- Check existing patterns in `stocks/bars.py`, `stocks/trades.py`, and `common.py`
- Consult linked documentation (Alpaca API, PySpark DataSource API, PyArrow)
- Ask the user for clarification rather than making assumptions
- Consider whether the change fits within the current task scope