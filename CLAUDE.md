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
- **DataSource Classes** (`bars.py`, `trades.py`): Define schema and validate options
- **DataSourceReader Classes** (`bars.py`, `trades.py`): Implement data fetching with PyArrow batch support
- **Base Classes** (`common.py`): Abstract base classes (`BaseAlpacaDataSource`, `BaseAlpacaReader`) providing common functionality
- **Partition Classes** (`common.py`): Enable parallel processing by distributing work
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
If implementing additional Alpaca data types (quotes, etc.):
1. Extend `BaseAlpacaDataSource` and `BaseAlpacaReader` from `common.py`
2. Follow the pattern established in `bars.py` or `trades.py`
3. Define schema matching Alpaca API response
4. Implement required abstract methods (`api_params`, `data_key`, `path_elements`, `_parse_record`)
5. Use type aliases for complex return types (e.g., `BarTuple`, `TradeTuple`)
6. PyArrow batching and partitioning are handled by base classes
7. Register the data source in `__init__.py`

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

## Questions or Clarifications

When implementation details are unclear:
- Check existing patterns in `bars.py`, `trades.py`, and `common.py`
- Consult linked documentation (Alpaca API, PySpark DataSource API, PyArrow)
- Ask the user for clarification rather than making assumptions
- Consider whether the change fits within the current task scope