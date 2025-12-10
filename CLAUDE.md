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
- **DataSource Classes** (`bars.py`): Define schema and validate options
- **DataSourceReader Classes** (`bars.py`): Implement data fetching with PyArrow batch support
- **Partition Classes** (`common.py`): Enable parallel processing by distributing work
- **Utility Functions** (`common.py`): Shared functionality for URL building and API requests

### Important Documentation

#### External APIs and Technologies
- [Alpaca Market Data API](https://docs.alpaca.markets/docs/about-market-data-api) - API specification and endpoints
- [PySpark DataSource API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) - PySpark custom data source implementation
- [PyArrow Documentation](https://arrow.apache.org/docs/python/) - Arrow batch processing

#### Dependencies
Dependencies are managed via Poetry and defined in `pyproject.toml`:
- Runtime: Python 3.11+, PySpark 4.0+, pandas, requests
- Development: pytest, black, flake8, mypy

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
- Follow existing code formatting (black, flake8)
- Use type hints consistently (mypy)
- Maintain existing naming conventions
- Keep docstrings updated when modifying functions

## Working with PySpark DataSource API

When modifying data source implementations:
1. **Schema Definition**: Must be defined in the DataSource class
2. **Reader Implementation**: Must implement the reader method returning a DataSourceReader
3. **Partitioning**: Implement `partitions()` method in the reader for parallelization
4. **Batch Processing**: Use `readFromIterator()` and yield PyArrow RecordBatch objects

## Common Tasks

### Adding New Data Source Types
If implementing additional Alpaca data types (trades, quotes, etc.):
1. Follow the pattern established in `bars.py`
2. Define schema matching Alpaca API response
3. Implement partitioning for parallel execution
4. Use PyArrow batching for performance
5. Include retry logic and error handling
6. Register the data source in `__init__.py`

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

## Questions or Clarifications

When implementation details are unclear:
- Check existing patterns in `bars.py` and `common.py`
- Consult linked documentation (Alpaca API, PySpark DataSource API, PyArrow)
- Ask the user for clarification rather than making assumptions
- Consider whether the change fits within the current task scope