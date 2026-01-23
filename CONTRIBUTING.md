# Contributing Guide

Thank you for your interest in contributing to alpaca-pyspark! This guide provides detailed information about the development environment, testing procedures, and contribution workflow.

## Development Environment Setup

### Prerequisites

- **Python 3.11 or higher**
- **Poetry** for dependency management ([installation guide](https://python-poetry.org/docs/#installation))
- **Apache Spark / PySpark 4.0+** environment
- **Git** for version control

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/tnixon/alpaca-pyspark.git
   cd alpaca-pyspark
   ```

2. **Install dependencies with Poetry:**
   ```bash
   poetry install
   ```
   This installs both runtime and development dependencies defined in `pyproject.toml`.

3. **Activate the virtual environment:**
   ```bash
   poetry shell
   ```

4. **Verify installation:**
   ```bash
   # Check Python version
   python --version  # Should be 3.11+
   
   # Check that key dependencies are available
   python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
   python -c "import pandas; print(f'Pandas version: {pandas.__version__}')"
   ```

### Project Structure

```
alpaca-pyspark/
├── alpaca_pyspark/          # Main package directory
│   ├── common.py            # Shared base classes, utilities, and partitioning logic
│   ├── bars.py              # Abstract base classes for bars data sources
│   ├── stocks/              # Stock market data sources
│   │   ├── bars.py          # Stock historical bars data source implementation
│   │   ├── trades.py        # Stock historical trades data source implementation
│   │   └── __init__.py      # Stocks sub-module exports
│   ├── options/             # Options data sources
│   │   ├── bars.py          # Options historical bars data source implementation
│   │   └── __init__.py      # Options sub-module exports
│   ├── corp_actions/        # Corporate actions data sources
│   │   ├── corporate_actions.py  # Corporate actions implementation
│   │   └── __init__.py      # Corporate actions sub-module exports
│   ├── crypto/              # Crypto data sources (placeholder)
│   │   └── __init__.py
│   └── __init__.py          # Package root
├── tests/                   # Test suite
│   ├── conftest.py          # Global test fixtures
│   ├── fixtures/            # Test data and mock responses
│   ├── unit/                # Pure unit tests
│   ├── integration/         # Integration tests with mocked APIs
│   └── spark/               # Tests requiring Spark session
├── .github/workflows/       # CI/CD workflows
├── pyproject.toml           # Poetry configuration and dependencies
├── README.md                # Project overview and quick start
├── USAGE.md                 # Detailed usage documentation
├── CONTRIBUTING.md          # This file
└── CLAUDE.md                # Development guidelines for AI assistants
```

### Key Components

- **Base Classes** (`common.py`): Universal abstract base classes providing common functionality for all asset types
- **Bars Abstract Classes** (`bars.py`): Abstract base classes for OHLCV data across all asset types
- **DataSource Implementations**: Asset-specific implementations extending the abstract base classes
- **Partition Classes** (`common.py`): Enable parallel processing by distributing work across symbols and time ranges
- **Utility Functions** (`common.py`): Common functionality for URL building and API requests

## Development Workflow

### 1. Code Quality Tools

The project uses three primary tools for maintaining code quality:

#### Ruff (Code Formatting)
Ruff replaces Black/YAPF for code formatting with better performance.

```bash
# Format all Python files
poetry run ruff format alpaca_pyspark/ tests/

# Check formatting without making changes
poetry run ruff format --check alpaca_pyspark/ tests/

# Configuration is in pyproject.toml under [tool.ruff]
```

#### Flake8 (Linting)
Checks for code style issues and potential problems.

```bash
# Run linting on the package
poetry run flake8 alpaca_pyspark/

# Run linting on tests
poetry run flake8 tests/

# Configuration is in .flake8 file
```

#### MyPy (Type Checking)
Performs static type analysis to catch type-related errors.

```bash
# Check types for the package
poetry run mypy alpaca_pyspark/

# Configuration is in pyproject.toml under [tool.mypy]
```

#### Run All Quality Checks

```bash
# Run all quality checks in sequence
poetry run ruff format --check alpaca_pyspark/ tests/ && \
poetry run flake8 alpaca_pyspark/ tests/ && \
poetry run mypy alpaca_pyspark/
```

### 2. Testing

The project uses pytest with comprehensive test coverage across multiple categories.

#### Test Categories

**Unit Tests** (`tests/unit/`):
- Pure unit tests with mocked external dependencies
- Fast execution (seconds)
- No network calls or real API access
- Test individual functions and classes in isolation
- Marked with `@pytest.mark.unit`

**Integration Tests** (`tests/integration/`):
- Test complete workflows end-to-end
- Use mock HTTP responses via `responses` library
- Test interactions between components
- Marked with `@pytest.mark.integration`

**Spark Tests** (`tests/spark/`):
- Tests requiring actual Spark session
- May be slower to execute
- Test full DataSource integration with Spark SQL
- Marked with `@pytest.mark.spark`

#### Running Tests

```bash
# Run all tests with coverage
poetry run pytest

# Run only unit tests (fast)
poetry run pytest -m unit

# Run tests without coverage (faster)
poetry run pytest --no-cov

# Run specific test file
poetry run pytest tests/unit/test_common.py

# Run specific test class
poetry run pytest tests/unit/test_common.py::TestBuildUrl

# Run specific test function
poetry run pytest tests/unit/test_common.py::TestBuildUrl::test_simple_url

# Run tests matching pattern
poetry run pytest -k "test_build_url"

# Run failed tests from last execution
poetry run pytest --lf

# Run tests with verbose output
poetry run pytest -v

# Run tests in parallel (if pytest-xdist is installed)
poetry run pytest -n auto
```

#### Test Fixtures

Common test fixtures are available in `tests/conftest.py`:

- `sample_symbols`: List of test stock symbols
- `sample_date_range`: Test date range with timezone
- `api_credentials`: Mock API credentials
- `base_options`: Complete options dict for DataSource testing
- `mock_alpaca_api`: HTTP mocking fixture using `responses` library

Example usage:
```python
def test_datasource_creation(sample_symbols, base_options):
    """Test fixtures are auto-injected by pytest."""
    datasource = MyDataSource(base_options)
    assert datasource.symbols == sample_symbols

def test_api_call(mock_alpaca_api):
    """Mock API responses for testing."""
    from tests.fixtures.mock_responses import MOCK_BARS_RESPONSE
    
    mock_alpaca_api.add(
        responses.GET,
        "https://data.alpaca.markets/v2/stocks/bars",
        json=MOCK_BARS_RESPONSE,
        status=200
    )
    # Test code here
```

#### Coverage Reports

After running tests with coverage:

```bash
# View HTML coverage report in browser
open htmlcov/index.html

# View coverage summary in terminal
poetry run pytest --cov=alpaca_pyspark --cov-report=term-missing
```

**Coverage Targets:**
- Core utilities (`common.py`): 80%+
- Abstract base classes (`bars.py`): 85%+
- Concrete implementations (`stocks/`, `options/`): 70%+
- Overall project: 70-80%

### 3. Building the Package

```bash
# Build distribution packages
poetry build

# Output appears in dist/ directory
ls -la dist/
```

### 4. Pre-commit Checklist

Before committing code, run this complete check:

```bash
# 1. Format code
poetry run ruff format alpaca_pyspark/ tests/

# 2. Run linting
poetry run flake8 alpaca_pyspark/ tests/

# 3. Check types
poetry run mypy alpaca_pyspark/

# 4. Run tests with coverage
poetry run pytest

# 5. Build package (optional)
poetry build
```

## Contribution Guidelines

### Code Style Standards

Follow the **Clean Code principles** outlined in the project's `CLAUDE.md`:

1. **Self-documenting code**: Use clear variable and function names
2. **Single responsibility**: Keep functions focused on one task
3. **DRY principle**: Don't repeat non-trivial code
4. **Minimize complexity**: Reduce nesting and complexity
5. **Readability first**: Write code for humans to understand

### Making Changes

#### For Bug Fixes:
1. Create a test that reproduces the bug
2. Verify the test fails with current code
3. Fix the bug with minimal changes
4. Verify the test passes
5. Update documentation if needed

#### For New Features:
1. Discuss the feature in an issue first
2. Follow existing patterns in the codebase
3. Write comprehensive tests
4. Update documentation
5. Consider backward compatibility

#### For New Data Source Types:

**For Stock Data Types** (quotes, snapshots, etc.):
1. Create new file in `stocks/` directory (e.g., `stocks/quotes.py`)
2. Extend `BaseAlpacaDataSource` and `BaseAlpacaReader` from `common.py`
3. Follow the pattern established in `stocks/bars.py` or `stocks/trades.py`
4. Define schema matching Alpaca API response
5. Implement required abstract methods
6. Add to `stocks/__init__.py` for auto-import

**For New Asset Types** (crypto, forex, etc.):
1. Create new sub-module directory (e.g., `crypto/`)
2. Create implementation files (e.g., `crypto/bars.py`)
3. Extend base classes from `common.py`
4. Follow existing patterns
5. Update main package `__init__.py`

### Testing Requirements

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

### Documentation Requirements

When making changes:
- Update docstrings for modified functions/classes
- Update `README.md` if the change affects basic usage
- Update `USAGE.md` for new features or configuration options
- Update `CONTRIBUTING.md` for process changes
- Add examples for new data sources or features

### Commit Message Guidelines

Use clear, descriptive commit messages:

```bash
# Good examples
git commit -m "feat: add historical quotes data source for stocks"
git commit -m "fix: handle empty API responses gracefully"
git commit -m "docs: update usage examples for corporate actions"
git commit -m "test: add unit tests for partition size calculation"

# Follow conventional commit format when possible
# Type: description
# 
# Types: feat, fix, docs, style, refactor, test, chore
```

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration:

### Lint Workflow (`.github/workflows/lint.yml`)

Automatically runs on all pull requests:
1. **Ruff formatting check**: Ensures code is properly formatted
2. **Flake8 linting**: Checks for style issues
3. **MyPy type checking**: Validates type annotations

### Running CI Locally

Simulate the CI environment locally:

```bash
# Run the same checks as CI
poetry run ruff format --check alpaca_pyspark/ tests/
poetry run flake8 alpaca_pyspark/ tests/
poetry run mypy alpaca_pyspark/
poetry run pytest
```

## Getting Help

### Documentation Resources

- **Project README**: General overview and quick start
- **Usage Guide**: Detailed configuration and examples
- **CLAUDE.md**: Development guidelines and architecture
- **API Documentation**: Links to external API docs

### Code Patterns

When unsure about implementation patterns:
- Check existing implementations in `stocks/bars.py`, `stocks/trades.py`
- Review base classes in `common.py` and `bars.py`
- Look at test examples in `tests/unit/` and `tests/integration/`

### Asking Questions

1. **Check existing issues** on GitHub
2. **Review documentation** thoroughly
3. **Create a new issue** with:
   - Clear problem description
   - Relevant code snippets
   - Expected vs. actual behavior
   - Environment details (Python version, PySpark version, etc.)

## Security Considerations

### API Credentials
- **NEVER commit API credentials** to version control
- Use environment variables, Spark secrets, or cloud secret managers
- Add credential files to `.gitignore`
- Document secure credential handling in examples

### Code Review
- All changes require code review
- Review for security vulnerabilities
- Verify no credentials are exposed
- Check for potential data leaks

## Performance Guidelines

### When making changes that affect data processing:

1. **Preserve PyArrow batching**: Don't introduce row-by-row processing
2. **Maintain partitioning**: Keep symbol-based partitioning for parallel execution
3. **Justify batch size changes**: Default is 10,000 rows per batch
4. **Consider API rate limiting**: Include retry logic and backoff strategies

### Testing Performance Changes

```bash
# Profile specific functions
poetry run python -m cProfile -s cumulative your_test_script.py

# Time critical operations
import timeit
timeit.timeit(lambda: your_function(), number=100)

# Use Spark UI for distributed performance analysis
# Available at http://localhost:4040 during Spark session
```

## Release Process

For maintainers:

1. **Update version** in `pyproject.toml`
2. **Update changelog** or release notes
3. **Run full test suite**: `poetry run pytest`
4. **Build package**: `poetry build`
5. **Tag release**: `git tag v0.1.0`
6. **Push tags**: `git push --tags`

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).