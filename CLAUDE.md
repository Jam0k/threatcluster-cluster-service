# Claude Development Guidelines

This document contains important information for Claude AI sessions working on the ThreatCluster project.

## IMPORTANT: No Scope Creep

**DO NOT** add features or functionality that were not explicitly requested:
- Do not create database tables unless specifically asked
- Do not add monitoring, logging tables, or statistics tracking beyond what's requested
- Do not implement "nice to have" features without asking first
- Stick to the exact requirements provided
- If you think something would be helpful, ASK before implementing it

Examples of scope creep to avoid:
- Creating tables like `fetch_stats` for monitoring when only fetching was requested
- Adding complex analytics when simple functionality was asked for
- Implementing additional endpoints, features, or data structures not in the requirements

## Dependency Management

**ALWAYS** check and update requirements.txt when:
- Adding new import statements to any Python file
- Using a new library or package
- Updating package versions

Before adding a new import:
1. Check if the package is already in requirements.txt
2. If not, add it with a specific version number
3. Verify the package name and version are correct

## RSS Feed Processing

The RSS feed fetching system is implemented in `src/feeds/`:

- **rss_fetcher.py**: Main RSS feed fetching module with security filtering
- **scheduler.py**: Daemon for periodic feed fetching

### Running RSS Feed Processing

```bash
# Import feeds from config
python -m src.database.sql.import_feeds

# Test RSS fetcher
python -m tests.test_rss_fetcher

# Run one-time fetch
python -m src.feeds.scheduler --once

# Run as daemon (periodic fetching)
python -m src.feeds.scheduler
```

### Key Features

- Fetches from active feeds in database
- Applies security keyword filtering to general news
- Stores raw XML data as JSONB
- Duplicate detection based on article URL
- Comprehensive error handling and logging
- Rate limiting and retry logic

## Project Structure

```
cluster-service/
├── src/
│   ├── config/
│   │   └── settings.py     # Main configuration loader
│   └── ...
├── tests/
│   └── test_db.py         # Database connection test
├── config/
│   ├── config.yaml        # Application configuration
│   └── keywords.yaml      # Keywords configuration
├── .env                   # Environment variables (DB credentials, etc.)
└── requirements.txt       # Python dependencies
```

## Python Import Best Practices

### Running Python Scripts

Always run Python scripts from the project root directory (`cluster-service/`) to ensure proper module resolution:

```bash
# Correct way to run tests
cd /home/james/Desktop/Threatcluster-2/cluster-service
python -m tests.test_db

# Or using pytest
pytest tests/test_db.py
```

### Why This Matters

- Python resolves imports relative to the current working directory
- Running from other directories will cause `ModuleNotFoundError`
- This approach maintains clean code without sys.path hacks

### Import Guidelines

1. **Use absolute imports** from the `src` package:
   ```python
   from src.config.settings import settings
   from src.models.article import Article
   ```

2. **Never use sys.path manipulation** in production code
   
3. **For scripts**, always include usage instructions in the docstring

## Database Configuration

- PostgreSQL connection details are in `.env`
- The `settings.py` module handles all configuration loading
- Database URL is automatically constructed with SSL enabled

## Testing

- Use pytest for all tests
- Run tests from the project root
- Database connection test available at `tests/test_db.py`

## Development Commands

```bash
# Test database connection
python -m tests.test_db

# Run all tests
pytest

# Run with coverage
pytest --cov=src
```

## Important Notes

- Always validate that required environment variables are set
- The project uses psycopg2-binary for PostgreSQL connections
- SSL mode is required for DigitalOcean managed databases