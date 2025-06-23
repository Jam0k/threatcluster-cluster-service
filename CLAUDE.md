# Claude Development Guidelines

This document contains important information for Claude AI sessions working on the ThreatCluster project.

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