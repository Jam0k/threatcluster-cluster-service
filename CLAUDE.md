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

## Article Scraping

The web scraping system is implemented in `src/scraper/`:

- **article_scraper.py**: Scrapes full article content from URLs
- **scraper_scheduler.py**: Daemon for periodic article scraping

### Running Article Scraper

```bash
# Test article scraper
python -m tests.test_article_scraper

# Scrape single article (test mode)
python -m src.scraper.article_scraper --test

# Scrape specific number of articles
python -m src.scraper.article_scraper --limit 50

# Run scraper once
python -m src.scraper.scraper_scheduler --once

# Run as daemon (checks every 15 minutes)
python -m src.scraper.scraper_scheduler
```

### Key Features

- Extracts full article content from web pages
- **Integrated content cleaning** - removes HTML, navigation, ads, footers
- Preserves security-relevant content (CVEs, IPs, domains, etc.)
- Falls back to RSS description if scraping fails
- Extracts and stores image URLs
- Domain-based rate limiting
- Handles paywalls and anti-bot protection
- Stores cleaned content in rss_feeds_clean table
- Tracks cleaning metrics (reduction percentage, chars removed)
- Sets rss_feeds_clean_processed = TRUE for successfully cleaned articles (ready for entity extraction)

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

## Entity Management

The entity management system is implemented in `src/database/`:

- **entities/**: Directory containing JSON files with manual entity definitions
- **sql/import_entities.py**: Script to import manual entities into the database

### Entity Categories

The system supports 13 entity categories:
- apt_group
- attack_type
- company
- country
- government_agency
- industry_sector
- malware_family
- mitre
- platform
- ransomware_group
- security_standard
- security_vendor
- vulnerability_type

### Importing Entities

```bash
# Import all entities from JSON files
python -m src.database.sql.import_entities
```

The import script:
- Reads all .json files from src/database/entities/
- Extracts category from filename (e.g., "apt_group" from "apt_group.json")
- Sets entities_source = "manual" for all imports
- Handles duplicates with ON CONFLICT UPDATE
- Provides detailed logging and summary statistics

## Entity Extraction

The entity extraction system is implemented in `src/entity_extraction/`:

- **entity_extractor.py**: Main extraction module with regex and predefined entity matching
- **entity_validator.py**: Validation and filtering for extracted entities  
- **entity_scheduler.py**: Daemon for periodic entity extraction

### Key Features

- Extracts technical indicators using regex patterns (CVEs, IPs, domains, hashes, etc.)
- Matches predefined entities from database (APT groups, malware families, etc.)
- Validates entities to filter out false positives
- Discovers and stores new dynamic entities
- Assigns confidence scores based on extraction method and context
- Processes articles in batches for efficiency

### Running Entity Extraction

```bash
# Import predefined entities first
python -m src.database.sql.import_entities

# Test entity extraction
python -m tests.test_entity_extractor

# Extract entities from single article (test mode)
python -m src.entity_extraction.entity_extractor --test

# Process specific number of articles
python -m src.entity_extraction.entity_extractor --limit 50

# Run extraction once
python -m src.entity_extraction.entity_scheduler --once

# Run as daemon (checks every 20 minutes)
python -m src.entity_extraction.entity_scheduler
```

### Entity Categories

The system handles 13+ entity categories:
- Technical: cve, ip_address, domain, file_hash, registry_key, file_path, email
- Threat Actors: apt_group, ransomware_group
- Malware: malware_family
- Organizations: company, security_vendor, government_agency
- Vulnerabilities: vulnerability_type, attack_type
- Infrastructure: platform, industry_sector
- Standards: security_standard, mitre

### Entity Storage Format

Entities are stored in `rss_feeds_clean_extracted_entities` as JSONB:
```json
{
  "entities": [
    {
      "entity_name": "CVE-2023-1234",
      "entity_category": "cve",
      "entities_id": 123,
      "confidence": 0.95,
      "position": "title",
      "extraction_method": "regex"
    }
  ],
  "extraction_timestamp": "2023-12-01T10:00:00Z",
  "entity_count": 15,
  "categories": ["cve", "apt_group", "malware_family"]
}
```

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

## Troubleshooting

### Articles not being processed by entity extraction
If entity extraction shows "no_articles_for_entity_extraction" but you have cleaned articles:
1. Check that `rss_feeds_clean_processed = TRUE` for cleaned articles
2. Run `python -m fix_processed_flags` to update existing articles
3. New articles will automatically have the correct flag set

### Entity name too long errors
If you encounter "value too long for type character varying" errors:
1. Run the migration script to update column lengths:
   ```bash
   psql $DATABASE_URL -f src/database/sql/update_varchar_lengths.sql
   ```
2. The entity extractor will automatically truncate entities longer than 500 characters
3. File paths and registry keys keep the end (most specific part) when truncated

## Semantic Clustering

The semantic clustering system groups related security articles using AI-powered similarity analysis. See [CLUSTERING.md](CLUSTERING.md) for detailed documentation.

### Quick Start

```bash
# Run clustering once on recent articles
python -m src.clustering.cluster_scheduler --once

# Run with extended time window (2 weeks)
python -m src.clustering.cluster_scheduler --once --full

# Run as daemon (checks every 30 minutes)
python -m src.clustering.cluster_scheduler

# View clustering results
python -m tests.test_cluster_report
```

### Key Features

- Uses sentence transformers (all-mpnet-base-v2) for semantic embeddings
- Implements DBSCAN and Agglomerative clustering algorithms
- Detects and prevents duplicate clusters
- Generates meaningful cluster names from extracted entities
- Validates clusters based on coherence, size, and time windows
- Supports batch processing for large volumes