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
- **Enhanced security filtering** applied to ALL feeds (not just general news)
- Advanced exclusion patterns to filter out non-security content
- Stores raw XML data as JSONB
- Duplicate detection based on article URL
- Comprehensive error handling and logging
- Rate limiting and retry logic

### Enhanced Security Filtering

The system now uses `config/security_keywords.yaml` for advanced filtering:
- **Required Keywords**: At least one security keyword must be present
- **Exclusion Patterns**: Filters out non-security discussions (hardware questions, network ops, product announcements)
- **Title Pattern Matching**: Identifies and excludes common non-security patterns
- **Domain Blocking**: Excludes social media and non-security domains

This prevents non-security content (like NANOG hardware discussions) from entering the pipeline.

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

## Main CLI Application

ThreatCluster provides an interactive menu-driven CLI interface:

### Interactive Menu-Driven CLI

The interactive CLI provides a user-friendly numbered menu system:

```bash
# Run the interactive CLI
python -m src.main

# Run with debug mode (verbose logging, full error traces)
python -m src.main --debug

# Run without screen clearing (keeps all output visible)
python -m src.main --no-clear

# Combine options
python -m src.main --debug --no-clear
```

Features:
- Numbered menu options for easy selection
- Run individual components with a single keypress
- Run full pipeline (fetch → scrape → extract → cluster → rank)
- Start/stop continuous processing in background
- View system status and recent results
- Colored output for better visibility
- **Persistent logging** - All operations logged to `logs/threatcluster_YYYYMMDD.log`
- **Debug mode** - Shows full error traces and verbose logging
- **No-clear mode** - Prevents screen clearing to see all output

Menu Options:
1. Fetch RSS Feeds - Get latest articles from configured feeds
2. Scrape Articles - Extract full article content from URLs
3. Extract Entities - Find security entities in articles
4. Cluster Articles - Group related articles semantically
5. Rank Articles - Score articles by importance
6. Run Full Pipeline Once - Execute all steps sequentially
7. Start Continuous Processing - Run pipeline repeatedly in background
8. Stop Continuous Processing - Stop the background processing
9. System Status - View counts and processing state
10. View Recent Results - See top articles and clusters
11. View Recent Log Entries - Display last 50 log entries with color coding

### Running Individual Components

To run components individually without the interactive menu, you can execute them directly:

```bash
# Import feeds from config
python -m src.database.sql.import_feeds

# Import entities from JSON files
python -m src.database.sql.import_entities

# Run RSS feed fetcher once
python -m src.feeds.scheduler --once

# Run article scraper once
python -m src.scraper.scraper_scheduler --once

# Run entity extraction once
python -m src.entity_extraction.entity_scheduler --once

# Run clustering once
python -m src.clustering.cluster_scheduler --once

# Run ranking once
python -m src.ranking.ranking_scheduler --once
```

### Daemon Mode

Each component can also run as a daemon for continuous operation:

```bash
# Run as daemons (periodic execution)
python -m src.feeds.scheduler
python -m src.scraper.scraper_scheduler
python -m src.entity_extraction.entity_scheduler
python -m src.clustering.cluster_scheduler
python -m src.ranking.ranking_scheduler
```

### Configuration

The scheduler settings in `config/config.yaml` control component execution:
- **interval_minutes**: How often each component runs in daemon mode
- **enabled**: Whether the component is active
- **delay_after_fetch_minutes**: Wait time between RSS fetch and article scraping

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

## Article Ranking

The article ranking system prioritizes articles based on multiple factors to surface the most important security news.

### Key Features

- **Multi-factor scoring**: Combines recency, source credibility, entity importance, and keyword severity
- **Weighted algorithm**: Configurable weights for each factor (default: 20% recency, 30% source, 30% entity, 20% keyword)
- **Dynamic cluster rankings**: Calculated from average article scores and cluster coherence
- **Comprehensive keyword library**: 361+ weighted keywords across 6 categories
- **Transparent scoring**: Stores detailed factor breakdowns for each article

### Running Article Ranking

```bash
# Run ranking once
python -m src.ranking.ranking_scheduler --once

# Run with specific time window (default: 72 hours)
python -m src.ranking.ranking_scheduler --once --hours 168

# Run as daemon (checks every 30 minutes)
python -m src.ranking.ranking_scheduler

# View ranking results
python -m tests.test_ranking_report
```

### Scoring Components

1. **Recency Score (0-100)**: Exponential decay over 24 hours
2. **Source Credibility (0-100)**: Based on RSS feed credibility rating
3. **Entity Importance (0-100)**: Weighted sum of extracted entities with bonuses for multiple high-importance entities
4. **Keyword Severity (0-100)**: Matches against weighted keywords with double weight for title matches

### Database Views

Two views provide easy access to ranking data:
- `cluster_data.articles_with_rankings`: Articles with full ranking details
- `cluster_data.cluster_rankings`: Clusters ranked by average article score and coherence

## Daily Email Service

The daily email service sends threat intelligence bulletins to subscribed users:

### Key Features

- **User Subscription**: Users can enable/disable daily emails via profile settings
- **AI Summaries**: Generates concise summaries of top clusters using OpenAI
- **Responsive HTML**: Professional email template with mobile support
- **Smart Scheduling**: Configurable send time (default 9:00 AM UTC)
- **Postmark Integration**: Reliable email delivery via Postmark API

### Running Daily Email Service

```bash
# Test email service (send immediately)
python -m src.email_service.email_service

# Run scheduler once  
python -m src.email_service.daily_email_scheduler --once

# Run as daemon (sends daily at configured time)
python -m src.email_service.daily_email_scheduler
```

### Configuration

Add to your `.env` file:
```bash
# Email Configuration (Postmark)
POSTMARK_API_TOKEN=your-postmark-api-token
EMAIL_FROM_ADDRESS=alerts@threatcluster.io
DAILY_EMAIL_SEND_TIME=09:00  # 24-hour format UTC
```

### Email Content

Each daily bulletin includes:
- Top 5 highest-scoring clusters from the last 24 hours
- AI-generated summaries for each cluster
- Links to full cluster analysis on ThreatCluster
- Article counts and creation times
- User subscription management link

## Entity Notification Service

The entity notification service sends alerts when new articles mention entities in a user's custom feeds:

### Key Features

- **Custom Feed Support**: Users can organize followed entities into custom feeds
- **Feed-Level Notifications**: Users can enable/disable notifications per feed
- **Entity Monitoring**: Tracks when new articles mention followed entities
- **Smart Batching**: Groups notifications by feed to reduce email volume
- **AWS SES Integration**: Reliable email delivery via Amazon SES
- **Activity Tracking**: Updates last activity timestamps for notified entities

### Running Entity Notification Service

```bash
# Test notification service
python -m tests.test_entity_notification

# Run scheduler once
python -m src.email_service.entity_notification_scheduler --once

# Run as daemon (checks every 30 minutes)
python -m src.email_service.entity_notification_scheduler
```

### Database Setup

Run the migrations to add feed support:
```bash
psql $DATABASE_URL -f src/database/sql/add_entity_feed_support.sql
psql $DATABASE_URL -f src/database/sql/add_feed_notification_preference.sql
```

### Configuration

Ensure AWS SES is configured in your `.env`:
```bash
# AWS SES Configuration
AWS_SES_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Notification Content

Entity notifications include:
- Articles grouped by entity
- Entity name and category
- Article title, source, and publication date
- Content preview
- Direct links to articles
- Feed management links

### API Endpoints

The API provides endpoints to manage feed notifications:
- `PATCH /api/v1/entity-feeds/{feed_id}/notifications` - Toggle notifications for a feed
- `PUT /api/v1/entity-feeds/{feed_id}` - Update feed including notifications_enabled field

## AI Entity Extraction and Management

The AI entity extraction system dynamically discovers and manages security entities using OpenAI:

### Key Features

- **AI-Powered Extraction**: Uses GPT-4o to extract entities from cluster summaries
- **Automatic Entity Creation**: Discovered entities are automatically added to the database
- **Entity Linking**: AI-extracted entities are linked to all articles in their source clusters
- **Description Generation**: Uses GPT-4o-mini to generate concise descriptions for new entities
- **Full Integration**: AI entities appear in cluster intelligence views and entity pages

### Components

1. **Entity Extraction in AI Summaries** (`src/ai_summary/prompts.py`)
   - Extracts technical indicators (CVEs, IPs, domains)
   - Identifies threat actors (APT groups, ransomware groups)
   - Discovers business entities (companies, vendors, agencies)

2. **Entity Sync Service** (`src/ai_summary/entity_sync_service.py`)
   - Syncs AI-extracted entities to the entities table
   - Assigns appropriate importance weights by category
   - Tracks new vs existing entities

3. **Entity Link Service** (`src/ai_summary/entity_link_service.py`)
   - Links AI entities to cluster articles
   - Merges with existing extracted entities
   - Maintains extraction metadata

4. **Entity Description Service** (`src/ai_summary/entity_description_service.py`)
   - Generates 1-2 sentence descriptions using GPT-4o-mini
   - Category-specific prompt engineering
   - Batch processing for efficiency

### Running Entity Services

```bash
# Test entity sync and linking
python -m test_entity_sync 1450

# Generate descriptions for entities without them
python -m src.ai_summary.entity_description_scheduler --once

# Run description generation as daemon
python -m src.ai_summary.entity_description_scheduler --interval 30

# Test full flow (AI summary -> entities -> descriptions)
python -m test_full_entity_flow
```

### Entity Flow

1. AI summary generation extracts entities from cluster articles
2. Entity sync service adds new entities to database
3. Entity link service connects entities to all cluster articles
4. Description service generates descriptions for new entities
5. Entities appear in cluster intelligence and entity pages with full metadata