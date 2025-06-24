Article Scraper Tests

This file describes the tests available for the article scraper module.

HOW TO RUN ALL TESTS
From the cluster-service directory:
python -m tests.test_article_scraper

HOW TO RUN ARTICLE SCRAPER
python -m src.scraper.article_scraper --help
python -m src.scraper.article_scraper --test     # Test with single article
python -m src.scraper.article_scraper --limit 10  # Process 10 articles
python -m src.scraper.article_scraper             # Process default batch size

HOW TO RUN SCRAPER SCHEDULER
python -m src.scraper.scraper_scheduler --once    # Run once and exit
python -m src.scraper.scraper_scheduler           # Run as daemon

INDIVIDUAL TESTS

1. Database Connection Test
   Description: Verifies connection to PostgreSQL and finds unprocessed articles
   What it does:
   - Connects to the database
   - Fetches unprocessed articles from rss_feeds_raw
   - Shows sample article details
   Pass criteria: Successfully connects and finds articles

2. Content Extraction Test
   Description: Tests HTML parsing and content extraction logic
   What it does:
   - Parses sample HTML with BeautifulSoup
   - Extracts article text content
   - Extracts image URLs
   - Removes unwanted elements (nav, footer, etc)
   Pass criteria: Extracts clean content and images

3. Rate Limiting Test
   Description: Tests domain-based rate limiting
   What it does:
   - Tests delay enforcement between requests
   - Verifies per-domain tracking
   - Ensures different domains are not delayed
   Pass criteria: Proper delays are enforced

4. Single Article Scraping Test
   Description: Tests scraping a real article from the database
   What it does:
   - Fetches one unprocessed article
   - Makes HTTP request to article URL
   - Extracts content and images
   - Shows scraping results
   Pass criteria: Successfully scrapes or falls back to RSS

5. Batch Processing Test
   Description: Tests processing multiple articles
   What it does:
   - Processes up to 3 articles
   - Handles successes and failures
   - Stores results in database
   - Reports statistics
   Pass criteria: Processes at least one article

6. Clean Table Storage Test
   Description: Verifies scraped content is stored correctly
   What it does:
   - Checks rss_feeds_clean table
   - Shows sample stored article
   - Verifies data structure
   Pass criteria: Articles exist in clean table

IMPORTANT NOTES
- Tests use REAL articles from your database
- Tests make REAL HTTP requests to external websites
- Tests respect rate limiting (1-2 second delays)
- Network connectivity required for scraping tests
- Some articles may fail due to paywalls or anti-bot protection

EXPECTED OUTPUT
Each test shows its progress and results.
Scraping tests show content length and image counts.
Final summary shows X/6 tests passed.
Exit code 0 means all tests passed.

COMMON ISSUES
- 403 Forbidden: Site blocks automated access
- Timeout: Site is slow or unreachable
- Empty content: Site uses JavaScript rendering
- Rate limiting: Too many requests to same domain

The scraper includes fallback to RSS description when full scraping fails.