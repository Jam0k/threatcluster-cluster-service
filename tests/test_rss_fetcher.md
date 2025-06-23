RSS Feed Fetcher Tests

This file describes the tests available for the RSS feed fetcher module.

HOW TO RUN ALL TESTS
From the cluster-service directory:
python -m tests.test_rss_fetcher

INDIVIDUAL TESTS

1. Database Connection Test
   Description: Verifies connection to PostgreSQL database and lists active feeds
   What it does:
   - Connects to the database using settings from .env
   - Fetches list of active feeds from rss_feeds table
   - Displays count and first 5 feeds
   Pass criteria: Successfully connects and finds at least 1 feed

2. Security Filtering Test  
   Description: Tests the keyword filtering logic for identifying security content
   What it does:
   - Tests 8 sample text strings
   - Checks if security keywords are properly detected
   - Validates both positive and negative cases
   Pass criteria: All 8 test cases pass

3. Single Feed Fetch Test
   Description: Tests fetching and processing a single RSS feed
   What it does:
   - Fetches real RSS data from Krebs on Security
   - Processes the articles
   - Stores them in the database
   - Reports statistics
   Pass criteria: Successfully stores at least 1 article

4. Full Process Test
   Description: Tests the complete RSS processing pipeline with multiple feeds
   What it does:
   - Gets first 3 feeds from database
   - Fetches real RSS data from each feed
   - Applies filtering if needed
   - Stores articles in database
   - Reports total statistics
   Pass criteria: Successfully processes feeds and stores articles

IMPORTANT NOTES
- Tests use REAL data from live RSS feeds
- Tests make REAL HTTP requests to external sites
- Tests store REAL data in your database
- Network connectivity required for tests to pass
- Database must be properly configured in .env

EXPECTED OUTPUT
Each test shows its progress and results.
Final summary shows X/4 tests passed.
Exit code 0 means all tests passed.
Exit code 1 means at least one test failed.