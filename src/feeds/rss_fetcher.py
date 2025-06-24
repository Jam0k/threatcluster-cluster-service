#!/usr/bin/env python3
"""
RSS Feed Fetcher Module

Fetches cybersecurity news from RSS feeds, applies security filtering,
and stores raw data in PostgreSQL for further processing.
"""
import feedparser
import psycopg2
import psycopg2.extras
import requests
import logging
from logging.handlers import RotatingFileHandler
import time
import json
import os
import re
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple, Any
import structlog

from src.config.settings import settings


# Configure structured logging
logger = structlog.get_logger(__name__)

# Configure file logging for RSS fetcher
def setup_file_logging():
    """Set up rotating file handler for RSS fetcher logs."""
    log_dir = os.path.join(os.path.dirname(__file__))
    log_file = os.path.join(log_dir, 'rss_fetcher.log')
    
    # Create a specific logger for file output
    file_logger = logging.getLogger('rss_fetcher_file')
    file_logger.setLevel(logging.INFO)
    
    # Create rotating file handler (10MB max, keep 5 backups)
    handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # Simple format for file logs
    formatter = logging.Formatter(
        '%(asctime)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    # Add handler if not already added
    if not file_logger.handlers:
        file_logger.addHandler(handler)
    
    return file_logger

# Set up file logging
file_logger = setup_file_logging()


class RSSFeedFetcher:
    """Fetches and processes RSS feeds with security filtering."""
    
    def __init__(self):
        """Initialize the RSS feed fetcher with configuration."""
        self.config = settings.app_config
        self.security_keywords = [kw.lower() for kw in self.config.get('security_keywords', [])]
        self.scraping_config = self.config.get('scraping', {})
        self.pipeline_config = self.config.get('pipeline', {})
        self.user_agent = self.scraping_config.get('user_agent', 'ThreatCluster/1.0')
        self.timeout = self.scraping_config.get('timeout_seconds', 30)
        self.max_retries = self.scraping_config.get('max_retries', 3)
        self.rate_limit_delay = self.scraping_config.get('rate_limit_delay', 1.0)
        self.time_window_hours = self.pipeline_config.get('time_window_hours', 72)
        
        # Load enhanced security keywords
        self._load_enhanced_keywords()
        
        # Calculate cutoff time for articles
        self.cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.time_window_hours)
        
        # Statistics tracking
        self.stats = {
            'feeds_processed': 0,
            'feeds_failed': 0,
            'articles_fetched': 0,
            'articles_filtered': 0,
            'articles_stored': 0,
            'duplicates_skipped': 0,
            'articles_too_old': 0,
            'errors': []
        }
    
    def _load_enhanced_keywords(self):
        """Load enhanced security keywords configuration."""
        import yaml
        
        # Try to load enhanced keywords
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'security_keywords.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                keywords_config = yaml.safe_load(f)
                
            self.required_keywords = [kw.lower() for kw in keywords_config.get('required_keywords', [])]
            self.exclusion_patterns = keywords_config.get('exclusion_patterns', [])
            self.non_security_titles = keywords_config.get('non_security_title_patterns', [])
            self.excluded_domains = keywords_config.get('excluded_domains', [])
            
            logger.info("loaded_enhanced_keywords",
                       required=len(self.required_keywords),
                       exclusions=len(self.exclusion_patterns))
        else:
            # Fallback to basic keywords
            self.required_keywords = self.security_keywords
            self.exclusion_patterns = []
            self.non_security_titles = []
            self.excluded_domains = []
            logger.warning("enhanced_keywords_not_found", path=config_path)
    
    def fetch_active_feeds(self) -> List[Dict[str, Any]]:
        """Fetch active RSS feed configurations from database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            cursor.execute("""
                SELECT rss_feeds_id, rss_feeds_url, rss_feeds_name, 
                       rss_feeds_category, rss_feeds_credibility
                FROM cluster_data.rss_feeds
                WHERE rss_feeds_is_active = TRUE
                ORDER BY rss_feeds_credibility DESC
            """)
            feeds = [dict(row) for row in cursor.fetchall()]
            logger.info("fetched_active_feeds", count=len(feeds))
            return feeds
        finally:
            cursor.close()
            conn.close()
    
    def fetch_rss_feed(self, feed_url: str, feed_name: str) -> Optional[feedparser.FeedParserDict]:
        """Fetch and parse RSS feed with retry logic."""
        headers = {'User-Agent': self.user_agent}
        
        for attempt in range(self.max_retries):
            try:
                logger.debug("fetching_feed", feed_name=feed_name, attempt=attempt+1)
                
                # First, fetch the feed content with requests (with timeout)
                response = requests.get(
                    feed_url,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                response.raise_for_status()
                
                # Then parse the content with feedparser
                feed = feedparser.parse(response.content)
                
                # Check for parsing errors
                if feed.bozo and feed.bozo_exception:
                    logger.warning("feed_parse_error", 
                                 feed_name=feed_name,
                                 error=str(feed.bozo_exception))
                
                # Check if we got entries
                if not feed.entries:
                    logger.warning("empty_feed", feed_name=feed_name)
                    return None
                
                logger.info("feed_fetched_successfully", 
                          feed_name=feed_name, 
                          entries=len(feed.entries))
                return feed
                
            except requests.exceptions.Timeout:
                error_msg = f"Timeout after {self.timeout}s"
                logger.error("feed_fetch_timeout",
                           feed_name=feed_name,
                           attempt=attempt+1,
                           timeout=self.timeout)
                if attempt >= self.max_retries - 1:
                    self.stats['errors'].append(f"{feed_name}: {error_msg}")
                    file_logger.info(f"{feed_name} | Fetched: 0 | Stored: 0 | Status: FAILED - {error_msg}")
                
            except requests.exceptions.RequestException as e:
                error_msg = f"Network error: {str(e)}"
                logger.error("feed_fetch_network_error",
                           feed_name=feed_name,
                           attempt=attempt+1,
                           error=str(e))
                if attempt >= self.max_retries - 1:
                    self.stats['errors'].append(f"{feed_name}: {error_msg}")
                    file_logger.info(f"{feed_name} | Fetched: 0 | Stored: 0 | Status: FAILED - {error_msg}")
                    
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                logger.error("feed_fetch_error",
                           feed_name=feed_name,
                           attempt=attempt+1,
                           error=str(e))
                if attempt >= self.max_retries - 1:
                    self.stats['errors'].append(f"{feed_name}: {error_msg}")
                    file_logger.info(f"{feed_name} | Fetched: 0 | Stored: 0 | Status: FAILED - {error_msg}")
            
            # Retry with exponential backoff if not last attempt
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)
        
        return None
    
    def is_security_relevant(self, title: str, description: str, link: str = "") -> bool:
        """Enhanced security relevance check with exclusion patterns."""
        # Combine title and description for checking
        text = f"{title} {description}".lower()
        title_lower = title.lower()
        
        # Check excluded domains
        if link:
            for domain in self.excluded_domains:
                if domain in link.lower():
                    logger.debug("excluded_domain", domain=domain, title=title[:50])
                    return False
        
        # Check non-security title patterns
        for pattern in self.non_security_titles:
            if re.match(pattern, title_lower):
                logger.debug("non_security_title_pattern", pattern=pattern, title=title[:50])
                return False
        
        # Check exclusion patterns
        for exclusion in self.exclusion_patterns:
            pattern = exclusion['pattern'].lower()
            unless_keywords = [kw.lower() for kw in exclusion.get('unless_contains', [])]
            
            if pattern in text:
                # Check if any of the "unless" keywords are present
                has_security_context = any(kw in text for kw in unless_keywords)
                if not has_security_context:
                    logger.debug("excluded_pattern", pattern=pattern, title=title[:50])
                    return False
        
        # Check for required security keywords
        has_security_keyword = any(keyword in text for keyword in self.required_keywords)
        
        if not has_security_keyword:
            logger.debug("no_security_keywords", title=title[:50])
            return False
        
        return True
    
    def parse_pubdate(self, date_string: str) -> datetime:
        """Parse various RSS date formats into datetime object."""
        if not date_string:
            return datetime.now(timezone.utc)
        
        try:
            # Use email.utils for RFC 2822 date parsing (standard RSS format)
            parsed_date = parsedate_to_datetime(date_string)
            
            # Convert to UTC if it has timezone info
            if parsed_date.tzinfo is not None:
                return parsed_date.astimezone(timezone.utc)
            else:
                # Assume UTC if no timezone specified
                return parsed_date.replace(tzinfo=timezone.utc)
                
        except (ValueError, TypeError) as e:
            # Try ISO format as fallback
            try:
                # Normalize ISO format with varying decimal seconds (e.g., .00Z to .000Z)
                normalized_date = re.sub(r'\.(\d{1,6})Z$', lambda m: '.{:0<6}Z'.format(m.group(1)), date_string)
                normalized_date = normalized_date.replace('Z', '+00:00')
                
                parsed_date = datetime.fromisoformat(normalized_date)
                return parsed_date.astimezone(timezone.utc)
            except (ValueError, AttributeError):
                pass
        
        # Final fallback to current time if all parsing fails
        logger.warning("date_parse_failed", date_string=date_string)
        return datetime.now(timezone.utc)
    
    def extract_article_data(self, entry: Dict, feed_id: int, feed_name: str) -> Optional[Dict]:
        """Extract the 4 key XML fields from RSS entry."""
        try:
            # Extract core fields
            title = entry.get('title', '').strip()
            link = entry.get('link', '').strip()
            description = entry.get('description', '').strip()
            pub_date_str = entry.get('published', entry.get('pubDate', ''))
            
            # Validate required fields
            if not title or not link:
                logger.warning("missing_required_fields", 
                             feed_name=feed_name,
                             has_title=bool(title),
                             has_link=bool(link))
                return None
            
            # Parse publication date
            pub_date = self.parse_pubdate(pub_date_str)
            
            # Create article data structure
            article_data = {
                'feed_id': feed_id,
                'xml_data': {
                    'title': title,
                    'link': link,
                    'description': description,
                    'pubDate': pub_date_str
                },
                'published_date': pub_date,
                'processed': False
            }
            
            return article_data
            
        except Exception as e:
            logger.error("article_extraction_error",
                       feed_name=feed_name,
                       error=str(e))
            return None
    
    def check_duplicate(self, cursor: psycopg2.extensions.cursor, link: str) -> bool:
        """Check if article already exists in database."""
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM cluster_data.rss_feeds_raw
                WHERE rss_feeds_raw_xml->>'link' = %s
            )
        """, (link,))
        return cursor.fetchone()[0]
    
    def store_articles(self, articles: List[Dict]) -> Tuple[int, int]:
        """Store articles in database with duplicate detection."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        stored = 0
        duplicates = 0
        
        try:
            for article in articles:
                link = article['xml_data']['link']
                
                # Check for duplicate
                if self.check_duplicate(cursor, link):
                    duplicates += 1
                    logger.debug("duplicate_article", link=link)
                    continue
                
                # Insert article
                cursor.execute("""
                    INSERT INTO cluster_data.rss_feeds_raw
                    (rss_feeds_raw_feed_id, rss_feeds_raw_xml, 
                     rss_feeds_raw_published_date, rss_feeds_raw_processed)
                    VALUES (%s, %s, %s, %s)
                """, (
                    article['feed_id'],
                    json.dumps(article['xml_data']),
                    article['published_date'],
                    article['processed']
                ))
                stored += 1
            
            conn.commit()
            logger.info("articles_stored", stored=stored, duplicates=duplicates)
            
        except Exception as e:
            logger.error("storage_error", error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
        
        return stored, duplicates
    
    def process_feed(self, feed_config: Dict) -> Dict[str, int]:
        """Process a single RSS feed."""
        feed_stats = {
            'fetched': 0,
            'filtered': 0,
            'stored': 0,
            'duplicates': 0
        }
        
        feed_id = feed_config['rss_feeds_id']
        feed_url = feed_config['rss_feeds_url']
        feed_name = feed_config['rss_feeds_name']
        feed_category = feed_config['rss_feeds_category']
        
        logger.info("processing_feed", feed_name=feed_name, category=feed_category)
        start_time = time.time()
        
        # Fetch feed
        feed = self.fetch_rss_feed(feed_url, feed_name)
        if not feed:
            self.stats['feeds_failed'] += 1
            # Log failure to file
            file_logger.info(f"{feed_name} | Fetched: 0 | Stored: 0 | Status: FAILED - Unable to fetch feed")
            return feed_stats
        
        # Process entries
        articles_to_store = []
        
        for entry in feed.entries:
            feed_stats['fetched'] += 1
            
            # First check publication date to avoid processing old articles
            pub_date_str = entry.get('published', entry.get('pubDate', ''))
            pub_date = self.parse_pubdate(pub_date_str)
            
            # Skip articles older than the time window
            if pub_date < self.cutoff_time:
                self.stats['articles_too_old'] += 1
                logger.debug("article_too_old", 
                           feed_name=feed_name,
                           title=entry.get('title', '')[:50],
                           pub_date=pub_date.isoformat(),
                           cutoff_time=self.cutoff_time.isoformat())
                continue
            
            # Extract article data
            article = self.extract_article_data(entry, feed_id, feed_name)
            if not article:
                continue
            
            # Apply enhanced security filtering to ALL feeds
            title = article['xml_data']['title']
            description = article['xml_data']['description']
            link = article['xml_data']['link']
            
            # Apply stricter filtering for feeds like NANOG that include non-security content
            if not self.is_security_relevant(title, description, link):
                feed_stats['filtered'] += 1
                logger.debug("article_filtered", 
                           feed_name=feed_name,
                           category=feed_category,
                           title=title[:50])
                continue
            
            articles_to_store.append(article)
        
        # Store articles
        if articles_to_store:
            stored, duplicates = self.store_articles(articles_to_store)
            feed_stats['stored'] = stored
            feed_stats['duplicates'] = duplicates
        
        # Log success to file
        processing_time = round(time.time() - start_time, 2)
        file_logger.info(
            f"{feed_name} | Fetched: {feed_stats['fetched']} | "
            f"Stored: {feed_stats['stored']} | Status: SUCCESS | "
            f"Time: {processing_time}s"
        )
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        return feed_stats
    
    def process_all_feeds(self) -> Dict[str, Any]:
        """Process all active RSS feeds."""
        logger.info("starting_rss_feed_processing", 
                   time_window_hours=self.time_window_hours)
        start_time = time.time()
        
        # Refresh cutoff time for this run
        self.cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.time_window_hours)
        
        # Reset statistics
        self.stats = {
            'feeds_processed': 0,
            'feeds_failed': 0,
            'articles_fetched': 0,
            'articles_filtered': 0,
            'articles_stored': 0,
            'duplicates_skipped': 0,
            'articles_too_old': 0,
            'errors': []
        }
        
        # Fetch active feeds
        feeds = self.fetch_active_feeds()
        if not feeds:
            logger.warning("no_active_feeds")
            return self.stats
        
        # Process each feed
        for feed in feeds:
            try:
                feed_stats = self.process_feed(feed)
                
                # Update global statistics
                self.stats['feeds_processed'] += 1
                self.stats['articles_fetched'] += feed_stats['fetched']
                self.stats['articles_filtered'] += feed_stats['filtered']
                self.stats['articles_stored'] += feed_stats['stored']
                self.stats['duplicates_skipped'] += feed_stats['duplicates']
                
            except Exception as e:
                logger.error("feed_processing_error",
                           feed_name=feed['rss_feeds_name'],
                           error=str(e))
                self.stats['feeds_failed'] += 1
                self.stats['errors'].append(f"{feed['rss_feeds_name']}: {str(e)}")
        
        # Calculate processing time
        processing_time = time.time() - start_time
        self.stats['processing_time_seconds'] = round(processing_time, 2)
        
        # Log summary
        logger.info("rss_processing_completed",
                   **self.stats)
        
        # Log summary to file
        file_logger.info(
            f"SUMMARY | Total Feeds: {self.stats['feeds_processed']} | "
            f"Failed: {self.stats['feeds_failed']} | "
            f"Articles Fetched: {self.stats['articles_fetched']} | "
            f"Articles Stored: {self.stats['articles_stored']} | "
            f"Too Old: {self.stats['articles_too_old']} | "
            f"Total Time: {self.stats['processing_time_seconds']}s"
        )
        
        return self.stats


def main():
    """Run RSS feed fetcher as standalone script."""
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    fetcher = RSSFeedFetcher()
    stats = fetcher.process_all_feeds()
    
    # Print summary
    print("\nRSS Feed Processing Summary:")
    print("=" * 50)
    print(f"Time window: Last {fetcher.time_window_hours} hours")
    print(f"Feeds processed: {stats['feeds_processed']}")
    print(f"Feeds failed: {stats['feeds_failed']}")
    print(f"Articles fetched: {stats['articles_fetched']}")
    print(f"Articles too old: {stats['articles_too_old']}")
    print(f"Articles filtered: {stats['articles_filtered']}")
    print(f"Articles stored: {stats['articles_stored']}")
    print(f"Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"Processing time: {stats.get('processing_time_seconds', 0)} seconds")
    
    if stats['errors']:
        print(f"\nErrors encountered: {len(stats['errors'])}")
        for error in stats['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")


if __name__ == "__main__":
    main()