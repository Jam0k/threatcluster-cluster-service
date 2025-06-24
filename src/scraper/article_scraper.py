#!/usr/bin/env python3
"""
Article Content Scraper Module

Fetches full article content from RSS feed links, extracts clean text and images,
and stores the content in the database for further processing.
"""
import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
import time
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any, Set
from urllib.parse import urlparse, urljoin
from collections import defaultdict
import re
import structlog

from src.config.settings import settings


# Configure structured logging
logger = structlog.get_logger(__name__)

# Configure file logging for scraper
def setup_file_logging():
    """Set up rotating file handler for scraper logs."""
    log_dir = os.path.join(os.path.dirname(__file__))
    log_file = os.path.join(log_dir, 'article_scraper.log')
    
    # Create a specific logger for file output
    file_logger = logging.getLogger('article_scraper_file')
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


class ArticleScraper:
    """Scrapes full article content from RSS feed links."""
    
    # Common content selectors to try
    CONTENT_SELECTORS = [
        'article',
        '[role="main"]',
        '.article-content',
        '.post-content',
        '.entry-content',
        '.content-body',
        '.story-body',
        '.article-body',
        'main',
        '.main-content',
        '#content',
        '.content',
        '[itemprop="articleBody"]',
        '.post',
        '.entry',
        '.single-post'
    ]
    
    # Elements to remove from content
    REMOVE_SELECTORS = [
        'script',
        'style',
        'nav',
        'header',
        'footer',
        '.advertisement',
        '.ads',
        '.social-share',
        '.comments',
        '.related-articles',
        '.sidebar',
        '.newsletter-signup',
        '.popup',
        '.modal'
    ]
    
    # Minimum content length to be considered valid
    MIN_CONTENT_LENGTH = 100
    
    # Image file extensions
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp'}
    
    def __init__(self):
        """Initialize the article scraper with configuration."""
        self.config = settings.app_config
        self.scraping_config = self.config.get('scraping', {})
        self.pipeline_config = self.config.get('pipeline', {})
        
        # Scraping settings
        self.user_agent = self.scraping_config.get('user_agent', 'ThreatCluster/1.0')
        self.timeout = self.scraping_config.get('timeout_seconds', 30)
        self.max_retries = self.scraping_config.get('max_retries', 3)
        self.rate_limit_delay = self.scraping_config.get('rate_limit_delay', 1.0)
        self.batch_size = self.pipeline_config.get('processing_batch_size', 100)
        
        # Domain rate limiting tracker
        self.domain_last_access = defaultdict(float)
        
        # Statistics tracking
        self.stats = {
            'articles_attempted': 0,
            'articles_success': 0,
            'articles_failed': 0,
            'content_fallback': 0,
            'total_content_length': 0,
            'errors': []
        }
    
    def get_unprocessed_articles(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch unprocessed articles from the database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            query = """
                SELECT 
                    rss_feeds_raw_id,
                    rss_feeds_raw_feed_id,
                    rss_feeds_raw_xml,
                    rss_feeds_raw_published_date
                FROM cluster_data.rss_feeds_raw
                WHERE rss_feeds_raw_processed = FALSE
                ORDER BY rss_feeds_raw_published_date DESC
                LIMIT %s
            """
            cursor.execute(query, (limit or self.batch_size,))
            articles = [dict(row) for row in cursor.fetchall()]
            
            logger.info("fetched_unprocessed_articles", count=len(articles))
            return articles
            
        finally:
            cursor.close()
            conn.close()
    
    def enforce_rate_limit(self, domain: str):
        """Enforce rate limiting per domain."""
        last_access = self.domain_last_access.get(domain, 0)
        time_since_last = time.time() - last_access
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            logger.debug("rate_limiting", domain=domain, sleep_seconds=sleep_time)
            time.sleep(sleep_time)
        
        self.domain_last_access[domain] = time.time()
    
    def fetch_page_content(self, url: str) -> Optional[requests.Response]:
        """Fetch webpage content with retry logic."""
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                    verify=True
                )
                
                # Check for success
                if response.status_code == 200:
                    return response
                
                # Handle specific error codes
                if response.status_code == 404:
                    logger.warning("page_not_found", url=url)
                    return None
                elif response.status_code in [403, 401]:
                    logger.warning("access_denied", url=url, status=response.status_code)
                    return None
                elif response.status_code == 429:
                    logger.warning("rate_limited", url=url)
                    time.sleep(30)  # Wait longer for rate limiting
                
            except requests.exceptions.Timeout:
                logger.error("request_timeout", url=url, attempt=attempt+1)
            except requests.exceptions.ConnectionError:
                logger.error("connection_error", url=url, attempt=attempt+1)
            except Exception as e:
                logger.error("fetch_error", url=url, error=str(e), attempt=attempt+1)
            
            # Exponential backoff
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)
        
        return None
    
    def extract_content(self, html: str, url: str) -> Tuple[str, List[str]]:
        """Extract article content and images from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements
        for selector in self.REMOVE_SELECTORS:
            for element in soup.select(selector):
                element.decompose()
        
        # Try to find main content
        content_element = None
        for selector in self.CONTENT_SELECTORS:
            content_element = soup.select_one(selector)
            if content_element:
                break
        
        # Fallback to body if no specific content area found
        if not content_element:
            content_element = soup.body or soup
        
        # Extract text
        text_content = self.extract_text(content_element)
        
        # Extract images
        image_urls = self.extract_images(soup, url)
        
        return text_content, image_urls
    
    def extract_text(self, element) -> str:
        """Extract clean text from HTML element."""
        if not element:
            return ""
        
        # Get all text with preserved structure
        lines = []
        for item in element.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'blockquote']):
            text = item.get_text(strip=True)
            if text:
                lines.append(text)
        
        # Join with double newlines to preserve paragraph structure
        content = '\n\n'.join(lines)
        
        # Clean up excessive whitespace
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r' {2,}', ' ', content)
        
        return content.strip()
    
    def extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract image URLs from the page."""
        image_urls = set()
        
        # Extract from img tags
        for img in soup.find_all('img'):
            src = img.get('src', '').strip()
            if src:
                full_url = urljoin(base_url, src)
                if self.is_valid_image_url(full_url):
                    image_urls.add(full_url)
            
            # Also check data-src for lazy loading
            data_src = img.get('data-src', '').strip()
            if data_src:
                full_url = urljoin(base_url, data_src)
                if self.is_valid_image_url(full_url):
                    image_urls.add(full_url)
        
        # Extract from links to images
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if any(href.lower().endswith(ext) for ext in self.IMAGE_EXTENSIONS):
                full_url = urljoin(base_url, href)
                if self.is_valid_image_url(full_url):
                    image_urls.add(full_url)
        
        return list(image_urls)
    
    def is_valid_image_url(self, url: str) -> bool:
        """Check if URL is a valid image URL."""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Check for common ad/tracking domains
            if any(domain in parsed.netloc for domain in ['doubleclick', 'googleadservices', 'amazon-adsystem']):
                return False
            
            # Check for tiny images (likely icons)
            if any(size in url for size in ['1x1', '2x2', 'pixel', 'spacer']):
                return False
            
            return True
        except:
            return False
    
    def scrape_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape a single article."""
        raw_id = article['rss_feeds_raw_id']
        feed_id = article['rss_feeds_raw_feed_id']
        xml_data = article['rss_feeds_raw_xml']
        
        # Extract URL and basic info from RSS data
        url = xml_data.get('link', '').strip()
        title = xml_data.get('title', '').strip()
        rss_description = xml_data.get('description', '').strip()
        
        if not url:
            logger.warning("no_url_found", raw_id=raw_id)
            return None
        
        logger.info("scraping_article", url=url, title=title[:50])
        
        # Parse domain for rate limiting
        domain = urlparse(url).netloc
        self.enforce_rate_limit(domain)
        
        # Fetch page content
        response = self.fetch_page_content(url)
        
        scraped_content = ""
        image_urls = []
        success = False
        error_message = None
        
        if response:
            try:
                # Extract content and images
                scraped_content, image_urls = self.extract_content(response.text, url)
                
                # Validate content length
                if len(scraped_content) >= self.MIN_CONTENT_LENGTH:
                    success = True
                    logger.info("scraping_success", 
                              url=url,
                              content_length=len(scraped_content),
                              images_found=len(image_urls))
                else:
                    logger.warning("insufficient_content", 
                                 url=url,
                                 content_length=len(scraped_content))
                    error_message = "Insufficient content extracted"
                    
            except Exception as e:
                logger.error("extraction_error", url=url, error=str(e))
                error_message = f"Extraction error: {str(e)}"
        else:
            error_message = "Failed to fetch page"
        
        # Fall back to RSS description if scraping failed
        if not success and rss_description:
            scraped_content = rss_description
            self.stats['content_fallback'] += 1
            logger.info("using_rss_fallback", url=url)
        
        # Prepare result
        result = {
            'raw_id': raw_id,
            'feed_id': feed_id,
            'title': title,
            'content': scraped_content,
            'images': image_urls,
            'success': success,
            'error': error_message,
            'content_length': len(scraped_content),
            'scraped_at': datetime.now(timezone.utc)
        }
        
        return result
    
    def store_scraped_content(self, results: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Store scraped content in the database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        stored = 0
        updated_raw = 0
        
        try:
            for result in results:
                if not result:
                    continue
                
                # Insert into rss_feeds_clean
                cursor.execute("""
                    INSERT INTO cluster_data.rss_feeds_clean
                    (rss_feeds_clean_raw_id, rss_feeds_clean_title, 
                     rss_feeds_clean_content, rss_feeds_clean_images,
                     rss_feeds_clean_processed)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (rss_feeds_clean_raw_id) DO NOTHING
                """, (
                    result['raw_id'],
                    json.dumps({'title': result['title']}),
                    json.dumps({
                        'content': result['content'],
                        'success': result['success'],
                        'error': result['error'],
                        'content_length': result['content_length'],
                        'scraped_at': result['scraped_at'].isoformat()
                    }),
                    json.dumps(result['images']) if result['images'] else None,
                    False  # Not processed for next stage
                ))
                
                if cursor.rowcount > 0:
                    stored += 1
                
                # Mark raw article as processed
                cursor.execute("""
                    UPDATE cluster_data.rss_feeds_raw
                    SET rss_feeds_raw_processed = TRUE
                    WHERE rss_feeds_raw_id = %s
                """, (result['raw_id'],))
                
                if cursor.rowcount > 0:
                    updated_raw += 1
            
            conn.commit()
            logger.info("content_stored", stored=stored, raw_updated=updated_raw)
            
        except Exception as e:
            logger.error("storage_error", error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
        
        return stored, updated_raw
    
    def process_batch(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Process a batch of articles."""
        logger.info("starting_batch_processing", limit=limit or self.batch_size)
        start_time = time.time()
        
        # Get unprocessed articles
        articles = self.get_unprocessed_articles(limit)
        if not articles:
            logger.info("no_unprocessed_articles")
            return self.stats
        
        # Process each article
        results = []
        for i, article in enumerate(articles):
            self.stats['articles_attempted'] += 1
            
            try:
                result = self.scrape_article(article)
                
                if result:
                    results.append(result)
                    
                    if result['success']:
                        self.stats['articles_success'] += 1
                        self.stats['total_content_length'] += result['content_length']
                    else:
                        self.stats['articles_failed'] += 1
                        if result['error']:
                            self.stats['errors'].append(result['error'])
                
                # Log progress
                if (i + 1) % 10 == 0:
                    logger.info("batch_progress", 
                              processed=i+1,
                              total=len(articles))
                    
            except Exception as e:
                logger.error("article_processing_error",
                           raw_id=article['rss_feeds_raw_id'],
                           error=str(e))
                self.stats['articles_failed'] += 1
                self.stats['errors'].append(str(e))
        
        # Store results
        if results:
            stored, updated = self.store_scraped_content(results)
            logger.info("batch_storage_complete", stored=stored, updated=updated)
        
        # Calculate statistics
        processing_time = time.time() - start_time
        self.stats['processing_time_seconds'] = round(processing_time, 2)
        
        if self.stats['articles_success'] > 0:
            self.stats['avg_content_length'] = int(
                self.stats['total_content_length'] / self.stats['articles_success']
            )
        
        # Log summary
        logger.info("batch_processing_completed", **self.stats)
        
        # Log to file
        file_logger.info(
            f"BATCH COMPLETE | Attempted: {self.stats['articles_attempted']} | "
            f"Success: {self.stats['articles_success']} | "
            f"Failed: {self.stats['articles_failed']} | "
            f"Fallback: {self.stats['content_fallback']} | "
            f"Time: {self.stats['processing_time_seconds']}s"
        )
        
        return self.stats


def main():
    """Run article scraper as standalone script."""
    import argparse
    
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Article Content Scraper')
    parser.add_argument('--limit', type=int, help='Limit number of articles to process')
    parser.add_argument('--test', action='store_true', help='Test with single article')
    
    args = parser.parse_args()
    
    scraper = ArticleScraper()
    
    if args.test:
        # Test mode - process just one article
        articles = scraper.get_unprocessed_articles(limit=1)
        if articles:
            result = scraper.scrape_article(articles[0])
            print("\nTest Scraping Result:")
            print("=" * 50)
            print(f"Title: {result['title'][:80]}...")
            print(f"Success: {result['success']}")
            print(f"Content Length: {result['content_length']}")
            print(f"Images Found: {len(result['images'])}")
            if result['error']:
                print(f"Error: {result['error']}")
            print("\nFirst 500 chars of content:")
            print(result['content'][:500])
    else:
        # Normal batch processing
        stats = scraper.process_batch(limit=args.limit)
        
        # Print summary
        print("\nArticle Scraping Summary:")
        print("=" * 50)
        print(f"Articles attempted: {stats['articles_attempted']}")
        print(f"Successful scrapes: {stats['articles_success']}")
        print(f"Failed scrapes: {stats['articles_failed']}")
        print(f"RSS fallbacks used: {stats['content_fallback']}")
        if stats.get('avg_content_length'):
            print(f"Average content length: {stats['avg_content_length']} chars")
        print(f"Processing time: {stats.get('processing_time_seconds', 0)} seconds")
        
        if stats['errors']:
            unique_errors = list(set(stats['errors']))
            print(f"\nUnique errors: {len(unique_errors)}")
            for error in unique_errors[:5]:
                print(f"  - {error}")


if __name__ == "__main__":
    main()