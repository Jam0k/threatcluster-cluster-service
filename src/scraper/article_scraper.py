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
import html
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
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
    MAX_CONTENT_LENGTH = 10000
    
    # Image file extensions
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp'}
    
    # Navigation and UI patterns to remove
    NAVIGATION_PATTERNS = [
        r'\b(Home|About|Contact|Menu|Navigation|Search)\b',
        r'\b(Subscribe|Newsletter|Sign up|Email updates?)\b',
        r'\b(Share|Tweet|Facebook|LinkedIn|Pinterest|Reddit)\b',
        r'\b(Previous|Next|Continue reading|Read more)\b',
        r'\b(Page \d+ of \d+|Pages?:?\s*\d+)\b',
        r'\b(Comments?|Leave a comment|Reply|Discuss)\b',
        r'\b(Advertisement|Sponsored|Promoted)\b',
        r'\b(Cookie policy|Privacy policy|Terms of service)\b',
        r'\b(Follow us|Connect with us|Join us)\b',
    ]
    
    # Footer patterns to remove
    FOOTER_PATTERNS = [
        r'The post .+ appeared first on .+',
        r'Source:?\s*https?://\S+',
        r'Originally published at .+',
        r'Copyright ©?\s*\d{4}',
        r'All rights reserved',
        r'Read the full article at .+',
        r'©\s*\d{4}\s*.+',
        r'Filed under:?\s*.+',
        r'Tagged with:?\s*.+',
        r'\d+ minute read',
        r'Reading time:?\s*\d+\s*min',
    ]
    
    # Security terms to preserve (case-insensitive)
    SECURITY_PRESERVE_PATTERNS = [
        # CVE patterns
        r'CVE-\d{4}-\d{4,}',
        # IP addresses
        r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
        # File hashes (MD5, SHA1, SHA256)
        r'\b[a-fA-F0-9]{32}\b',
        r'\b[a-fA-F0-9]{40}\b', 
        r'\b[a-fA-F0-9]{64}\b',
        # Domain patterns for security context
        r'(?:malicious|phishing|c2|command.?and.?control|malware|apt)\s+(?:domain|site|server)s?\s*:?\s*([a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?\.[a-zA-Z]{2,})',
        # Email addresses
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        # File paths
        r'[A-Za-z]:\\[\w\s\\.-]+|/[\w\s/.-]+',
        # Registry keys
        r'HKEY_[A-Z_]+(?:\\[^\\]+)+',
        # Port numbers
        r':\d{1,5}\b',
    ]
    
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
        
        # Compile regex patterns for efficiency
        self.navigation_regex = re.compile(
            '|'.join(self.NAVIGATION_PATTERNS), 
            re.IGNORECASE | re.MULTILINE
        )
        self.footer_regex = re.compile(
            '|'.join(self.FOOTER_PATTERNS),
            re.IGNORECASE | re.MULTILINE
        )
        
        # Statistics tracking
        self.stats = {
            'articles_attempted': 0,
            'articles_success': 0,
            'articles_failed': 0,
            'content_fallback': 0,
            'total_content_length': 0,
            'total_chars_removed': 0,
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
    
    def extract_banner_image(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract the main banner/hero image from the article."""
        # 1. First try Open Graph and Twitter Card meta tags - these are most reliable
        meta_image = None
        
        # Open Graph image (most standard)
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            meta_image = urljoin(base_url, og_image['content'].strip())
            if self.is_valid_banner_image(meta_image):
                logger.info("Found Open Graph image", url=base_url, image=meta_image)
                return meta_image
        
        # Twitter Card image (fallback)
        twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
        if twitter_image and twitter_image.get('content'):
            meta_image = urljoin(base_url, twitter_image['content'].strip())
            if self.is_valid_banner_image(meta_image):
                logger.info("Found Twitter Card image", url=base_url, image=meta_image)
                return meta_image
        
        # Schema.org image
        schema_image = soup.find('meta', attrs={'itemprop': 'image'})
        if schema_image and schema_image.get('content'):
            meta_image = urljoin(base_url, schema_image['content'].strip())
            if self.is_valid_banner_image(meta_image):
                logger.info("Found Schema.org image", url=base_url, image=meta_image)
                return meta_image
        
        # 2. Look for images with specific banner/hero attributes
        # These are the most likely to be the main article image
        banner_selectors = [
            # Direct hero/banner image selectors
            'img.featured-image',
            'img.hero-image',
            'img.banner-image',
            'img.post-image',
            'img.article-image',
            'img.main-image',
            'img.wp-post-image',  # WordPress featured image
            'img[itemprop="image"]',  # Schema.org markup
            'img.attachment-post-thumbnail',  # WordPress
            'img.size-full',  # Often used for main images
            # Images in hero/banner containers
            '.hero img',
            '.banner img',
            '.featured-image img',
            '.post-thumbnail img',
            '.article-hero img',
            '.entry-image img',
            '.article-header img',
            '.post-header img',
            # WordPress patterns
            '.wp-block-image img',
            '.single-featured-image img',
            '.wp-post-image img',
            # Common news site patterns
            '.lead-image img',
            '.primary-image img',
            '.story-image img',
            '.article-lead-image img',
            # Figure tags often contain main images
            'figure.featured img',
            'figure.main-image img',
            'figure.post-thumbnail img'
        ]
        
        for selector in banner_selectors:
            banner_img = soup.select_one(selector)
            if banner_img:
                img_url = self._get_image_url(banner_img, base_url)
                if img_url and self.is_valid_banner_image(img_url, check_context=True, img_tag=banner_img):
                    logger.info("Found banner image by selector", url=base_url, selector=selector, image=img_url)
                    return img_url
        
        # 3. Look in article header ONLY (not the entire content)
        article_headers = soup.find_all(['header'], class_=re.compile(r'(entry|post|article)', re.I))
        for header in article_headers[:3]:
            # Only look for direct child images or images in figure tags
            for img_container in header.find_all(['img', 'figure'], recursive=True)[:5]:
                if img_container.name == 'figure':
                    img = img_container.find('img')
                else:
                    img = img_container
                
                if img:
                    img_url = self._get_image_url(img, base_url)
                    if img_url and self.is_valid_banner_image(img_url, check_context=True, img_tag=img):
                        logger.info("Found banner image in article header", url=base_url, image=img_url)
                        return img_url
        
        # 4. Check for images immediately after the title
        # Find the article title
        title_tags = soup.find_all(['h1', 'h2'], class_=re.compile(r'(title|headline|entry-title)', re.I))
        for title in title_tags[:2]:
            # Look for image in the next few siblings
            next_sibling = title.find_next_sibling()
            for _ in range(5):  # Check next 5 siblings
                if not next_sibling:
                    break
                    
                img = None
                if next_sibling.name == 'img':
                    img = next_sibling
                elif next_sibling.name in ['div', 'figure', 'p']:
                    img = next_sibling.find('img')
                
                if img:
                    img_url = self._get_image_url(img, base_url)
                    if img_url and self.is_valid_banner_image(img_url, check_context=True, img_tag=img):
                        logger.info("Found banner image after title", url=base_url, image=img_url)
                        return img_url
                
                next_sibling = next_sibling.find_next_sibling()
        
        # 5. As a last resort, look for the first LARGE image in the main content
        # But be very selective - only in article content areas
        article_content = soup.find(['article', 'main', 'div'], 
                                  class_=re.compile(r'^(post-content|article-content|entry-content|content-body)$', re.I))
        
        if article_content:
            # Only check first 3 images in actual content
            for img in article_content.find_all('img')[:3]:
                img_url = self._get_image_url(img, base_url)
                if img_url and self.is_valid_banner_image(img_url, check_context=True, img_tag=img):
                    # Extra validation for content images - must be large
                    width = img.get('width')
                    height = img.get('height')
                    if width and height:
                        try:
                            w = int(str(width).replace('px', ''))
                            h = int(str(height).replace('px', ''))
                            if w >= 600 and h >= 400:  # Must be quite large
                                logger.info("Found large banner image in content", url=base_url, image=img_url)
                                return img_url
                        except:
                            pass
        
        logger.info("No suitable banner image found", url=base_url)
        return None
    
    def _get_image_url(self, img_tag, base_url: str) -> Optional[str]:
        """Extract image URL from img tag, handling lazy loading."""
        # Try regular src first
        src = img_tag.get('src', '').strip()
        if src and not src.startswith('data:'):
            return urljoin(base_url, src)
        
        # Try data-src for lazy loading
        data_src = img_tag.get('data-src', '').strip()
        if data_src:
            return urljoin(base_url, data_src)
        
        # Try srcset for responsive images
        srcset = img_tag.get('srcset', '').strip()
        if srcset:
            # Get the largest image from srcset
            parts = srcset.split(',')
            if parts:
                # Take the last one (usually largest)
                last_part = parts[-1].strip().split(' ')[0]
                return urljoin(base_url, last_part)
        
        return None
    
    def is_valid_banner_image(self, url: str, check_context: bool = False, img_tag=None) -> bool:
        """Check if URL is suitable for a banner image."""
        if not self.is_valid_image_url(url):
            return False
        
        # Exclude images with certain patterns in URL
        # Be VERY strict about what we exclude
        excluded_url_patterns = [
            'icon', 'logo', 'avatar', 'profile', 'author',
            'button', 'arrow', 'emoji', 'badge', 'symbol',
            '1x1', '2x2', 'pixel', 'tracking', 'analytics',
            'comment', 'user', 'thumbnail', 'thumb',
            'social', 'share', 'facebook', 'twitter', 'linkedin', 'pinterest',
            'sidebar', 'widget', 'advertisement', 'sponsor', 'ad-',
            'team', 'staff', 'contributor', 'writer', 'editor',
            'headshot', 'portrait', 'bio', 'mugshot',
            'footer', 'header-logo', 'nav-', 'menu-',
            'related', 'recommended', 'more-stories',
            'newsletter', 'subscribe', 'signup',
            'partner', 'client-logo', 'brand-'
        ]
        
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in excluded_url_patterns):
            return False
        
        # If we have the img tag, check its attributes
        if check_context and img_tag:
            # Check class and id
            img_class = str(img_tag.get('class', '')).lower()
            img_id = str(img_tag.get('id', '')).lower()
            img_alt = str(img_tag.get('alt', '')).lower()
            
            # Also check parent element classes
            parent_class = ''
            parent = img_tag.parent
            if parent:
                parent_class = str(parent.get('class', '')).lower()
                # Check grandparent too
                grandparent = parent.parent
                if grandparent:
                    parent_class += ' ' + str(grandparent.get('class', '')).lower()
            
            combined_attrs = f"{img_class} {img_id} {img_alt} {parent_class}"
            
            # Exclude based on attributes - be very strict
            excluded_attr_patterns = excluded_url_patterns + [
                'byline', 'meta', 'info', 'details',
                'related', 'recommended', 'popular', 'trending',
                'aside', 'secondary', 'supplemental',
                'advertisement', 'promo', 'sponsor',
                'share-button', 'social-icon',
                'author-image', 'contributor-photo',
                'video-thumbnail', 'play-button'
            ]
            
            if any(pattern in combined_attrs for pattern in excluded_attr_patterns):
                return False
            
            # Positive signals - if these are present, it's likely a banner
            positive_patterns = [
                'featured', 'hero', 'banner', 'lead',
                'main-image', 'primary', 'cover',
                'story-image', 'article-image'
            ]
            
            if any(pattern in combined_attrs for pattern in positive_patterns):
                # Even with positive signals, still check dimensions
                pass  # Continue to dimension check below
            
            # Check dimensions if available
            width = img_tag.get('width')
            height = img_tag.get('height')
            
            if width and height:
                try:
                    w = int(str(width).replace('px', ''))
                    h = int(str(height).replace('px', ''))
                    
                    # Exclude small images - be strict about banner size
                    if w < 400 or h < 250:
                        return False
                    
                    # Banner images typically have landscape orientation
                    aspect_ratio = w / h if h > 0 else 0
                    
                    # Exclude square images unless they're very large
                    if 0.9 <= aspect_ratio <= 1.1 and w < 800:
                        return False
                    
                    # Exclude tall images (likely sidebar or info graphics)
                    if aspect_ratio < 0.8:
                        return False
                    
                    # Prefer landscape images (typical for banners)
                    if aspect_ratio >= 1.3:
                        # This is good - likely a banner
                        return True
                        
                except ValueError:
                    pass
        
        return True
    
    def extract_image_from_rss(self, xml_data: Dict[str, Any]) -> Optional[str]:
        """Extract image URL from RSS feed data before scraping the page."""
        try:
            # 1. Check for media:content (most reliable for RSS images)
            media_content = xml_data.get('media_content', [])
            if isinstance(media_content, list):
                for media in media_content:
                    if isinstance(media, dict) and media.get('type', '').startswith('image/'):
                        image_url = media.get('url')
                        if image_url and self.is_valid_banner_image(image_url):
                            logger.info("Found image in media:content", image=image_url)
                            return image_url
            
            # 2. Check media:thumbnail
            media_thumbnail = xml_data.get('media_thumbnail', [])
            if isinstance(media_thumbnail, list) and media_thumbnail:
                for thumb in media_thumbnail:
                    if isinstance(thumb, dict):
                        image_url = thumb.get('url')
                        if image_url and self.is_valid_banner_image(image_url):
                            logger.info("Found image in media:thumbnail", image=image_url)
                            return image_url
            
            # 3. Check enclosures (common for podcast/media feeds)
            enclosures = xml_data.get('enclosures', [])
            if isinstance(enclosures, list):
                for enclosure in enclosures:
                    if isinstance(enclosure, dict) and enclosure.get('type', '').startswith('image/'):
                        image_url = enclosure.get('href') or enclosure.get('url')
                        if image_url and self.is_valid_banner_image(image_url):
                            logger.info("Found image in enclosure", image=image_url)
                            return image_url
            
            # 4. Check for images in RSS content/summary
            content = xml_data.get('content', '')
            if not content:
                content = xml_data.get('summary', '')
            
            if content and isinstance(content, str):
                # Parse the RSS content HTML
                soup = BeautifulSoup(content, 'html.parser')
                img = soup.find('img')
                if img and img.get('src'):
                    src = img['src']
                    # Make URL absolute if needed
                    if not src.startswith('http'):
                        link = xml_data.get('link', '')
                        if link:
                            src = urljoin(link, src)
                    
                    if self.is_valid_banner_image(src):
                        logger.info("Found image in RSS content", image=src)
                        return src
            
            return None
            
        except Exception as e:
            logger.debug("Error extracting image from RSS", error=str(e))
            return None
    
    def extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract image URLs from the page - now returns only banner image."""
        banner_image = self.extract_banner_image(soup, base_url)
        
        # Return as list for backward compatibility
        if banner_image:
            return [banner_image]
        else:
            return []
    
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
    
    def clean_html_entities(self, text: str) -> str:
        """Decode HTML entities and clean malformed HTML."""
        if not text:
            return ""
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove any remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Clean up HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        
        # Remove CSS and JavaScript
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        return text
    
    def clean_whitespace(self, text: str) -> str:
        """Normalize whitespace and formatting."""
        if not text:
            return ""
        
        # Replace various whitespace with regular spaces
        text = re.sub(r'[\t\r\f\v]+', ' ', text)
        
        # Replace multiple spaces with single space
        text = re.sub(r' {2,}', ' ', text)
        
        # Replace multiple newlines with double newline
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove spaces at line beginnings and ends
        text = '\n'.join(line.strip() for line in text.split('\n'))
        
        # Remove empty lines
        lines = [line for line in text.split('\n') if line.strip()]
        text = '\n'.join(lines)
        
        return text.strip()
    
    def remove_navigation_elements(self, text: str) -> str:
        """Remove navigation and UI elements."""
        if not text:
            return ""
        
        # Remove navigation patterns
        text = self.navigation_regex.sub(' ', text)
        
        # Remove isolated navigation words at line starts/ends
        lines = text.split('\n')
        cleaned_lines = []
        
        nav_words = {'Home', 'About', 'Contact', 'Menu', 'Search', 'Share', 
                     'Tweet', 'Facebook', 'LinkedIn', 'Email', 'Subscribe'}
        
        for line in lines:
            words = line.strip().split()
            if len(words) <= 3 and any(word in nav_words for word in words):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def remove_footers(self, text: str) -> str:
        """Remove footer patterns and metadata."""
        if not text:
            return ""
        
        # Remove footer patterns
        text = self.footer_regex.sub('', text)
        
        # Remove author bylines (unless they contain security info)
        if not re.search(r'(CVE|vulnerability|exploit|malware)', text[:200], re.IGNORECASE):
            text = re.sub(r'^By\s+.+?\n', '', text, flags=re.MULTILINE)
            text = re.sub(r'^Author:?\s*.+?\n', '', text, flags=re.MULTILINE | re.IGNORECASE)
        
        return text
    
    def remove_urls(self, text: str) -> str:
        """Remove or clean URLs while preserving domain names in security context."""
        if not text:
            return ""
        
        # First, preserve security-relevant domains
        preserved_domains = []
        
        # Find and temporarily replace security domains
        domain_pattern = r'(?:(?:malicious|phishing|c2|command.?and.?control|malware|apt)\s+(?:domain|site|server)s?\s*:?\s*)([a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?\.[a-zA-Z]{2,})'
        for match in re.finditer(domain_pattern, text, re.IGNORECASE):
            placeholder = f"__DOMAIN_{len(preserved_domains)}__"
            preserved_domains.append(match.group(1))
            text = text.replace(match.group(0), f"{match.group(0).split()[0]} {placeholder}")
        
        # Remove general URLs
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        text = re.sub(r'\[http[^\]]+\]', '', text)  # Remove markdown URLs
        
        # Restore preserved domains
        for i, domain in enumerate(preserved_domains):
            text = text.replace(f"__DOMAIN_{i}__", domain)
        
        return text
    
    def clean_content(self, content: str) -> Tuple[str, Dict[str, Any]]:
        """Apply all cleaning operations to content."""
        original_length = len(content)
        
        # Apply cleaning operations in sequence
        content = self.clean_html_entities(content)
        content = self.remove_urls(content)
        content = self.remove_navigation_elements(content)
        content = self.remove_footers(content)
        content = self.clean_whitespace(content)
        
        # Truncate if too long
        if len(content) > self.MAX_CONTENT_LENGTH:
            content = content[:self.MAX_CONTENT_LENGTH] + "..."
        
        # Calculate cleaning metrics
        cleaned_length = len(content)
        chars_removed = original_length - cleaned_length
        reduction_percent = (chars_removed / original_length * 100) if original_length > 0 else 0
        
        cleaning_metadata = {
            'original_length': original_length,
            'cleaned_length': cleaned_length,
            'chars_removed': chars_removed,
            'reduction_percent': round(reduction_percent, 2)
        }
        
        return content, cleaning_metadata
    
    def clean_title(self, title: str) -> str:
        """Clean article title."""
        if not title:
            return ""
        
        # Remove HTML entities
        title = html.unescape(title)
        
        # Remove any HTML tags
        title = re.sub(r'<[^>]+>', '', title)
        
        # Remove site names often appended to titles (only for known patterns)
        # This regex specifically targets common news site suffixes
        site_patterns = [
            r'\s*\|\s*SecurityWeek\.Com$',
            r'\s*\|\s*BleepingComputer$',
            r'\s*\|\s*The Hacker News$',
            r'\s*\|\s*Dark Reading$',
            r'\s*\|\s*ZDNet$',
            r'\s*\|\s*Threatpost$',
            r'\s*\|\s*CyberScoop$',
            r'\s*\|\s*Krebs on Security$',
            r'\s*\|\s*Ars Technica$',
            r'\s*\|\s*The Register$',
            r'\s*\|\s*Wired$',
            r'\s*\|\s*TechCrunch$',
            r'\s*-\s*SecurityWeek\.Com$',
            r'\s*-\s*BleepingComputer$',
            r'\s*–\s*[A-Za-z\s]+\s*\|\s*WIRED$',  # For patterns like "Title – Category | WIRED"
        ]
        
        for pattern in site_patterns:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)
        
        # Clean whitespace
        title = ' '.join(title.split())
        
        return title.strip()
    
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
        
        # First, try to extract image from RSS data
        rss_image = self.extract_image_from_rss(xml_data)
        
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
                scraped_content, page_images = self.extract_content(response.text, url)
                
                # Use RSS image if found, otherwise use scraped images
                if rss_image:
                    image_urls = [rss_image]
                    logger.info("using_rss_image", url=url, image=rss_image)
                else:
                    image_urls = page_images
                    if image_urls:
                        logger.info("using_scraped_image", url=url, image=image_urls[0])
                
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
            # If we have RSS image but no scraped images, keep the RSS image
            if rss_image and not image_urls:
                image_urls = [rss_image]
        
        # Clean the content
        cleaned_content, cleaning_metadata = self.clean_content(scraped_content)
        cleaned_title = self.clean_title(title)
        
        # Update stats
        self.stats['total_chars_removed'] += cleaning_metadata['chars_removed']
        
        # Validate cleaned content length
        if len(cleaned_content) < self.MIN_CONTENT_LENGTH:
            if not error_message:
                error_message = "Content too short after cleaning"
            success = False
        
        # Prepare result
        result = {
            'raw_id': raw_id,
            'feed_id': feed_id,
            'title': cleaned_title,
            'content': cleaned_content,
            'images': image_urls,
            'success': success,
            'error': error_message,
            'content_length': len(cleaned_content),
            'cleaning_metadata': cleaning_metadata,
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
                        'cleaning_metadata': result['cleaning_metadata'],
                        'scraped_at': result['scraped_at'].isoformat()
                    }),
                    json.dumps(result['images']) if result['images'] else None,
                    result['success']  # Set to TRUE if scraping/cleaning was successful
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
            self.stats['avg_chars_removed'] = int(
                self.stats['total_chars_removed'] / self.stats['articles_success']
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
            print(f"Original Title: {articles[0]['rss_feeds_raw_xml'].get('title', '')[:80]}...")
            print(f"Cleaned Title: {result['title'][:80]}...")
            print(f"Success: {result['success']}")
            print(f"Final Content Length: {result['content_length']}")
            print(f"Cleaning Reduction: {result['cleaning_metadata']['reduction_percent']}%")
            print(f"Characters Removed: {result['cleaning_metadata']['chars_removed']}")
            print(f"Images Found: {len(result['images'])}")
            if result['error']:
                print(f"Error: {result['error']}")
            print("\nFirst 500 chars of cleaned content:")
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
        if stats.get('avg_chars_removed'):
            print(f"Average chars removed by cleaning: {stats['avg_chars_removed']} chars")
        print(f"Processing time: {stats.get('processing_time_seconds', 0)} seconds")
        
        if stats['errors']:
            unique_errors = list(set(stats['errors']))
            print(f"\nUnique errors: {len(unique_errors)}")
            for error in unique_errors[:5]:
                print(f"  - {error}")


if __name__ == "__main__":
    main()