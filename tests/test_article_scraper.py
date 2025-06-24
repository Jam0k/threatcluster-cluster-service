#!/usr/bin/env python3
"""
Test script for Article Scraper

Tests the web scraping functionality with real articles from the database.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m tests.test_article_scraper
"""
import sys
import json
from src.scraper.article_scraper import ArticleScraper
from src.config.settings import settings
import psycopg2
import psycopg2.extras


def test_database_connection():
    """Test database connectivity and find scraped articles."""
    print("\nTesting database connection...")
    print("=" * 50)
    
    try:
        scraper = ArticleScraper()
        articles = scraper.get_unprocessed_articles(limit=5)
        
        print(f"✓ Successfully connected to database")
        print(f"✓ Found {len(articles)} unprocessed articles")
        
        if articles:
            print("\nFirst unprocessed article:")
            article = articles[0]
            xml_data = article['rss_feeds_raw_xml']
            print(f"  Title: {xml_data.get('title', 'N/A')[:60]}...")
            print(f"  URL: {xml_data.get('link', 'N/A')}")
            print(f"  Published: {article['rss_feeds_raw_published_date']}")
        
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


def test_content_extraction():
    """Test content extraction with sample HTML."""
    print("\nTesting content extraction...")
    print("=" * 50)
    
    scraper = ArticleScraper()
    
    # Sample HTML for testing
    sample_html = """
    <html>
    <head><title>Test Article</title></head>
    <body>
        <nav>Navigation menu</nav>
        <article>
            <h1>Main Article Title</h1>
            <p>This is the first paragraph of the article content.</p>
            <p>This is the second paragraph with more details.</p>
            <img src="/images/test.jpg" alt="Test image">
            <p>Final paragraph of the article.</p>
        </article>
        <footer>Footer content</footer>
    </body>
    </html>
    """
    
    content, images = scraper.extract_content(sample_html, "https://example.com/article")
    
    print(f"✓ Extracted content length: {len(content)} chars")
    print(f"✓ Images found: {len(images)}")
    print(f"\nExtracted content preview:")
    print(content[:200])
    
    if images:
        print(f"\nExtracted images:")
        for img in images:
            print(f"  - {img}")
    
    return len(content) > 0


def test_single_article_scraping():
    """Test scraping a single real article."""
    print("\nTesting single article scraping...")
    print("=" * 50)
    
    try:
        scraper = ArticleScraper()
        articles = scraper.get_unprocessed_articles(limit=1)
        
        if not articles:
            print("⚠ No unprocessed articles found to test")
            return False
        
        article = articles[0]
        xml_data = article['rss_feeds_raw_xml']
        
        print(f"Testing article: {xml_data.get('title', 'N/A')[:60]}...")
        print(f"URL: {xml_data.get('link', 'N/A')}")
        
        # Scrape the article
        result = scraper.scrape_article(article)
        
        if result:
            print(f"\n✓ Scraping completed")
            print(f"  Success: {result['success']}")
            print(f"  Content length: {result['content_length']} chars")
            print(f"  Images found: {len(result['images'])}")
            
            if result['error']:
                print(f"  Error: {result['error']}")
            
            if result['content']:
                print(f"\nContent preview (first 300 chars):")
                print(f"  {result['content'][:300]}...")
            
            return True
        else:
            print("✗ Scraping returned no result")
            return False
            
    except Exception as e:
        print(f"✗ Scraping test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rate_limiting():
    """Test rate limiting functionality."""
    print("\nTesting rate limiting...")
    print("=" * 50)
    
    import time
    
    scraper = ArticleScraper()
    
    # Test rate limiting for same domain
    domain = "example.com"
    
    print(f"Testing {scraper.rate_limit_delay}s delay for domain: {domain}")
    
    start_time = time.time()
    scraper.enforce_rate_limit(domain)
    first_delay = time.time() - start_time
    print(f"✓ First access: {first_delay:.2f}s (no delay expected)")
    
    # Second access should be delayed
    start_time = time.time()
    scraper.enforce_rate_limit(domain)
    second_delay = time.time() - start_time
    print(f"✓ Second access: {second_delay:.2f}s (delay expected)")
    
    # Different domain should not be delayed
    start_time = time.time()
    scraper.enforce_rate_limit("different.com")
    third_delay = time.time() - start_time
    print(f"✓ Different domain: {third_delay:.2f}s (no delay expected)")
    
    return second_delay >= scraper.rate_limit_delay - 0.1


def test_batch_processing():
    """Test batch processing of multiple articles."""
    print("\nTesting batch processing...")
    print("=" * 50)
    
    try:
        scraper = ArticleScraper()
        
        # Process a small batch
        print("Processing batch of up to 3 articles...")
        stats = scraper.process_batch(limit=3)
        
        print(f"\n✓ Batch processing completed")
        print(f"  Articles attempted: {stats['articles_attempted']}")
        print(f"  Successful: {stats['articles_success']}")
        print(f"  Failed: {stats['articles_failed']}")
        print(f"  RSS fallbacks: {stats['content_fallback']}")
        
        if stats.get('avg_content_length'):
            print(f"  Average content length: {stats['avg_content_length']} chars")
        
        print(f"  Processing time: {stats.get('processing_time_seconds', 0):.2f}s")
        
        return stats['articles_attempted'] > 0
        
    except Exception as e:
        print(f"✗ Batch processing failed: {e}")
        return False


def check_clean_table():
    """Check if articles were stored in the clean table."""
    print("\nChecking clean table storage...")
    print("=" * 50)
    
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM cluster_data.rss_feeds_clean
        """)
        total_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                rss_feeds_clean_title,
                rss_feeds_clean_content,
                rss_feeds_clean_images
            FROM cluster_data.rss_feeds_clean
            LIMIT 1
        """)
        
        sample = cursor.fetchone()
        
        print(f"✓ Total articles in clean table: {total_count}")
        
        if sample:
            # psycopg2 automatically converts JSONB to dict, no need for json.loads
            title_data = sample[0]
            content_data = sample[1]
            
            print(f"\nSample stored article:")
            print(f"  Title: {title_data.get('title', 'N/A')[:60]}...")
            print(f"  Content length: {content_data.get('content_length', 0)} chars")
            print(f"  Scraping success: {content_data.get('success', False)}")
            
            if sample[2]:
                images = sample[2]  # Already a list
                print(f"  Images: {len(images)}")
        
        return total_count > 0
        
    except Exception as e:
        print(f"✗ Error checking clean table: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def main():
    """Run all tests."""
    print("Article Scraper Test Suite")
    print("=" * 70)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Content Extraction", test_content_extraction),
        ("Rate Limiting", test_rate_limiting),
        ("Single Article Scraping", test_single_article_scraping),
        ("Batch Processing", test_batch_processing),
        ("Clean Table Storage", check_clean_table),
    ]
    
    passed = 0
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"\n✗ {test_name} test failed")
        except Exception as e:
            print(f"\n✗ {test_name} test error: {e}")
    
    print("\n" + "=" * 70)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())