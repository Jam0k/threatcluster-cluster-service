#!/usr/bin/env python3
"""
Test script for RSS Feed Fetcher

Tests the RSS feed fetching functionality with a small subset of feeds.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m tests.test_rss_fetcher
"""
import sys
from src.feeds.rss_fetcher import RSSFeedFetcher
from src.config.settings import settings


def test_single_feed():
    """Test fetching a single RSS feed."""
    print("Testing single RSS feed fetch...")
    print("=" * 50)
    
    fetcher = RSSFeedFetcher()
    
    # Test with a known good feed
    test_feed = {
        'rss_feeds_id': 1,
        'rss_feeds_url': 'https://krebsonsecurity.com/feed/',
        'rss_feeds_name': 'Krebs on Security',
        'rss_feeds_category': 'cybersecurity',
        'rss_feeds_credibility': 90
    }
    
    stats = fetcher.process_feed(test_feed)
    
    print(f"Feed: {test_feed['rss_feeds_name']}")
    print(f"Articles fetched: {stats['fetched']}")
    print(f"Articles filtered: {stats['filtered']}")
    print(f"Articles stored: {stats['stored']}")
    print(f"Duplicates skipped: {stats['duplicates']}")
    
    return stats['stored'] > 0


def test_security_filtering():
    """Test security keyword filtering."""
    print("\nTesting security keyword filtering...")
    print("=" * 50)
    
    fetcher = RSSFeedFetcher()
    
    # Test cases
    test_cases = [
        ("Microsoft patches critical vulnerability in Windows", True),
        ("Apple announces new iPhone features", False),
        ("Ransomware attack hits major hospital", True),
        ("Stock market reaches new highs", False),
        ("Zero-day exploit discovered in Chrome", True),
        ("New restaurant opens downtown", False),
        ("Cybersecurity firm reports data breach", True),
        ("Weather forecast for the weekend", False),
    ]
    
    passed = 0
    for text, expected in test_cases:
        result = fetcher.is_security_relevant(text, "")
        status = "✓" if result == expected else "✗"
        print(f"{status} '{text[:50]}...' -> {result} (expected: {expected})")
        if result == expected:
            passed += 1
    
    print(f"\nPassed {passed}/{len(test_cases)} tests")
    return passed == len(test_cases)


def test_database_connection():
    """Test database connectivity."""
    print("\nTesting database connection...")
    print("=" * 50)
    
    try:
        fetcher = RSSFeedFetcher()
        feeds = fetcher.fetch_active_feeds()
        print(f"✓ Successfully connected to database")
        print(f"✓ Found {len(feeds)} active feeds")
        
        # Show first 5 feeds
        print("\nFirst 5 active feeds:")
        for feed in feeds[:5]:
            print(f"  - {feed['rss_feeds_name']} (credibility: {feed['rss_feeds_credibility']})")
        
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


def test_full_process():
    """Test full RSS processing with limited feeds."""
    print("\nTesting full RSS processing (limited to 3 feeds)...")
    print("=" * 50)
    
    fetcher = RSSFeedFetcher()
    
    # Get only first 3 feeds for testing
    feeds = fetcher.fetch_active_feeds()[:3]
    
    if not feeds:
        print("✗ No active feeds found in database")
        return False
    
    # Process limited feeds
    total_stored = 0
    for feed in feeds:
        print(f"\nProcessing: {feed['rss_feeds_name']}")
        stats = fetcher.process_feed(feed)
        total_stored += stats['stored']
        print(f"  Stored: {stats['stored']} articles")
    
    print(f"\n✓ Total articles stored: {total_stored}")
    return total_stored > 0


def main():
    """Run all tests."""
    print("RSS Feed Fetcher Test Suite")
    print("=" * 70)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Security Filtering", test_security_filtering),
        ("Single Feed Fetch", test_single_feed),
        ("Full Process", test_full_process),
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