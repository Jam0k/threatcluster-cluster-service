#!/usr/bin/env python3
"""
Test MISP Fetcher

Tests the MISP fetcher functionality.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from src.config.settings import settings
from src.misp.misp_fetcher import MISPFetcher
from src.misp.misp_parser import MISPParser


def test_misp_parser():
    """Test the MISP parser with sample data."""
    print("Testing MISP Parser...")
    
    parser = MISPParser()
    
    # Sample threat actor data
    sample_json = '''
    {
        "values": [
            {
                "description": "Test APT group for validation",
                "meta": {
                    "attribution-confidence": "75",
                    "country": "XX",
                    "refs": [
                        "https://example.com/report1",
                        "https://example.com/report2"
                    ],
                    "synonyms": [
                        "TEST-APT",
                        "TestGroup"
                    ],
                    "targeted-sector": [
                        "Technology",
                        "Finance"
                    ]
                },
                "uuid": "test-uuid-123",
                "value": "TEST-APT-GROUP"
            }
        ]
    }
    '''
    
    entities = parser.parse_threat_actors(sample_json)
    
    if entities:
        entity = entities[0]
        print(f"✓ Parser working: Found {entity['entities_name']}")
        print(f"  Category: {entity['entities_category']}")
        print(f"  Importance: {entity['entities_importance_weight']}")
        print(f"  Has JSON: {'entities_json' in entity}")
    else:
        print("✗ Parser failed: No entities parsed")
    
    return len(entities) > 0


def test_misp_fetcher():
    """Test the MISP fetcher configuration."""
    print("\nTesting MISP Fetcher Configuration...")
    
    fetcher = MISPFetcher()
    
    # Check configuration
    feeds = fetcher.config.get('feeds', [])
    print(f"✓ Configuration loaded: {len(feeds)} feeds configured")
    
    for feed in feeds:
        if feed.get('active'):
            print(f"  - {feed['name']}: {feed['url'][:60]}...")
    
    return True


def test_database_connection():
    """Test database connectivity."""
    print("\nTesting Database Connection...")
    
    try:
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        # Check entities table
        cursor.execute("""
            SELECT COUNT(*) 
            FROM cluster_data.entities 
            WHERE entities_source = 'misp'
        """)
        
        count = cursor.fetchone()[0]
        print(f"✓ Database connected: {count} MISP entities in database")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}")
        return False


def test_fetch_sample():
    """Test fetching a small sample."""
    print("\nTesting MISP Fetch (Sample)...")
    
    try:
        fetcher = MISPFetcher()
        
        # Test fetching content from URL
        test_url = "https://raw.githubusercontent.com/MISP/misp-galaxy/refs/heads/main/clusters/threat-actor.json"
        print(f"Fetching from: {test_url[:60]}...")
        
        content = fetcher._fetch_feed_content(test_url)
        
        if content:
            print(f"✓ Content fetched: {len(content)} bytes")
            
            # Try to parse it
            parser = MISPParser()
            entities = parser.parse_threat_actors(content)
            print(f"✓ Parsed {len(entities)} threat actors")
            
            if entities:
                # Show first few
                print("\nFirst 5 threat actors:")
                for entity in entities[:5]:
                    print(f"  - {entity['entities_name']} (weight: {entity['entities_importance_weight']})")
            
            return True
        else:
            print("✗ Failed to fetch content")
            return False
            
    except Exception as e:
        print(f"✗ Fetch error: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("MISP Fetcher Test Suite")
    print("=" * 60)
    
    tests = [
        test_misp_parser,
        test_database_connection,
        test_misp_fetcher,
        test_fetch_sample
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test failed with error: {e}")
    
    print("\n" + "=" * 60)
    print(f"Tests completed: {passed}/{len(tests)} passed")
    print("=" * 60)
    
    if passed == len(tests):
        print("\n✓ All tests passed! MISP fetcher is ready to use.")
        print("\nTo run the fetcher:")
        print("  python -m src.misp.misp_scheduler --once")
        print("\nTo run as daemon:")
        print("  python -m src.misp.misp_scheduler")
    else:
        print("\n✗ Some tests failed. Please check the errors above.")
    
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())