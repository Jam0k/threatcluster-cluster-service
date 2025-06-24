#!/usr/bin/env python3
"""
Test script for Entity Extractor

Tests the entity extraction functionality with various test cases.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m tests.test_entity_extractor
"""
import sys
import json
from src.entity_extraction.entity_extractor import EntityExtractor
from src.entity_extraction.entity_validator import EntityValidator
from src.config.settings import settings
import psycopg2
import psycopg2.extras


def test_regex_extraction():
    """Test regex pattern extraction."""
    print("\nTesting regex pattern extraction...")
    print("=" * 50)
    
    extractor = EntityExtractor()
    
    test_cases = [
        {
            'text': "The vulnerability CVE-2023-12345 was discovered in the system.",
            'expected': {'cve': ['CVE-2023-12345']}
        },
        {
            'text': "The malware connects to 185.220.101.45 and downloads payload from evil-malware.com",
            'expected': {'ip_address': ['185.220.101.45'], 'domain': ['evil-malware.com']}
        },
        {
            'text': "File hash: d41d8cd98f00b204e9800998ecf8427e",
            'expected': {'file_hash': ['d41d8cd98f00b204e9800998ecf8427e']}
        },
        {
            'text': "Bitcoin wallet: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            'expected': {'cryptocurrency': ['1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa']}
        },
        {
            'text': "Registry key: HKEY_LOCAL_MACHINE\\Software\\Microsoft\\Windows\\CurrentVersion\\Run",
            'expected': {'registry_key': ['HKEY_LOCAL_MACHINE\\Software\\Microsoft\\Windows\\CurrentVersion\\Run']}
        }
    ]
    
    passed = 0
    for test in test_cases:
        entities = extractor.extract_regex_entities(test['text'])
        
        # Group by category
        found = {}
        for entity in entities:
            category = entity['entity_category']
            if category not in found:
                found[category] = []
            found[category].append(entity['entity_name'])
        
        # Check expectations
        success = True
        for expected_cat, expected_values in test['expected'].items():
            if expected_cat not in found or set(expected_values) != set(found.get(expected_cat, [])):
                success = False
                break
        
        if success:
            print(f"✓ Test passed: {test['text'][:50]}...")
            passed += 1
        else:
            print(f"✗ Test failed: {test['text'][:50]}...")
            print(f"  Expected: {test['expected']}")
            print(f"  Found: {found}")
    
    print(f"\nRegex extraction tests: {passed}/{len(test_cases)} passed")
    return passed == len(test_cases)


def test_entity_validation():
    """Test entity validation."""
    print("\nTesting entity validation...")
    print("=" * 50)
    
    validator = EntityValidator()
    
    test_cases = [
        # Valid entities
        ('CVE-2023-12345', 'cve', True),
        ('CVE-1999-0001', 'cve', True),
        ('8.8.8.8', 'ip_address', True),
        ('google.com', 'domain', False),  # Popular domain, not an IOC
        ('evil-malware.com', 'domain', True),
        ('security.microsoft.com', 'domain', True),
        ('d41d8cd98f00b204e9800998ecf8427e', 'file_hash', True),
        ('user@example.org', 'email', True),
        
        # Invalid entities
        ('CVE-1998-12345', 'cve', False),  # Year too early
        ('192.168.1.1', 'ip_address', False),  # Private IP
        ('10.0.0.1', 'ip_address', False),  # Private IP
        ('example.com', 'domain', False),  # Test domain
        ('test.com', 'domain', False),  # Test domain
        ('00000000000000000000000000000000', 'file_hash', False),  # All zeros
        ('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', 'file_hash', False),  # All F's
        ('user@example.com', 'email', False),  # Example email
        ('invalid-cve', 'cve', False),
        ('999.999.999.999', 'ip_address', False),
        ('notahash', 'file_hash', False)
    ]
    
    passed = 0
    for entity, category, expected in test_cases:
        result = validator.validate_entity(entity, category)
        if result == expected:
            print(f"✓ {category}: {entity} -> {result}")
            passed += 1
        else:
            print(f"✗ {category}: {entity} -> Expected {expected}, got {result}")
    
    print(f"\nValidation tests: {passed}/{len(test_cases)} passed")
    return passed == len(test_cases)


def test_predefined_entity_extraction():
    """Test predefined entity extraction."""
    print("\nTesting predefined entity extraction...")
    print("=" * 50)
    
    extractor = EntityExtractor()
    
    # Check if we have predefined entities loaded
    total_entities = sum(len(entities) for entities in extractor.predefined_entities.values())
    print(f"Loaded {total_entities} predefined entities across {len(extractor.predefined_entities)} categories")
    
    if total_entities == 0:
        print("⚠ No predefined entities loaded. Make sure to run import_entities.py first.")
        return False
    
    # Test text with known entities
    test_text = """
    The Lazarus Group launched a new campaign using Cobalt Strike beacons.
    Microsoft Security detected the attack on Windows servers.
    The vulnerability was a Zero-Day Remote Code Execution flaw.
    """
    
    entities = extractor.extract_predefined_entities(test_text)
    
    print(f"\nFound {len(entities)} predefined entities:")
    by_category = {}
    for entity in entities:
        cat = entity['entity_category']
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(entity['entity_name'])
    
    for category, names in by_category.items():
        print(f"  {category}: {', '.join(names)}")
    
    # Check if we found expected entities
    expected_categories = ['apt_group', 'malware_family', 'security_vendor', 'platform', 'vulnerability_type']
    found_categories = set(by_category.keys())
    
    missing = set(expected_categories) - found_categories
    if missing:
        print(f"\n⚠ Missing expected categories: {missing}")
        return False
    
    return True


def test_full_article_extraction():
    """Test full article entity extraction."""
    print("\nTesting full article extraction...")
    print("=" * 50)
    
    extractor = EntityExtractor()
    
    # Create a test article
    test_article = {
        'rss_feeds_clean_id': 999999,  # Fake ID
        'rss_feeds_clean_title': {
            'title': 'Critical CVE-2023-98765 Found in Microsoft Exchange'
        },
        'rss_feeds_clean_content': {
            'content': """
            Security researchers discovered a critical vulnerability CVE-2023-98765 
            in Microsoft Exchange Server that allows Remote Code Execution.
            
            The Lazarus Group has been observed exploiting this vulnerability 
            using Cobalt Strike beacons communicating with C2 server at 185.220.101.45.
            
            Indicators of Compromise:
            - Malicious domain: evil-exchange-exploit.com
            - File hash (SHA256): 3b4d8c6f7e2a1d9c5f8b4a6e3d7c2b1a9f8e7d6c5b4a3d2c1b0a9f8e7d6c5b4a
            - Registry key: HKEY_LOCAL_MACHINE\\Software\\Microsoft\\Exchange\\Backdoor
            
            Affected platforms include Windows Server 2019 and Windows Server 2022.
            Organizations should patch immediately to prevent exploitation.
            """
        },
        'rss_feeds_raw_published_date': '2023-12-01 10:00:00',
        'rss_feeds_credibility': 90
    }
    
    # Extract entities
    entities = extractor.extract_entities_from_article(test_article)
    
    print(f"\nExtracted {len(entities)} entities:")
    
    # Group by category
    by_category = {}
    for entity in entities:
        cat = entity['entity_category']
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append({
            'name': entity['entity_name'],
            'confidence': entity['confidence'],
            'position': entity.get('position', 'unknown')
        })
    
    # Display results
    for category, entity_list in sorted(by_category.items()):
        print(f"\n{category}:")
        for entity in entity_list:
            print(f"  - {entity['name']} (confidence: {entity['confidence']:.2f}, position: {entity['position']})")
    
    # Check if we found expected entity types
    expected_types = ['cve', 'apt_group', 'malware_family', 'ip_address', 'domain', 
                     'file_hash', 'registry_key', 'platform', 'vulnerability_type']
    found_types = set(by_category.keys())
    
    print(f"\nExpected entity types: {len(expected_types)}")
    print(f"Found entity types: {len(found_types)}")
    
    missing = set(expected_types) - found_types
    if missing:
        print(f"Missing types: {missing}")
    
    return len(entities) > 10  # Expect at least 10 entities


def test_database_operations():
    """Test database operations for entity extraction."""
    print("\nTesting database operations...")
    print("=" * 50)
    
    try:
        extractor = EntityExtractor()
        
        # Get count of unprocessed articles
        articles = extractor.get_unprocessed_articles(limit=5)
        print(f"✓ Found {len(articles)} unprocessed articles")
        
        if articles:
            # Show sample article
            article = articles[0]
            title = article['rss_feeds_clean_title'].get('title', 'N/A') if isinstance(article['rss_feeds_clean_title'], dict) else 'N/A'
            print(f"\nSample article:")
            print(f"  ID: {article['rss_feeds_clean_id']}")
            print(f"  Title: {title[:60]}...")
            print(f"  Published: {article['rss_feeds_raw_published_date']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Database operation failed: {e}")
        return False


def test_confidence_scoring():
    """Test confidence scoring logic."""
    print("\nTesting confidence scoring...")
    print("=" * 50)
    
    extractor = EntityExtractor()
    
    # Test article with entity in both title and content
    test_article = {
        'rss_feeds_clean_id': 1,
        'rss_feeds_clean_title': {'title': 'CVE-2023-12345 Critical Vulnerability'},
        'rss_feeds_clean_content': {
            'content': 'The vulnerability CVE-2023-12345 is critical. CVE-2023-12345 affects many systems.'
        },
        'rss_feeds_raw_published_date': '2023-12-01',
        'rss_feeds_credibility': 90
    }
    
    entities = extractor.extract_entities_from_article(test_article)
    
    # Find the CVE entity
    cve_entity = next((e for e in entities if e['entity_name'] == 'CVE-2023-12345'), None)
    
    if cve_entity:
        print(f"✓ CVE entity found")
        print(f"  Base confidence: 0.95")
        print(f"  Final confidence: {cve_entity['confidence']}")
        print(f"  Position: {cve_entity['position']}")
        
        # Should have boosted confidence due to title appearance and multiple occurrences
        if cve_entity['confidence'] > 0.95:
            print("✓ Confidence correctly boosted")
            return True
        else:
            print("✗ Confidence not boosted as expected")
            return False
    else:
        print("✗ CVE entity not found")
        return False


def main():
    """Run all tests."""
    print("Entity Extractor Test Suite")
    print("=" * 70)
    
    tests = [
        ("Regex Extraction", test_regex_extraction),
        ("Entity Validation", test_entity_validation),
        ("Predefined Entity Extraction", test_predefined_entity_extraction),
        ("Full Article Extraction", test_full_article_extraction),
        ("Confidence Scoring", test_confidence_scoring),
        ("Database Operations", test_database_operations),
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
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())