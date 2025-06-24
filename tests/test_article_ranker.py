#!/usr/bin/env python3
"""
Test Article Ranking Module

Tests the article ranking functionality with various article types
and validates scoring algorithms.

Usage:
    python -m tests.test_article_ranker
"""

import sys
from datetime import datetime, timedelta, timezone
from src.ranking.article_ranker import ArticleRanker
import psycopg2
import psycopg2.extras
from src.config.settings import settings


def test_recency_scoring():
    """Test recency score calculation."""
    print("\nTesting recency scoring...")
    print("=" * 50)
    
    ranker = ArticleRanker()
    
    # Test cases
    now = datetime.now(timezone.utc)
    test_cases = [
        ("Just published", now, 100.0),
        ("1 hour ago", now - timedelta(hours=1), 95.8),
        ("6 hours ago", now - timedelta(hours=6), 75.0),
        ("12 hours ago", now - timedelta(hours=12), 50.0),
        ("24 hours ago", now - timedelta(hours=24), 0.0),
        ("48 hours ago", now - timedelta(hours=48), 0.0),
    ]
    
    for desc, pub_date, expected in test_cases:
        score = ranker.calculate_recency_score(pub_date)
        print(f"{desc}: {score:.1f} (expected ~{expected:.1f})")
        
        # Allow small variance due to processing time
        assert abs(score - expected) < 2.0, f"Score {score} too far from expected {expected}"
    
    print("✓ Recency scoring passed")
    return True


def test_keyword_scoring():
    """Test keyword severity scoring."""
    print("\nTesting keyword severity scoring...")
    print("=" * 50)
    
    ranker = ArticleRanker()
    
    # Test cases
    test_cases = [
        {
            "title": "Critical Zero-Day Vulnerability Discovered",
            "content": "A new zero-day exploit has been found affecting millions.",
            "expected_min": 80,
            "desc": "Critical severity keywords"
        },
        {
            "title": "Ransomware Attack Hits Major Hospital",
            "content": "LockBit ransomware group claims responsibility for attack.",
            "expected_min": 70,
            "desc": "High severity attack"
        },
        {
            "title": "New Security Update Released",
            "content": "Microsoft releases patches for several vulnerabilities.",
            "expected_min": 30,
            "desc": "Medium severity update"
        },
        {
            "title": "Weekly Security News Roundup",
            "content": "This week in cybersecurity news and updates.",
            "expected_min": 0,
            "desc": "Low severity general news"
        }
    ]
    
    for test in test_cases:
        score, keywords = ranker.calculate_keyword_severity_score(
            test["title"], 
            test["content"]
        )
        print(f"\n{test['desc']}:")
        print(f"  Title: {test['title']}")
        print(f"  Score: {score:.1f}")
        print(f"  Keywords found: {', '.join(keywords) if keywords else 'None'}")
        
        assert score >= test["expected_min"], f"Score {score} below expected {test['expected_min']}"
    
    print("\n✓ Keyword scoring passed")
    return True


def test_entity_scoring():
    """Test entity importance scoring."""
    print("\nTesting entity importance scoring...")
    print("=" * 50)
    
    ranker = ArticleRanker()
    
    # Test cases
    test_cases = [
        {
            "desc": "High-importance entities",
            "entities": {
                "entities": [
                    {"entity_name": "Lazarus Group", "entity_category": "apt_group"},
                    {"entity_name": "CVE-2023-1234", "entity_category": "cve"},
                    {"entity_name": "REvil", "entity_category": "ransomware_group"}
                ]
            },
            "expected_min": 70
        },
        {
            "desc": "Mixed importance entities",
            "entities": {
                "entities": [
                    {"entity_name": "Microsoft", "entity_category": "company"},
                    {"entity_name": "Windows", "entity_category": "platform"},
                    {"entity_name": "example.com", "entity_category": "domain"}
                ]
            },
            "expected_min": 40
        },
        {
            "desc": "No entities",
            "entities": {"entities": []},
            "expected_min": 0
        }
    ]
    
    for test in test_cases:
        score, contributing = ranker.calculate_entity_importance_score(test["entities"])
        print(f"\n{test['desc']}:")
        print(f"  Score: {score:.1f}")
        print(f"  Contributing: {list(contributing.keys())}")
        
        assert score >= test["expected_min"], f"Score {score} below expected {test['expected_min']}"
    
    print("\n✓ Entity scoring passed")
    return True


def test_article_ranking():
    """Test full article ranking with real data."""
    print("\nTesting article ranking with database data...")
    print("=" * 50)
    
    ranker = ArticleRanker()
    
    # Get a few test articles
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        cursor.execute("""
            SELECT 
                rfc.rss_feeds_clean_id,
                rfc.rss_feeds_clean_title,
                rfc.rss_feeds_clean_content,
                rfc.rss_feeds_clean_extracted_entities,
                rfr.rss_feeds_raw_published_date,
                rf.rss_feeds_credibility,
                rf.rss_feeds_name
            FROM cluster_data.rss_feeds_clean rfc
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            ORDER BY rfr.rss_feeds_raw_published_date DESC
            LIMIT 5
        """)
        
        articles = cursor.fetchall()
        
        if not articles:
            print("No articles found for testing")
            return True
        
        print(f"\nRanking {len(articles)} articles:")
        
        for article in articles:
            article_dict = dict(article)
            score, factors = ranker.calculate_article_score(article_dict)
            
            title = article_dict['rss_feeds_clean_title'].get('title', 'Unknown')
            print(f"\n{title[:60]}...")
            print(f"  Source: {article_dict['rss_feeds_name']}")
            print(f"  Final Score: {score}")
            print(f"  Components:")
            print(f"    Recency: {factors['recency_score']}")
            print(f"    Source: {factors['source_credibility']}")
            print(f"    Entity: {factors['entity_importance']}")
            print(f"    Keyword: {factors['keyword_severity']}")
            
            if factors['keyword_matches']:
                print(f"  Keywords: {', '.join(factors['keyword_matches'][:3])}")
            if factors['contributing_entities']:
                entities = list(factors['contributing_entities'].keys())[:3]
                print(f"  Entities: {', '.join(entities)}")
        
        print("\n✓ Article ranking passed")
        return True
        
    finally:
        cursor.close()
        conn.close()


def test_ranking_distribution():
    """Test ranking score distribution analysis."""
    print("\nTesting ranking distribution...")
    print("=" * 50)
    
    ranker = ArticleRanker()
    
    # Rank some articles first
    print("Running batch ranking...")
    stats = ranker.rank_articles(batch_size=20, time_window_hours=168)
    
    print(f"Ranked {stats['articles_ranked']} articles")
    
    # Get distribution
    dist = ranker.get_ranking_distribution()
    
    print("\nScore Distribution:")
    print(f"  Total articles: {dist['total_articles']}")
    print(f"  Average score: {dist['average_score']}")
    print(f"  Min score: {dist['min_score']}")
    print(f"  Max score: {dist['max_score']}")
    print(f"  Std deviation: {dist['std_deviation']}")
    print(f"\nBreakdown:")
    print(f"  Critical (90+): {dist['distribution']['critical']}")
    print(f"  High (70-89): {dist['distribution']['high']}")
    print(f"  Medium (50-69): {dist['distribution']['medium']}")
    print(f"  Low (<50): {dist['distribution']['low']}")
    
    return True


def main():
    """Run all tests."""
    print("Article Ranking Test Suite")
    print("=" * 70)
    
    tests = [
        ("Recency Scoring", test_recency_scoring),
        ("Keyword Scoring", test_keyword_scoring),
        ("Entity Scoring", test_entity_scoring),
        ("Article Ranking", test_article_ranking),
        ("Ranking Distribution", test_ranking_distribution),
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