#!/usr/bin/env python3
"""
Test the new scoring system for RSS feed filtering.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.feeds.rss_fetcher import RSSFeedFetcher


def test_scoring_examples():
    """Test the scoring system with known examples."""
    fetcher = RSSFeedFetcher()
    
    # Test cases: (title, description, expected_result_for_general_news)
    test_cases = [
        # Consumer tech - should be filtered out
        (
            "Prime Day deal: Pick up this Roomba combo robot vacuum and mop while it's nearly 50 percent off",
            "Engadget has been testing and reviewing consumer tech since 2004. Our stories may include affiliate links; if you buy something through a link, we may earn a commission.",
            False  # Should be rejected from general news
        ),
        (
            "A bundle of two Blink Mini 2 security cameras is only $35 for Prime Day",
            "The best security camera deals for Prime Day. Save on home security devices.",
            False  # Should be rejected - consumer product
        ),
        (
            "The best Prime Day kitchen deals you can get right now on air fryers, Instant Pots and sous vide machines",
            "Save big on kitchen appliances during Amazon Prime Day 2025.",
            False  # Should be rejected - kitchen deals
        ),
        (
            "Amazon Prime Day 2025: The best deals live right now, plus everything else you need to know",
            "Shop the best Prime Day deals on tech, home, and more.",
            False  # Should be rejected - shopping deals
        ),
        
        # Real security news - should pass
        (
            "Ingram Micro outage caused by SafePay ransomware attack",
            "An ongoing outage at IT giant Ingram Micro is caused by a SafePay ransomware attack that led to the shutdown of systems.",
            True  # Should pass - real ransomware attack
        ),
        (
            "Russia Jailed Hacker Who Worked for Ukrainian Intelligence to Launch Cyberattacks on Critical Infrastructure",
            "Russian Federal Security Service (FSB) officers have detained two hackers in Siberia who conducted cyberattacks on critical infrastructure facilities.",
            True  # Should pass - real cyber attack
        ),
        (
            "New Phishing Attack Impersonates as DWP Attacking Users to Steal Credit Card Data",
            "A sophisticated phishing campaign targeting UK citizens has emerged, masquerading as official communications from the Department for Work and Pensions.",
            True  # Should pass - phishing attack
        ),
        (
            "CitrixBleed 2 Vulnerability PoC Released",
            "Critical flaw in Citrix NetScaler devices echoes infamous 2023 security breach that crippled major organizations worldwide.",
            True  # Should pass - vulnerability
        ),
        
        # Edge cases
        (
            "Live Webinar | Reducing and Managing Human Risk in the Age of AI",
            "3rd Party Risk Management, Governance & Risk Management, IT Risk Management Live Webinar",
            False  # Should be rejected - webinar/marketing
        ),
        (
            "Security camera vulnerability allows remote access without authentication",
            "Researchers discovered a critical vulnerability in popular security camera models that allows attackers to gain remote access.",
            True  # Should pass - real vulnerability in security camera
        ),
    ]
    
    print("Testing RSS Feed Scoring System")
    print("=" * 80)
    
    for title, description, expected_for_general in test_cases:
        print(f"\nTitle: {title[:60]}...")
        print(f"Description: {description[:80]}...")
        
        # Test as general news (threshold: 70)
        result_general = fetcher.is_security_relevant(title, description, "", "general_news")
        score_general = fetcher.last_relevance_score
        
        # Test as cybersecurity feed (threshold: 30)
        result_cyber = fetcher.is_security_relevant(title, description, "", "cybersecurity")
        score_cyber = fetcher.last_relevance_score
        
        print(f"Score: {score_general}")
        print(f"General News (threshold 70): {'PASS' if result_general else 'FAIL'} - {'✓' if result_general == expected_for_general else '✗ WRONG'}")
        print(f"Cybersecurity (threshold 30): {'PASS' if result_cyber else 'FAIL'}")
        
        if result_general != expected_for_general:
            print(f"  WARNING: Expected {'PASS' if expected_for_general else 'FAIL'} for general news")


def test_score_breakdown():
    """Show detailed score breakdown for specific examples."""
    fetcher = RSSFeedFetcher()
    
    print("\n\nDetailed Score Breakdown")
    print("=" * 80)
    
    examples = [
        ("Prime Day deal: Pick up this Roomba combo robot vacuum and mop", "Get 50% off this smart home device"),
        ("Critical RCE vulnerability discovered in Roomba vacuum cleaners", "Security researchers found a remote code execution flaw affecting millions of devices"),
    ]
    
    for title, desc in examples:
        print(f"\nAnalyzing: {title}")
        score = fetcher._calculate_relevance_score(title, desc)
        print(f"Final Score: {score}")


if __name__ == "__main__":
    test_scoring_examples()
    test_score_breakdown()