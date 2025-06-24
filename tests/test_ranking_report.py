#!/usr/bin/env python3
"""
Ranking Report

Comprehensive report of article and cluster rankings with detailed analysis.

Usage:
    python -m tests.test_ranking_report
"""

import psycopg2
import psycopg2.extras
from datetime import datetime
from src.config.settings import settings


def print_separator(char="=", length=120):
    """Print a separator line."""
    print(char * length)


def format_entities(entities_dict):
    """Format contributing entities for display."""
    if not entities_dict:
        return "None"
    
    entity_list = []
    for name, details in list(entities_dict.items())[:5]:
        entity_list.append(f"{name} ({details['category']}:{details['weight']})")
    
    return ", ".join(entity_list)


def main():
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        print_separator()
        print("THREATCLUSTER RANKING REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_separator()
        
        # Overall statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_articles,
                AVG(article_rankings_score) as avg_score,
                MIN(article_rankings_score) as min_score,
                MAX(article_rankings_score) as max_score,
                STDDEV(article_rankings_score) as stddev_score,
                COUNT(CASE WHEN article_rankings_score >= 90 THEN 1 END) as critical_count,
                COUNT(CASE WHEN article_rankings_score >= 70 AND article_rankings_score < 90 THEN 1 END) as high_count,
                COUNT(CASE WHEN article_rankings_score >= 50 AND article_rankings_score < 70 THEN 1 END) as medium_count,
                COUNT(CASE WHEN article_rankings_score < 50 THEN 1 END) as low_count
            FROM cluster_data.article_rankings
        """)
        
        stats = cursor.fetchone()
        
        print("\nRANKING STATISTICS")
        print("-" * 40)
        print(f"Total Ranked Articles: {stats['total_articles']}")
        print(f"Average Score: {stats['avg_score']:.2f}")
        print(f"Score Range: {stats['min_score']} - {stats['max_score']}")
        print(f"Standard Deviation: {stats['stddev_score']:.2f}")
        print(f"\nDistribution:")
        print(f"  Critical (90+): {stats['critical_count']} ({stats['critical_count']/stats['total_articles']*100:.1f}%)")
        print(f"  High (70-89): {stats['high_count']} ({stats['high_count']/stats['total_articles']*100:.1f}%)")
        print(f"  Medium (50-69): {stats['medium_count']} ({stats['medium_count']/stats['total_articles']*100:.1f}%)")
        print(f"  Low (<50): {stats['low_count']} ({stats['low_count']/stats['total_articles']*100:.1f}%)")
        
        # Top individual articles
        print("\n" + "=" * 120)
        print("TOP 15 INDIVIDUAL ARTICLES")
        print("=" * 120)
        
        cursor.execute("""
            SELECT 
                ar.*,
                rfc.rss_feeds_clean_extracted_entities
            FROM cluster_data.articles_with_rankings ar
            JOIN cluster_data.rss_feeds_clean rfc ON ar.rss_feeds_clean_id = rfc.rss_feeds_clean_id
            LIMIT 15
        """)
        
        for i, article in enumerate(cursor, 1):
            print(f"\n{i}. [{article['article_rankings_score']}] {article['article_title']}")
            print(f"   Source: {article['source_name']} (Credibility: {article['source_credibility']})")
            print(f"   Published: {article['published_date'].strftime('%Y-%m-%d %H:%M')}")
            print(f"   Cluster: {article['clusters_name'] or 'Unclustered'}")
            
            # Show score breakdown
            print(f"   Score Components:")
            print(f"     • Recency: {article['recency_score']:.1f}/100")
            print(f"     • Source: {article['source_credibility']}/100")
            print(f"     • Entity: {article['entity_score']:.1f}/100")
            print(f"     • Keyword: {article['keyword_score']:.1f}/100")
            
            # Show contributing factors
            factors = article['article_rankings_factors']
            if factors.get('keyword_matches'):
                print(f"   Key Terms: {', '.join(factors['keyword_matches'])}")
            if factors.get('contributing_entities'):
                print(f"   Key Entities: {format_entities(factors['contributing_entities'])}")
        
        # Top clusters
        print("\n" + "=" * 120)
        print("TOP 10 CLUSTERS BY SCORE")
        print("=" * 120)
        
        cursor.execute("""
            SELECT * FROM cluster_data.cluster_rankings
            LIMIT 10
        """)
        
        for i, cluster in enumerate(cursor, 1):
            print(f"\n{i}. [{cluster['cluster_score']}] {cluster['clusters_name']}")
            print(f"   Articles: {cluster['article_count']}")
            print(f"   Average Article Score: {cluster['avg_article_score']}")
            print(f"   Coherence: {cluster['clusters_coherence_score']:.3f}")
            
            # Get top articles in this cluster
            cursor.execute("""
                SELECT article_title, article_rankings_score
                FROM cluster_data.articles_with_rankings
                WHERE article_rankings_cluster_id = %s
                ORDER BY article_rankings_score DESC
                LIMIT 3
            """, (cluster['clusters_id'],))
            
            articles = cursor.fetchall()
            if articles:
                print("   Top Articles:")
                for article in articles:
                    print(f"     • [{article['article_rankings_score']}] {article['article_title'][:70]}...")
        
        # Keyword effectiveness analysis
        print("\n" + "=" * 120)
        print("MOST EFFECTIVE KEYWORDS")
        print("=" * 120)
        
        cursor.execute("""
            SELECT 
                keyword,
                COUNT(*) as occurrence_count,
                AVG(score) as avg_contribution
            FROM (
                SELECT 
                    jsonb_array_elements_text(ar.article_rankings_factors->'keyword_matches') as keyword,
                    ar.article_rankings_score as score
                FROM cluster_data.article_rankings ar
                WHERE ar.article_rankings_factors->'keyword_matches' IS NOT NULL
                    AND jsonb_typeof(ar.article_rankings_factors->'keyword_matches') = 'array'
            ) keywords
            GROUP BY keyword
            ORDER BY occurrence_count DESC, avg_contribution DESC
            LIMIT 20
        """)
        
        print("\nMost Frequently Matched Keywords:")
        for row in cursor:
            keyword = row['keyword'].strip('"')
            print(f"  • {keyword}: {row['occurrence_count']} occurrences (avg score: {row['avg_contribution']:.1f})")
        
        # Entity analysis
        print("\n" + "=" * 120)
        print("TOP CONTRIBUTING ENTITIES")
        print("=" * 120)
        
        cursor.execute("""
            SELECT 
                entity_name,
                entity_category,
                COUNT(*) as appearance_count,
                AVG(article_score) as avg_article_score
            FROM (
                SELECT 
                    (jsonb_each(ar.article_rankings_factors->'contributing_entities')).key as entity_name,
                    (jsonb_each(ar.article_rankings_factors->'contributing_entities')).value->>'category' as entity_category,
                    ar.article_rankings_score as article_score
                FROM cluster_data.article_rankings ar
                WHERE ar.article_rankings_factors->'contributing_entities' IS NOT NULL
            ) entities
            GROUP BY entity_name, entity_category
            ORDER BY appearance_count DESC, avg_article_score DESC
            LIMIT 20
        """)
        
        print("\nMost Impactful Entities:")
        for row in cursor:
            print(f"  • {row['entity_name']} ({row['entity_category']}): "
                  f"{row['appearance_count']} articles, avg score: {row['avg_article_score']:.1f}")
        
        # Recent high-priority articles
        print("\n" + "=" * 120)
        print("RECENT HIGH-PRIORITY ARTICLES (Last 24 hours, Score >= 60)")
        print("=" * 120)
        
        cursor.execute("""
            SELECT *
            FROM cluster_data.articles_with_rankings
            WHERE recency_score > 50
            AND article_rankings_score >= 60
            ORDER BY article_rankings_score DESC
            LIMIT 10
        """)
        
        recent_high = cursor.fetchall()
        if recent_high:
            for article in recent_high:
                print(f"\n[{article['article_rankings_score']}] {article['article_title']}")
                print(f"  Published: {article['published_date'].strftime('%Y-%m-%d %H:%M')} | "
                      f"Source: {article['source_name']}")
        else:
            print("\nNo recent high-priority articles found.")
        
        print("\n" + "=" * 120)
        
    finally:
        cursor.close()
        conn.close()
        


if __name__ == "__main__":
    main()