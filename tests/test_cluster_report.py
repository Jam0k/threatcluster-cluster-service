#!/usr/bin/env python3
"""
Cluster Analysis Report

Generates a comprehensive report of all semantic clusters with:
- Cluster summary and statistics
- Primary article with full details
- Secondary articles summary
- Top entities extracted
- Source feed information

Usage:
    python -m tests.test_cluster_report
    
This will display:
1. Overall clustering statistics
2. Detailed breakdown of each cluster
3. Analysis of non-security content
4. Recommendations for feed filtering
"""

import psycopg2
import psycopg2.extras
from datetime import datetime
from collections import Counter
from src.config.settings import settings

def format_entities_summary(all_entities):
    """Create a summary of top entities across all articles."""
    entity_counts = Counter()
    
    for entities_data in all_entities:
        if entities_data and 'entities' in entities_data:
            for entity in entities_data['entities']:
                category = entity.get('entity_category', 'unknown')
                name = entity.get('entity_name', '')
                key = f"{category}:{name}"
                entity_counts[key] += 1
    
    # Get top 10 entities
    top_entities = []
    for key, count in entity_counts.most_common(10):
        category, name = key.split(':', 1)
        top_entities.append(f"{name} ({category})")
    
    return top_entities

def print_cluster_report(cluster_data, articles):
    """Print a formatted cluster report."""
    cluster = cluster_data
    
    print("=" * 100)
    print(f"CLUSTER: {cluster['clusters_name']}")
    print(f"ID: {cluster['clusters_id']} | Coherence: {cluster['clusters_coherence_score']:.3f} | Articles: {len(articles)}")
    print(f"Created: {cluster['clusters_created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 100)
    
    # Get primary article
    primary_articles = [a for a in articles if a['cluster_articles_is_primary']]
    secondary_articles = [a for a in articles if not a['cluster_articles_is_primary']]
    
    # Sources summary
    sources = set()
    for article in articles:
        sources.add(f"{article['rss_feeds_name']}")
    print(f"Sources: {', '.join(sources)}")
    
    # Entity summary across all articles
    all_entities = [a['rss_feeds_clean_extracted_entities'] for a in articles]
    top_entities = format_entities_summary(all_entities)
    if top_entities:
        print(f"Top Entities: {', '.join(top_entities[:5])}")
    print()
    
    # Primary article details
    if primary_articles:
        primary = primary_articles[0]
        print("PRIMARY ARTICLE:")
        print(f"  Title: {primary['rss_feeds_clean_title'].get('title', 'No title')}")
        print(f"  Source: {primary['rss_feeds_name']}")
        print(f"  Published: {primary['rss_feeds_raw_published_date']}")
        
        raw_xml = primary['rss_feeds_raw_xml']
        link = raw_xml.get('link', 'N/A') if isinstance(raw_xml, dict) else 'N/A'
        print(f"  Link: {link}")
        
        # Content preview
        content = primary['rss_feeds_clean_content'].get('content', '') if isinstance(primary['rss_feeds_clean_content'], dict) else ''
        content_preview = ' '.join(content.split()[:50]) + "..." if len(content.split()) > 50 else content
        print(f"  Preview: {content_preview}")
    
    # Secondary articles summary
    if secondary_articles:
        print(f"\nSECONDARY ARTICLES ({len(secondary_articles)}):")
        for article in secondary_articles[:3]:  # Show up to 3
            title = article['rss_feeds_clean_title'].get('title', 'No title')
            sim_score = article['cluster_articles_similarity_score']
            print(f"  • {title[:80]}... (similarity: {sim_score:.3f})")
        
        if len(secondary_articles) > 3:
            print(f"  ... and {len(secondary_articles) - 3} more articles")
    
    print()

def main():
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        # Get cluster summary statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT c.clusters_id) as total_clusters,
                COUNT(DISTINCT ca.cluster_articles_clean_id) as total_clustered_articles,
                AVG(c.clusters_coherence_score) as avg_coherence,
                COUNT(DISTINCT rf.rss_feeds_id) as unique_sources
            FROM cluster_data.clusters c
            JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            WHERE c.clusters_is_active = true
        """)
        
        stats = cursor.fetchone()
        
        print("=" * 100)
        print("SEMANTIC CLUSTERING ANALYSIS REPORT")
        print("=" * 100)
        print(f"Total Clusters: {stats['total_clusters']}")
        print(f"Total Clustered Articles: {stats['total_clustered_articles']}")
        print(f"Average Coherence Score: {stats['avg_coherence']:.3f}")
        print(f"Unique Sources: {stats['unique_sources']}")
        print()
        
        # Get all clusters ordered by creation date
        cursor.execute("""
            SELECT c.clusters_id, c.clusters_name, c.clusters_summary,
                   c.clusters_coherence_score, c.clusters_created_at
            FROM cluster_data.clusters c
            WHERE c.clusters_is_active = true
            ORDER BY c.clusters_created_at DESC
        """)
        
        clusters = cursor.fetchall()
        
        # Process each cluster
        for cluster in clusters:
            # Get articles for this cluster
            cursor.execute("""
                SELECT 
                    ca.cluster_articles_clean_id,
                    ca.cluster_articles_is_primary,
                    ca.cluster_articles_similarity_score,
                    rfc.rss_feeds_clean_title,
                    rfc.rss_feeds_clean_content,
                    rfc.rss_feeds_clean_extracted_entities,
                    rfr.rss_feeds_raw_published_date,
                    rfr.rss_feeds_raw_xml,
                    rf.rss_feeds_name,
                    rf.rss_feeds_url
                FROM cluster_data.cluster_articles ca
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                WHERE ca.cluster_articles_cluster_id = %s
                ORDER BY ca.cluster_articles_is_primary DESC, ca.cluster_articles_similarity_score DESC
            """, (cluster['clusters_id'],))
            
            articles = cursor.fetchall()
            print_cluster_report(cluster, articles)
        
        # Special section for non-security content
        print("=" * 100)
        print("NON-SECURITY CONTENT ANALYSIS")
        print("=" * 100)
        
        cursor.execute("""
            SELECT DISTINCT rf.rss_feeds_name, rf.rss_feeds_url, COUNT(DISTINCT ca.cluster_articles_clean_id) as article_count
            FROM cluster_data.clusters c
            JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            WHERE c.clusters_is_active = true
            AND (c.clusters_name LIKE '%Hardware%' OR c.clusters_name LIKE '%Question%')
            GROUP BY rf.rss_feeds_name, rf.rss_feeds_url
            ORDER BY article_count DESC
        """)
        
        non_security = cursor.fetchall()
        if non_security:
            print("\nFeeds containing non-security discussions:")
            for feed in non_security:
                print(f"  • {feed['rss_feeds_name']} ({feed['rss_feeds_url']}) - {feed['article_count']} articles")
            print("\nRecommendation: Consider filtering these feeds or adjusting clustering parameters to focus on security content.")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()