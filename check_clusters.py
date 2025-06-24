#!/usr/bin/env python3
"""Check clustering results"""

import psycopg2
import psycopg2.extras
from src.config.settings import settings

def main():
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        # Get clusters
        cursor.execute("""
            SELECT c.clusters_id, c.clusters_name, c.clusters_coherence_score,
                   c.clusters_created_at, COUNT(ca.cluster_articles_clean_id) as article_count
            FROM cluster_data.clusters c
            LEFT JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            WHERE c.clusters_is_active = true
            GROUP BY c.clusters_id
            ORDER BY c.clusters_created_at DESC
            LIMIT 10
        """)
        
        clusters = cursor.fetchall()
        
        print(f"Found {len(clusters)} active clusters:\n")
        
        for cluster in clusters:
            print(f"Cluster ID: {cluster['clusters_id']}")
            print(f"  Name: {cluster['clusters_name']}")
            print(f"  Articles: {cluster['article_count']}")
            print(f"  Coherence: {cluster['clusters_coherence_score']:.3f}")
            print(f"  Created: {cluster['clusters_created_at']}")
            
            # Get articles in this cluster
            cursor.execute("""
                SELECT rfc.rss_feeds_clean_title,
                       ca.cluster_articles_is_primary,
                       ca.cluster_articles_similarity_score
                FROM cluster_data.cluster_articles ca
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                WHERE ca.cluster_articles_cluster_id = %s
                ORDER BY ca.cluster_articles_is_primary DESC, ca.cluster_articles_similarity_score DESC
                LIMIT 3
            """, (cluster['clusters_id'],))
            
            articles = cursor.fetchall()
            print("  Articles:")
            for article in articles:
                title = article['rss_feeds_clean_title'].get('title', 'N/A')[:60]
                primary = " (PRIMARY)" if article['cluster_articles_is_primary'] else ""
                print(f"    - {title}...{primary}")
            print()
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()