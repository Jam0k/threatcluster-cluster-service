#!/usr/bin/env python3
"""
Remove Engadget, Security Online articles and Prime Day mentions
"""
import psycopg2
from src.config.settings import settings

def remove_articles():
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor()
    
    try:
        # First, get counts before deletion
        print("Checking articles to remove...\n")
        
        # Check Engadget articles
        cursor.execute("""
            SELECT COUNT(DISTINCT c.rss_feeds_clean_id)
            FROM cluster_data.rss_feeds_clean c
            JOIN cluster_data.rss_feeds_raw r ON c.rss_feeds_clean_raw_id = r.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
            WHERE LOWER(f.rss_feeds_name) LIKE '%engadget%'
        """)
        engadget_count = cursor.fetchone()[0]
        print(f"Engadget articles found: {engadget_count}")
        
        # Check Security Online articles
        cursor.execute("""
            SELECT COUNT(DISTINCT c.rss_feeds_clean_id)
            FROM cluster_data.rss_feeds_clean c
            JOIN cluster_data.rss_feeds_raw r ON c.rss_feeds_clean_raw_id = r.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
            WHERE LOWER(f.rss_feeds_name) LIKE '%security online%'
        """)
        security_online_count = cursor.fetchone()[0]
        print(f"Security Online articles found: {security_online_count}")
        
        # Check Prime Day articles
        cursor.execute("""
            SELECT COUNT(DISTINCT c.rss_feeds_clean_id)
            FROM cluster_data.rss_feeds_clean c
            WHERE LOWER(c.rss_feeds_clean_title->>'title') LIKE '%prime day%'
            OR LOWER(c.rss_feeds_clean_content->>'content') LIKE '%prime day%'
        """)
        prime_day_count = cursor.fetchone()[0]
        print(f"Prime Day articles found: {prime_day_count}")
        
        total_to_remove = engadget_count + security_online_count + prime_day_count
        
        if total_to_remove == 0:
            print("\nNo articles to remove!")
            return
        
        # Ask for confirmation
        print(f"\nTotal articles to remove: {total_to_remove}")
        response = input("Do you want to proceed? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Removal cancelled.")
            return
        
        print("\nRemoving articles...")
        
        # Remove from article_rankings first (foreign key constraint)
        cursor.execute("""
            DELETE FROM cluster_data.article_rankings
            WHERE article_rankings_clean_id IN (
                SELECT c.rss_feeds_clean_id
                FROM cluster_data.rss_feeds_clean c
                JOIN cluster_data.rss_feeds_raw r ON c.rss_feeds_clean_raw_id = r.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
                WHERE LOWER(f.rss_feeds_name) LIKE '%engadget%'
                OR LOWER(f.rss_feeds_name) LIKE '%security online%'
            )
        """)
        rankings_removed = cursor.rowcount
        
        # Also remove Prime Day rankings
        cursor.execute("""
            DELETE FROM cluster_data.article_rankings
            WHERE article_rankings_clean_id IN (
                SELECT c.rss_feeds_clean_id
                FROM cluster_data.rss_feeds_clean c
                WHERE LOWER(c.rss_feeds_clean_title->>'title') LIKE '%prime day%'
                OR LOWER(c.rss_feeds_clean_content->>'content') LIKE '%prime day%'
            )
        """)
        prime_day_rankings_removed = cursor.rowcount
        
        print(f"Removed article rankings: {rankings_removed + prime_day_rankings_removed}")
        
        # Remove from cluster_articles (foreign key constraint)
        
        # Remove Engadget cluster associations
        cursor.execute("""
            DELETE FROM cluster_data.cluster_articles
            WHERE cluster_articles_clean_id IN (
                SELECT c.rss_feeds_clean_id
                FROM cluster_data.rss_feeds_clean c
                JOIN cluster_data.rss_feeds_raw r ON c.rss_feeds_clean_raw_id = r.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
                WHERE LOWER(f.rss_feeds_name) LIKE '%engadget%'
            )
        """)
        engadget_cluster_removed = cursor.rowcount
        
        # Remove Security Online cluster associations
        cursor.execute("""
            DELETE FROM cluster_data.cluster_articles
            WHERE cluster_articles_clean_id IN (
                SELECT c.rss_feeds_clean_id
                FROM cluster_data.rss_feeds_clean c
                JOIN cluster_data.rss_feeds_raw r ON c.rss_feeds_clean_raw_id = r.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
                WHERE LOWER(f.rss_feeds_name) LIKE '%security online%'
            )
        """)
        security_online_cluster_removed = cursor.rowcount
        
        # Remove Prime Day cluster associations
        cursor.execute("""
            DELETE FROM cluster_data.cluster_articles
            WHERE cluster_articles_clean_id IN (
                SELECT c.rss_feeds_clean_id
                FROM cluster_data.rss_feeds_clean c
                WHERE LOWER(c.rss_feeds_clean_title->>'title') LIKE '%prime day%'
                OR LOWER(c.rss_feeds_clean_content->>'content') LIKE '%prime day%'
            )
        """)
        prime_day_cluster_removed = cursor.rowcount
        
        print(f"Removed cluster associations: {engadget_cluster_removed + security_online_cluster_removed + prime_day_cluster_removed}")
        
        # Now remove the articles themselves
        
        # Remove Engadget articles
        cursor.execute("""
            DELETE FROM cluster_data.rss_feeds_clean
            WHERE rss_feeds_clean_raw_id IN (
                SELECT r.rss_feeds_raw_id
                FROM cluster_data.rss_feeds_raw r
                JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
                WHERE LOWER(f.rss_feeds_name) LIKE '%engadget%'
            )
        """)
        engadget_clean_removed = cursor.rowcount
        
        # Remove Security Online articles
        cursor.execute("""
            DELETE FROM cluster_data.rss_feeds_clean
            WHERE rss_feeds_clean_raw_id IN (
                SELECT r.rss_feeds_raw_id
                FROM cluster_data.rss_feeds_raw r
                JOIN cluster_data.rss_feeds f ON r.rss_feeds_raw_feed_id = f.rss_feeds_id
                WHERE LOWER(f.rss_feeds_name) LIKE '%security online%'
            )
        """)
        security_online_clean_removed = cursor.rowcount
        
        # Remove Prime Day articles
        cursor.execute("""
            DELETE FROM cluster_data.rss_feeds_clean
            WHERE LOWER(rss_feeds_clean_title->>'title') LIKE '%prime day%'
            OR LOWER(rss_feeds_clean_content->>'content') LIKE '%prime day%'
        """)
        prime_day_clean_removed = cursor.rowcount
        
        # Remove from raw table
        cursor.execute("""
            DELETE FROM cluster_data.rss_feeds_raw
            WHERE rss_feeds_raw_feed_id IN (
                SELECT rss_feeds_id
                FROM cluster_data.rss_feeds
                WHERE LOWER(rss_feeds_name) LIKE '%engadget%'
                OR LOWER(rss_feeds_name) LIKE '%security online%'
            )
        """)
        raw_removed = cursor.rowcount
        
        # Also remove Prime Day from raw
        cursor.execute("""
            DELETE FROM cluster_data.rss_feeds_raw
            WHERE LOWER(rss_feeds_raw_xml->>'title') LIKE '%prime day%'
            OR LOWER(rss_feeds_raw_xml->>'description') LIKE '%prime day%'
        """)
        prime_day_raw_removed = cursor.rowcount
        
        # Clean up empty clusters
        cursor.execute("""
            UPDATE cluster_data.clusters
            SET clusters_is_active = false
            WHERE clusters_id NOT IN (
                SELECT DISTINCT cluster_articles_cluster_id
                FROM cluster_data.cluster_articles
            )
            AND clusters_is_active = true
        """)
        empty_clusters_deactivated = cursor.rowcount
        
        conn.commit()
        
        print("\n✅ Removal complete!")
        print(f"Engadget articles removed: {engadget_clean_removed}")
        print(f"Security Online articles removed: {security_online_clean_removed}")
        print(f"Prime Day articles removed: {prime_day_clean_removed}")
        print(f"Raw entries removed: {raw_removed + prime_day_raw_removed}")
        print(f"Empty clusters deactivated: {empty_clusters_deactivated}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    remove_articles()