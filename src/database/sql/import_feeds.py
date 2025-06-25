#!/usr/bin/env python3
"""
Import RSS feeds from config/feeds.yaml into the database.

This script reads the RSS feed configuration from feeds.yaml and imports
all feeds into the cluster_data.rss_feeds table.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m src.database.sql.import_feeds
"""
import psycopg2
import yaml
import sys
from pathlib import Path
from src.config.settings import settings


def load_feeds_config():
    """Load feeds configuration from YAML file."""
    config_path = Path(settings.config_dir) / "feeds.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Feeds configuration not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if 'feeds' not in config:
        raise ValueError("No 'feeds' key found in configuration file")
    
    return config['feeds']


def import_feeds():
    """Import RSS feeds into the database."""
    feeds = load_feeds_config()
    
    # Connect to database
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor()
    
    try:
        # Count existing feeds
        cursor.execute("SELECT COUNT(*) FROM cluster_data.rss_feeds")
        existing_count = cursor.fetchone()[0]
        print(f"Found {existing_count} existing feeds in database")
        
        # Import each feed
        imported = 0
        skipped = 0
        
        for feed in feeds:
            try:
                # Insert feed, skip if URL already exists
                cursor.execute("""
                    INSERT INTO cluster_data.rss_feeds 
                    (rss_feeds_url, rss_feeds_name, rss_feeds_category, 
                     rss_feeds_credibility, rss_feeds_is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (rss_feeds_url) DO NOTHING
                    RETURNING rss_feeds_id
                """, (
                    feed['url'],
                    feed['name'],
                    feed['category'],
                    feed.get('credibility', 50),
                    feed.get('is_active', True)
                ))
                
                result = cursor.fetchone()
                if result:
                    imported += 1
                    print(f"✓ Imported: {feed['name']}")
                else:
                    skipped += 1
                    print(f"- Skipped (already exists): {feed['name']}")
                    
            except psycopg2.Error as e:
                print(f"✗ Error importing {feed['name']}: {e}")
                conn.rollback()
                continue
        
        # Commit all changes
        conn.commit()
        
        # Final summary
        print(f"\n{'='*50}")
        print(f"Import Summary:")
        print(f"  Total feeds in config: {len(feeds)}")
        print(f"  Successfully imported: {imported}")
        print(f"  Skipped (duplicates): {skipped}")
        print(f"  Errors: {len(feeds) - imported - skipped}")
        
        # Show current total
        cursor.execute("SELECT COUNT(*) FROM cluster_data.rss_feeds")
        total_count = cursor.fetchone()[0]
        print(f"  Total feeds in database: {total_count}")
        print(f"{'='*50}\n")
        
    except Exception as e:
        print(f"\n✗ Import failed: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()
    
    return imported


def verify_import():
    """Verify imported feeds by showing a summary."""
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor()
    
    try:
        # Count by category
        cursor.execute("""
            SELECT rss_feeds_category, COUNT(*), AVG(rss_feeds_credibility)::INTEGER
            FROM cluster_data.rss_feeds
            WHERE rss_feeds_is_active = TRUE
            GROUP BY rss_feeds_category
            ORDER BY rss_feeds_category
        """)
        
        print("Active feeds by category:")
        for category, count, avg_cred in cursor.fetchall():
            print(f"  {category}: {count} feeds (avg credibility: {avg_cred})")
        
        # Show top credibility sources
        cursor.execute("""
            SELECT rss_feeds_name, rss_feeds_credibility, rss_feeds_category
            FROM cluster_data.rss_feeds
            WHERE rss_feeds_is_active = TRUE
            ORDER BY rss_feeds_credibility DESC
            LIMIT 10
        """)
        
        print("\nTop 10 most credible sources:")
        for name, cred, category in cursor.fetchall():
            print(f"  {cred}: {name} ({category})")
            
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("RSS Feed Import Tool")
    print("=" * 50)
    
    try:
        # Import feeds
        imported = import_feeds()
        if imported > 0:
            print(f"\n✓ Import completed successfully! Imported {imported} feeds.")
            
            # Show verification summary
            print("\nVerifying import...")
            verify_import()
        else:
            print("\n✗ No feeds were imported!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)