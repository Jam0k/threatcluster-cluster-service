#!/usr/bin/env python3
"""
Regenerate AI summary for a specific cluster

Usage:
    python -m src.ai_summary.regenerate_summary --cluster-id <cluster_id>
"""
import argparse
import asyncio
import logging
import sys
import asyncpg
import json
from typing import Optional

from src.config.settings import settings
from src.ai_summary.ai_summary_service import AISummaryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def regenerate_cluster_summary(cluster_id: int):
    """Regenerate AI summary for a specific cluster"""
    # Create database connection
    conn = await asyncpg.connect(
        host=settings.db_config['host'],
        port=settings.db_config['port'],
        database=settings.db_config['database'],
        user=settings.db_config['username'],
        password=settings.db_config['password'],
        ssl='require'
    )
    
    try:
        # Get cluster articles
        query = """
            SELECT 
                rfc.rss_feeds_clean_id,
                rfc.rss_feeds_clean_title->>'title' as title,
                rfc.rss_feeds_clean_content->>'content' as content,
                rf.rss_feeds_name as source,
                rfr.rss_feeds_raw_published_date as published_date
            FROM cluster_data.cluster_articles ca
            JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            WHERE ca.cluster_articles_cluster_id = $1
            ORDER BY rfr.rss_feeds_raw_published_date DESC
        """
        
        rows = await conn.fetch(query, cluster_id)
        
        if not rows:
            logger.error(f"No articles found for cluster {cluster_id}")
            return False
            
        logger.info(f"Found {len(rows)} articles for cluster {cluster_id}")
        
        # Format articles for AI summary
        articles = []
        for row in rows:
            articles.append({
                'title': row['title'] or 'Untitled',
                'content': row['content'] or '',
                'source': row['source'] or 'Unknown',
                'published_date': row['published_date'].isoformat() if row['published_date'] else ''
            })
        
        # Generate AI summary
        ai_service = AISummaryService()
        summary_data = await ai_service.generate_ai_summary(articles)
        
        if summary_data:
            # Update cluster with new summary
            update_query = """
                UPDATE cluster_data.clusters 
                SET 
                    ai_summary = $1,
                    has_ai_summary = true,
                    ai_summary_generated_at = CURRENT_TIMESTAMP
                WHERE clusters_id = $2
            """
            
            await conn.execute(update_query, json.dumps(summary_data), cluster_id)
            
            logger.info(f"Successfully updated AI summary for cluster {cluster_id}")
            
            # Log TTPs if present
            if 'ttps' in summary_data and summary_data['ttps']:
                logger.info(f"Generated {len(summary_data['ttps'])} TTPs:")
                for ttp in summary_data['ttps'][:5]:  # Show first 5
                    logger.info(f"  - {ttp}")
                    
            print(f"✓ Successfully regenerated summary for cluster {cluster_id}")
            return True
        else:
            logger.error("Failed to generate AI summary")
            return False
            
    except Exception as e:
        logger.error(f"Error regenerating summary for cluster {cluster_id}: {e}")
        return False
    finally:
        await conn.close()


async def main(args=None):
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Regenerate AI summary for a cluster')
    parser.add_argument('--cluster-id', type=int, required=True, help='Cluster ID to regenerate')
    
    if args:
        parsed_args = parser.parse_args(args)
    else:
        parsed_args = parser.parse_args()
    
    success = await regenerate_cluster_summary(parsed_args.cluster_id)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))