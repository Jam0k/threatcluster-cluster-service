#!/usr/bin/env python3
"""
Entity Sync Scheduler - Periodic synchronization of AI-extracted entities to database
"""
import logging
import argparse
import time
from datetime import datetime

from src.ai_summary.entity_sync_service import EntitySyncService
from src.config.settings import settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_entity_sync(hours: int = 24):
    """
    Run entity synchronization for recent clusters.
    
    Args:
        hours: Number of hours to look back for clusters with AI summaries
    """
    logger.info(f"Starting entity sync for clusters from the last {hours} hours")
    
    try:
        service = EntitySyncService()
        
        # Sync entities from recent clusters
        stats = service.sync_all_recent_clusters(hours=hours)
        
        logger.info(f"Entity sync completed: {stats}")
        
        # Log entity statistics
        entity_stats = service.get_entity_statistics()
        if entity_stats:
            logger.info(f"Current entity counts by source: {entity_stats.get('source_counts', {})}")
            
            ai_counts = entity_stats.get('ai_category_counts', {})
            if ai_counts:
                logger.info("AI-extracted entities by category:")
                for category, count in list(ai_counts.items())[:10]:  # Top 10 categories
                    logger.info(f"  - {category}: {count}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error during entity sync: {e}")
        return None


def run_scheduler(interval_minutes: int = 60, hours_lookback: int = 24):
    """
    Run entity sync on a schedule.
    
    Args:
        interval_minutes: Minutes between sync runs
        hours_lookback: Hours to look back for clusters
    """
    logger.info(f"Starting entity sync scheduler - running every {interval_minutes} minutes")
    logger.info(f"Looking back {hours_lookback} hours for clusters with AI summaries")
    
    while True:
        try:
            # Run sync
            start_time = datetime.now()
            stats = run_entity_sync(hours=hours_lookback)
            
            if stats:
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Sync completed in {duration:.1f} seconds")
            
            # Wait for next run
            logger.info(f"Sleeping for {interval_minutes} minutes until next run...")
            time.sleep(interval_minutes * 60)
            
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in scheduler: {e}")
            logger.info(f"Retrying in {interval_minutes} minutes...")
            time.sleep(interval_minutes * 60)


def main():
    """Main function to run entity sync scheduler"""
    parser = argparse.ArgumentParser(description='Entity Sync Scheduler')
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit instead of continuous scheduling'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Minutes between sync runs (default: 60)'
    )
    parser.add_argument(
        '--hours',
        type=int,
        default=24,
        help='Hours to look back for clusters (default: 24)'
    )
    
    args = parser.parse_args()
    
    if args.once:
        # Run once and exit
        logger.info("Running entity sync once")
        stats = run_entity_sync(hours=args.hours)
        if stats:
            logger.info("Entity sync completed successfully")
        else:
            logger.error("Entity sync failed")
    else:
        # Run on schedule
        run_scheduler(
            interval_minutes=args.interval,
            hours_lookback=args.hours
        )


if __name__ == "__main__":
    main()