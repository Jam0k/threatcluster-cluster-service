#!/usr/bin/env python3
"""
Entity Extraction Scheduler

Schedules periodic entity extraction from cleaned articles.
Can be run as a daemon or triggered manually.
"""
import time
import signal
import sys
from datetime import datetime
from typing import Optional
import structlog
import psycopg2

from src.config.settings import settings
from src.entity_extraction.entity_extractor import EntityExtractor


logger = structlog.get_logger(__name__)


class EntityExtractionScheduler:
    """Manages scheduled entity extraction from articles."""
    
    def __init__(self):
        """Initialize the scheduler."""
        self.config = settings.app_config.get('pipeline', {})
        self.batch_size = self.config.get('processing_batch_size', 100)
        self.running = False
        self.extractor = EntityExtractor()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_unprocessed_count(self) -> int:
        """Get count of articles needing entity extraction."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM cluster_data.rss_feeds_clean
                WHERE rss_feeds_clean_processed = TRUE
                AND rss_feeds_clean_extracted_entities IS NULL
            """)
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()
            conn.close()
    
    def run_once(self) -> dict:
        """Run a single entity extraction cycle."""
        logger.info("starting_entity_extraction_cycle")
        
        try:
            # Check if there are articles to process
            unprocessed_count = self.get_unprocessed_count()
            
            if unprocessed_count == 0:
                logger.info("no_articles_for_entity_extraction")
                return {'articles_processed': 0}
            
            logger.info("articles_pending_entity_extraction", count=unprocessed_count)
            
            # Process articles in batches
            total_stats = {
                'articles_processed': 0,
                'entities_extracted': 0,
                'new_entities_discovered': 0,
                'extraction_errors': 0,
                'batches_processed': 0
            }
            
            while unprocessed_count > 0:
                # Process one batch
                batch_stats = self.extractor.process_batch(limit=self.batch_size)
                
                # Aggregate statistics
                total_stats['articles_processed'] += batch_stats['articles_processed']
                total_stats['entities_extracted'] += batch_stats['entities_extracted']
                total_stats['new_entities_discovered'] += batch_stats['new_entities_discovered']
                total_stats['extraction_errors'] += batch_stats.get('extraction_errors', 0)
                total_stats['batches_processed'] += 1
                
                # Check remaining articles
                unprocessed_count = self.get_unprocessed_count()
                
                if batch_stats['articles_processed'] == 0:
                    # No articles were processed, break to avoid infinite loop
                    break
                
                logger.info("batch_completed",
                          batch=total_stats['batches_processed'],
                          remaining=unprocessed_count)
            
            logger.info("entity_extraction_cycle_completed", **total_stats)
            return total_stats
            
        except Exception as e:
            logger.error("entity_extraction_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, extracting entities periodically."""
        logger.info("starting_entity_extraction_daemon",
                   check_interval_minutes=20)
        
        self.running = True
        
        while self.running:
            try:
                # Check for unprocessed articles
                unprocessed_count = self.get_unprocessed_count()
                
                if unprocessed_count > 0:
                    logger.info("unprocessed_articles_found", count=unprocessed_count)
                    self.run_once()
                else:
                    logger.debug("no_articles_for_entity_extraction")
                
                # Wait before next check (20 minutes)
                for _ in range(20):
                    if not self.running:
                        break
                    time.sleep(60)
                    
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                # Continue running after errors
                time.sleep(300)  # Wait 5 minutes after error


def main():
    """Run the entity extraction scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Entity Extraction Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    parser.add_argument('--limit', type=int,
                       help='Limit number of articles to process')
    
    args = parser.parse_args()
    
    scheduler = EntityExtractionScheduler()
    
    if args.limit:
        # Override batch size for this run
        scheduler.extractor.batch_size = args.limit
    
    if args.once:
        # Run once
        stats = scheduler.run_once()
        print("\nEntity extraction completed!")
        print(f"Articles processed: {stats.get('articles_processed', 0)}")
        print(f"Entities extracted: {stats.get('entities_extracted', 0)}")
        print(f"New entities discovered: {stats.get('new_entities_discovered', 0)}")
        print(f"Batches processed: {stats.get('batches_processed', 0)}")
    else:
        # Run as daemon
        print("Starting entity extraction daemon")
        print("Checking for new articles every 20 minutes...")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()