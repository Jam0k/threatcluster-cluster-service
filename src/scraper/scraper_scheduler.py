#!/usr/bin/env python3
"""
Article Scraper Scheduler

Schedules periodic article scraping after RSS feeds are fetched.
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
from src.scraper.article_scraper import ArticleScraper


logger = structlog.get_logger(__name__)


class ScraperScheduler:
    """Manages scheduled article scraping."""
    
    def __init__(self):
        """Initialize the scheduler."""
        self.config = settings.app_config.get('pipeline', {})
        self.fetch_interval_hours = self.config.get('fetch_interval_hours', 1)
        self.fetch_interval_seconds = self.fetch_interval_hours * 3600
        self.batch_size = self.config.get('processing_batch_size', 100)
        self.running = False
        self.scraper = ArticleScraper()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_unprocessed_count(self) -> int:
        """Get count of unprocessed articles."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM cluster_data.rss_feeds_raw
                WHERE rss_feeds_raw_processed = FALSE
            """)
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()
            conn.close()
    
    def run_once(self) -> dict:
        """Run a single scraping cycle."""
        logger.info("starting_scraping_cycle")
        
        try:
            # Check if there are articles to process
            unprocessed_count = self.get_unprocessed_count()
            
            if unprocessed_count == 0:
                logger.info("no_articles_to_scrape")
                return {'articles_attempted': 0}
            
            logger.info("articles_pending_scraping", count=unprocessed_count)
            
            # Process articles in batches
            total_stats = {
                'articles_attempted': 0,
                'articles_success': 0,
                'articles_failed': 0,
                'content_fallback': 0,
                'batches_processed': 0
            }
            
            while unprocessed_count > 0:
                # Process one batch
                batch_stats = self.scraper.process_batch(limit=self.batch_size)
                
                # Aggregate statistics
                total_stats['articles_attempted'] += batch_stats['articles_attempted']
                total_stats['articles_success'] += batch_stats['articles_success']
                total_stats['articles_failed'] += batch_stats['articles_failed']
                total_stats['content_fallback'] += batch_stats.get('content_fallback', 0)
                total_stats['batches_processed'] += 1
                
                # Check remaining articles
                unprocessed_count = self.get_unprocessed_count()
                
                if batch_stats['articles_attempted'] == 0:
                    # No articles were processed, break to avoid infinite loop
                    break
                
                logger.info("batch_completed",
                          batch=total_stats['batches_processed'],
                          remaining=unprocessed_count)
            
            logger.info("scraping_cycle_completed", **total_stats)
            return total_stats
            
        except Exception as e:
            logger.error("scraping_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, scraping articles periodically."""
        logger.info("starting_scraper_daemon",
                   check_interval_minutes=15)
        
        self.running = True
        
        while self.running:
            try:
                # Check for unprocessed articles
                unprocessed_count = self.get_unprocessed_count()
                
                if unprocessed_count > 0:
                    logger.info("unprocessed_articles_found", count=unprocessed_count)
                    self.run_once()
                else:
                    logger.debug("no_unprocessed_articles")
                
                # Wait before next check (15 minutes)
                for _ in range(15):
                    if not self.running:
                        break
                    time.sleep(60)
                    
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                # Continue running after errors
                time.sleep(300)  # Wait 5 minutes after error


def main():
    """Run the scraper scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Article Scraper Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    parser.add_argument('--limit', type=int,
                       help='Limit number of articles to process')
    
    args = parser.parse_args()
    
    scheduler = ScraperScheduler()
    
    if args.limit:
        # Override batch size for this run
        scheduler.scraper.batch_size = args.limit
    
    if args.once:
        # Run once
        stats = scheduler.run_once()
        print("\nScraping completed!")
        print(f"Articles scraped: {stats.get('articles_success', 0)}")
        print(f"Articles failed: {stats.get('articles_failed', 0)}")
        print(f"Batches processed: {stats.get('batches_processed', 0)}")
    else:
        # Run as daemon
        print("Starting article scraper daemon")
        print("Checking for new articles every 15 minutes...")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()