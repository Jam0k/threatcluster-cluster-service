#!/usr/bin/env python3
"""
RSS Feed Scheduler

Schedules periodic RSS feed fetching based on configuration.
Can be run as a daemon or triggered manually.
"""
import time
import signal
import sys
import json
from datetime import datetime
from typing import Optional
import structlog
import psycopg2

from src.config.settings import settings
from src.feeds.rss_fetcher import RSSFeedFetcher


logger = structlog.get_logger(__name__)


class FeedScheduler:
    """Manages scheduled RSS feed fetching."""
    
    def __init__(self):
        """Initialize the scheduler."""
        self.config = settings.app_config.get('pipeline', {})
        self.fetch_interval_hours = self.config.get('fetch_interval_hours', 1)
        self.fetch_interval_seconds = self.fetch_interval_hours * 3600
        self.running = False
        self.fetcher = RSSFeedFetcher()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_last_fetch_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful fetch."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT MAX(rss_feeds_raw_created_at)
                FROM cluster_data.rss_feeds_raw
            """)
            result = cursor.fetchone()
            return result[0] if result[0] else None
        finally:
            cursor.close()
            conn.close()
    
    def should_fetch(self) -> bool:
        """Determine if it's time to fetch feeds."""
        last_fetch = self.get_last_fetch_time()
        
        if not last_fetch:
            logger.info("no_previous_fetch_found")
            return True
        
        time_since_fetch = datetime.now() - last_fetch
        should_fetch = time_since_fetch.total_seconds() >= self.fetch_interval_seconds
        
        logger.debug("fetch_check",
                    last_fetch=last_fetch.isoformat(),
                    time_since_fetch_hours=time_since_fetch.total_seconds() / 3600,
                    should_fetch=should_fetch)
        
        return should_fetch
    
    def run_once(self) -> dict:
        """Run a single fetch cycle."""
        logger.info("starting_fetch_cycle")
        
        try:
            stats = self.fetcher.process_all_feeds()
            return stats
            
        except Exception as e:
            logger.error("fetch_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, fetching feeds periodically."""
        logger.info("starting_feed_scheduler_daemon",
                   fetch_interval_hours=self.fetch_interval_hours)
        
        self.running = True
        
        while self.running:
            try:
                if self.should_fetch():
                    self.run_once()
                else:
                    logger.debug("skipping_fetch_not_time_yet")
                
                # Sleep for a short interval to check conditions
                # This allows for responsive shutdown
                for _ in range(60):  # Check every minute
                    if not self.running:
                        break
                    time.sleep(60)
                    
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                # Continue running after errors
                time.sleep(300)  # Wait 5 minutes after error
    


def main():
    """Run the scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='RSS Feed Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    
    args = parser.parse_args()
    
    scheduler = FeedScheduler()
    
    if args.once:
        # Run once
        stats = scheduler.run_once()
        print("\nFetch completed successfully!")
        print(f"Articles stored: {stats['articles_stored']}")
        print(f"Processing time: {stats.get('processing_time_seconds', 0)} seconds")
    
    else:
        # Run as daemon
        print(f"Starting RSS feed scheduler (interval: {scheduler.fetch_interval_hours} hours)")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()