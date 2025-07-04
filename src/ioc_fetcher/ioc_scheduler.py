#!/usr/bin/env python3
"""
IOC Fetcher Scheduler

Schedules periodic IOC fetching from threat intelligence feeds.
Can be run as a daemon or triggered manually.
"""
import time
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional
import structlog
import psycopg2

from src.config.settings import settings
from src.ioc_fetcher.ioc_fetcher import IOCFetcher

logger = structlog.get_logger(__name__)


class IOCScheduler:
    """Manages scheduled IOC fetching."""
    
    def __init__(self):
        """Initialize the scheduler."""
        # Default to 24-hour interval
        self.fetch_interval_hours = 24
        self.fetch_interval_seconds = self.fetch_interval_hours * 3600
        self.running = False
        self.fetcher = IOCFetcher()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_last_fetch_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful IOC fetch."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            # Check when IOCs were last added/updated from ioc_feeds source
            cursor.execute("""
                SELECT MAX(entities_added_on)
                FROM cluster_data.entities
                WHERE entities_source = 'ioc_feeds'
            """)
            result = cursor.fetchone()
            return result[0] if result[0] else None
        finally:
            cursor.close()
            conn.close()
    
    def should_fetch(self) -> bool:
        """Determine if it's time to fetch IOCs."""
        last_fetch = self.get_last_fetch_time()
        
        if not last_fetch:
            logger.info("no_previous_ioc_fetch_found")
            return True
        
        time_since_fetch = datetime.now() - last_fetch
        should_fetch = time_since_fetch.total_seconds() >= self.fetch_interval_seconds
        
        logger.debug("ioc_fetch_check",
                    last_fetch=last_fetch.isoformat(),
                    time_since_fetch_hours=time_since_fetch.total_seconds() / 3600,
                    should_fetch=should_fetch)
        
        return should_fetch
    
    def run_once(self) -> dict:
        """Run a single IOC fetch cycle."""
        logger.info("starting_ioc_fetch_cycle")
        
        try:
            # Get current stats before fetch
            before_stats = self.fetcher.get_stats()
            
            # Fetch IOCs from all feeds
            fetch_stats = self.fetcher.fetch_all_feeds()
            
            # Get stats after fetch
            after_stats = self.fetcher.get_stats()
            
            # Calculate changes
            changes = {
                'new_iocs': after_stats['total_iocs'] - before_stats['total_iocs'],
                'ip_changes': after_stats['by_category'].get('ip_address', 0) - 
                             before_stats['by_category'].get('ip_address', 0),
                'domain_changes': after_stats['by_category'].get('domain', 0) - 
                                before_stats['by_category'].get('domain', 0),
                'hash_changes': after_stats['by_category'].get('file_hash', 0) - 
                              before_stats['by_category'].get('file_hash', 0)
            }
            
            # Add changes to fetch stats
            fetch_stats['changes'] = changes
            fetch_stats['before_stats'] = before_stats
            fetch_stats['after_stats'] = after_stats
            
            logger.info("ioc_fetch_cycle_complete",
                       new_iocs=changes['new_iocs'],
                       total_iocs=after_stats['total_iocs'])
            
            return fetch_stats
            
        except Exception as e:
            logger.error("ioc_fetch_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, fetching IOCs periodically."""
        logger.info("starting_ioc_scheduler_daemon",
                   fetch_interval_hours=self.fetch_interval_hours)
        
        self.running = True
        
        while self.running:
            try:
                if self.should_fetch():
                    self.run_once()
                else:
                    logger.debug("skipping_ioc_fetch_not_time_yet")
                
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
    
    def print_stats(self, stats: dict):
        """Print fetch statistics in a user-friendly format."""
        print("\n" + "="*60)
        print("IOC Fetch Statistics")
        print("="*60)
        
        print(f"\nFeeds Processed: {stats['feeds_processed']}")
        print(f"Feeds Failed: {stats['feeds_failed']}")
        
        print(f"\nTotal IOCs Fetched: {stats['total_iocs_fetched']:,}")
        print(f"Total IOCs Validated: {stats['total_iocs_validated']:,}")
        print(f"Total IOCs Stored: {stats['total_iocs_stored']:,}")
        
        print("\nIOCs by Type:")
        for ioc_type, count in stats['iocs_by_type'].items():
            print(f"  {ioc_type}: {count:,}")
        
        if 'changes' in stats:
            print(f"\nNew IOCs Added: {stats['changes']['new_iocs']:,}")
            print(f"  IPs: {stats['changes']['ip_changes']:,}")
            print(f"  Domains: {stats['changes']['domain_changes']:,}")
            print(f"  Hashes: {stats['changes']['hash_changes']:,}")
        
        if 'after_stats' in stats:
            print(f"\nTotal IOCs in Database: {stats['after_stats']['total_iocs']:,}")
            print("\nTop Feed Sources:")
            for source in stats['after_stats']['top_sources'][:5]:
                print(f"  {source['source']}: {source['count']:,}")
        
        if stats.get('errors'):
            print("\nErrors:")
            for error in stats['errors']:
                print(f"  {error['feed']}: {error['error']}")
        
        print(f"\nProcessing Time: {stats.get('duration_seconds', 0):.2f} seconds")
        print("="*60 + "\n")


def main():
    """Run the IOC scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='IOC Fetcher Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    
    args = parser.parse_args()
    
    scheduler = IOCScheduler()
    
    if args.once:
        # Run once
        print("Starting IOC fetch...")
        stats = scheduler.run_once()
        scheduler.print_stats(stats)
    
    else:
        # Run as daemon
        print(f"Starting IOC scheduler daemon (interval: {scheduler.fetch_interval_hours} hours)")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()