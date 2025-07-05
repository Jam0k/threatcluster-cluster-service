#!/usr/bin/env python3
"""
MISP Scheduler

Schedules periodic MISP feed fetching.
Can be run as a daemon or triggered manually.
"""
import time
import signal
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional
import structlog
import psycopg2

from src.config.settings import settings
from src.misp.misp_fetcher import MISPFetcher

logger = structlog.get_logger(__name__)


class MISPScheduler:
    """Manages scheduled MISP feed fetching."""
    
    def __init__(self):
        """Initialize the scheduler."""
        # Default to 24-hour interval
        self.fetch_interval_hours = 24
        self.fetch_interval_seconds = self.fetch_interval_hours * 3600
        self.running = False
        self.fetcher = MISPFetcher()
        
        # Load interval from config if available
        config = self.fetcher.config.get('config', {})
        self.fetch_interval_hours = config.get('fetch_interval_hours', 24)
        self.fetch_interval_seconds = self.fetch_interval_hours * 3600
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_last_fetch_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful MISP fetch."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            # Check when MISP entities were last added/updated
            cursor.execute("""
                SELECT MAX(entities_added_on)
                FROM cluster_data.entities
                WHERE entities_source = 'misp'
            """)
            result = cursor.fetchone()
            return result[0] if result[0] else None
        finally:
            cursor.close()
            conn.close()
    
    def should_fetch(self) -> bool:
        """Determine if it's time to fetch MISP feeds."""
        last_fetch = self.get_last_fetch_time()
        
        if not last_fetch:
            logger.info("no_previous_misp_fetch_found")
            return True
        
        time_since_fetch = datetime.now() - last_fetch
        should_fetch = time_since_fetch.total_seconds() >= self.fetch_interval_seconds
        
        logger.debug("misp_fetch_check",
                    last_fetch=last_fetch.isoformat(),
                    time_since_fetch_hours=time_since_fetch.total_seconds() / 3600,
                    should_fetch=should_fetch)
        
        return should_fetch
    
    def run_once(self) -> dict:
        """Run a single MISP fetch cycle."""
        logger.info("starting_misp_fetch_cycle")
        
        try:
            stats = self.fetcher.fetch_all_feeds()
            
            # Log summary
            logger.info("misp_fetch_complete",
                       feeds_processed=stats['feeds_processed'],
                       entities_inserted=stats['entities_inserted'],
                       entities_updated=stats['entities_updated'],
                       duration_seconds=stats['duration_seconds'])
            
            # Show entity counts
            counts = self.fetcher.get_entity_count()
            logger.info("misp_entity_counts", **counts)
            
            return stats
            
        except Exception as e:
            logger.error("misp_fetch_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, fetching MISP feeds periodically."""
        logger.info("starting_misp_scheduler_daemon",
                   fetch_interval_hours=self.fetch_interval_hours)
        
        self.running = True
        
        while self.running:
            try:
                if self.should_fetch():
                    self.run_once()
                else:
                    logger.debug("misp_fetch_not_due")
                
                # Sleep for 1 hour and check again
                logger.debug("sleeping_until_next_check")
                time.sleep(3600)  # 1 hour
                
            except Exception as e:
                logger.error("misp_daemon_error", error=str(e))
                # Sleep for 5 minutes on error
                time.sleep(300)


def main():
    """Main entry point for MISP scheduler."""
    parser = argparse.ArgumentParser(
        description='MISP Feed Scheduler - Fetches threat actor data from MISP galaxy'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (no daemon mode)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force fetch regardless of last fetch time'
    )
    
    args = parser.parse_args()
    
    # Configure structured logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    scheduler = MISPScheduler()
    
    try:
        if args.once:
            # Run once mode
            if args.force or scheduler.should_fetch():
                logger.info("running_misp_fetch_once")
                stats = scheduler.run_once()
                
                # Print summary
                print("\n" + "=" * 60)
                print("MISP Fetch Summary")
                print("=" * 60)
                print(f"Feeds processed: {stats['feeds_processed']}")
                print(f"Feeds failed: {stats['feeds_failed']}")
                print(f"Entities fetched: {stats['entities_fetched']}")
                print(f"Entities inserted: {stats['entities_inserted']}")
                print(f"Entities updated: {stats['entities_updated']}")
                print(f"Entities skipped: {stats['entities_skipped']}")
                print(f"Duration: {stats['duration_seconds']:.2f} seconds")
                
                if stats['errors']:
                    print(f"\nErrors: {len(stats['errors'])}")
                    for error in stats['errors']:
                        print(f"  - {error}")
                
                print("=" * 60)
            else:
                logger.info("misp_fetch_not_due")
                print("MISP fetch not due yet. Use --force to override.")
        else:
            # Daemon mode
            logger.info("starting_misp_daemon")
            scheduler.run_daemon()
            
    except KeyboardInterrupt:
        logger.info("interrupted_by_user")
        sys.exit(0)
    except Exception as e:
        logger.error("misp_scheduler_error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()