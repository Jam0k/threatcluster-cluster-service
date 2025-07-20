"""
AI Summary Scheduler - Runs hourly to process clusters without AI summaries
"""
import asyncio
import argparse
import logging
import signal
import sys
from datetime import datetime
import time

from .ai_summary_service import AISummaryService
from src.config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/ai_summary_scheduler_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger(__name__)


class AISummaryScheduler:
    """Scheduler for running AI summary generation periodically"""
    
    def __init__(self, interval_seconds: int = 3600, batch_size: int = 10, include_updates: bool = True):
        """
        Initialize the scheduler.
        
        Args:
            interval_seconds: Time between runs in seconds (default: 3600 = 1 hour)
            batch_size: Number of clusters to process per run
            include_updates: Whether to regenerate summaries for updated clusters
        """
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self.include_updates = include_updates
        self.service = AISummaryService()
        self.running = False
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.total_regenerated = 0
        self.total_new = 0
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        sys.exit(0)
    
    async def run_once(self):
        """Run the AI summary generation once"""
        logger.info(f"Starting AI summary generation run (batch size: {self.batch_size}, "
                   f"include updates: {self.include_updates})")
        
        try:
            results = await self.service.process_clusters_batch(
                limit=self.batch_size,
                include_updates=self.include_updates
            )
            
            # Update totals
            self.total_processed += results['processed']
            self.total_successful += results['successful']
            self.total_failed += results['failed']
            if self.include_updates:
                self.total_regenerated += results.get('regenerated', 0)
                self.total_new += results.get('new', 0)
            
            logger.info(f"Run complete: {results['successful']} successful, "
                       f"{results['failed']} failed, "
                       f"{results['processing_time_seconds']:.2f} seconds")
            
            if self.include_updates:
                logger.info(f"  - New summaries: {results.get('new', 0)}")
                logger.info(f"  - Regenerated: {results.get('regenerated', 0)}")
            
            # Log details for each cluster
            for cluster_result in results['clusters']:
                reason = cluster_result.get('reason', 'new')
                if cluster_result['status'] == 'success':
                    logger.info(f"✓ Cluster {cluster_result['cluster_id']}: "
                               f"{cluster_result['cluster_name']} ({reason})")
                else:
                    logger.error(f"✗ Cluster {cluster_result['cluster_id']}: "
                                f"{cluster_result['cluster_name']} ({reason}) - "
                                f"{cluster_result.get('error', 'Unknown error')}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error during AI summary generation run: {e}", exc_info=True)
            return None
    
    async def run_daemon(self):
        """Run the scheduler as a daemon, processing clusters every interval"""
        logger.info(f"Starting AI Summary Scheduler daemon (interval: {self.interval_seconds}s)")
        logger.info(f"Batch size: {self.batch_size} clusters per run")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.running = True
        run_count = 0
        
        while self.running:
            run_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Run #{run_count} starting at {datetime.now()}")
            logger.info(f"Total stats: {self.total_successful} successful, "
                       f"{self.total_failed} failed, {self.total_processed} processed")
            if self.include_updates:
                logger.info(f"  - New: {self.total_new}, Regenerated: {self.total_regenerated}")
            
            # Run the generation
            await self.run_once()
            
            # Wait for next run
            if self.running:
                logger.info(f"Next run in {self.interval_seconds} seconds "
                           f"({self.interval_seconds/60:.1f} minutes)")
                
                # Use asyncio.sleep in chunks to allow for graceful shutdown
                sleep_chunks = min(self.interval_seconds, 10)  # Check every 10 seconds
                for _ in range(0, self.interval_seconds, sleep_chunks):
                    if not self.running:
                        break
                    await asyncio.sleep(min(sleep_chunks, self.interval_seconds - _))
        
        logger.info("AI Summary Scheduler daemon stopped")
    
    def get_status(self) -> dict:
        """Get current scheduler status"""
        return {
            'running': self.running,
            'total_processed': self.total_processed,
            'total_successful': self.total_successful,
            'total_failed': self.total_failed,
            'batch_size': self.batch_size,
            'interval_seconds': self.interval_seconds
        }


async def main():
    """Main entry point for the scheduler"""
    parser = argparse.ArgumentParser(
        description='AI Summary Scheduler - Generate AI summaries for security clusters'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit instead of running as daemon'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=3600,
        help='Interval between runs in seconds (default: 3600 = 1 hour)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of clusters to process per run (default: 10)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode with batch size of 1'
    )
    parser.add_argument(
        '--no-updates',
        action='store_true',
        help='Only process new clusters, skip regeneration of updated clusters'
    )
    
    args = parser.parse_args()
    
    # Override batch size for test mode
    if args.test:
        args.batch_size = 1
        logger.info("Running in test mode (batch size = 1)")
    
    # Create scheduler
    scheduler = AISummaryScheduler(
        interval_seconds=args.interval,
        batch_size=args.batch_size,
        include_updates=not args.no_updates
    )
    
    # Run once or as daemon
    if args.once:
        logger.info("Running AI summary generation once")
        await scheduler.run_once()
    else:
        logger.info("Starting AI summary scheduler daemon")
        await scheduler.run_daemon()


def run_scheduler():
    """Entry point for running the scheduler"""
    asyncio.run(main())


if __name__ == "__main__":
    run_scheduler()