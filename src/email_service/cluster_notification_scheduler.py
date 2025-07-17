"""
Scheduler for cluster notification emails
Runs periodically to check for cluster updates and send notifications
"""

import logging
import asyncio
import argparse
from datetime import datetime
import signal
import sys

from src.config.settings import settings
from src.email_service.cluster_notification_service import ClusterNotificationService

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = False


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_flag
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    shutdown_flag = True


async def run_notification_check():
    """Run the cluster notification check"""
    logger.info("Starting cluster notification check...")
    
    try:
        service = ClusterNotificationService()
        await service.check_all_clusters_for_updates()
        logger.info("Cluster notification check completed successfully")
    except Exception as e:
        logger.error(f"Error during notification check: {e}", exc_info=True)
        raise


async def run_scheduler(interval_minutes: int = 15):
    """Run the notification service on a schedule"""
    logger.info(f"Starting cluster notification scheduler (interval: {interval_minutes} minutes)")
    
    while not shutdown_flag:
        try:
            start_time = datetime.now()
            await run_notification_check()
            
            # Calculate time to next run
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, interval_minutes * 60 - elapsed)
            
            if sleep_time > 0 and not shutdown_flag:
                logger.info(f"Sleeping for {sleep_time:.0f} seconds until next check...")
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"Unexpected error in scheduler: {e}", exc_info=True)
            if not shutdown_flag:
                # Wait before retrying
                await asyncio.sleep(60)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="ThreatCluster Cluster Notification Scheduler"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (don't start scheduler)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=15,
        help="Interval between checks in minutes (default: 15)"
    )
    
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.once:
            # Run once and exit
            asyncio.run(run_notification_check())
        else:
            # Run as daemon
            asyncio.run(run_scheduler(args.interval))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()