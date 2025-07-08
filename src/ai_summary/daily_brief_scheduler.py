#!/usr/bin/env python3
"""
Daily Brief Scheduler - Generates daily AI threat intelligence summaries
Runs once per day to create a comprehensive threat brief
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, time, timedelta
import pytz

from src.config.settings import settings
from .daily_brief_service import DailyBriefService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"Received signal {signum}. Requesting shutdown...")
    shutdown_requested = True


async def run_daily_brief_generation():
    """Run a single daily brief generation"""
    try:
        service = DailyBriefService()
        result = await service.generate_daily_brief()
        
        if result['status'] == 'success':
            logger.info(f"Successfully generated daily brief: {result}")
        elif result['status'] == 'exists':
            logger.info(f"Daily brief already exists for {result['date']}")
        elif result['status'] == 'no_data':
            logger.warning(f"No threat data available for {result['date']}")
        else:
            logger.error(f"Unexpected result: {result}")
            
    except Exception as e:
        logger.error(f"Error in daily brief generation: {e}", exc_info=True)


async def calculate_next_run_time(hour: int = 6, minute: int = 0) -> float:
    """
    Calculate seconds until next run time (default: 6:00 AM UTC)
    
    Args:
        hour: Hour to run (0-23)
        minute: Minute to run (0-59)
        
    Returns:
        Seconds until next run
    """
    now = datetime.now(pytz.UTC)
    
    # Calculate today's run time
    today_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # If we've already passed today's run time, schedule for tomorrow
    if now >= today_run:
        next_run = today_run + timedelta(days=1)
    else:
        next_run = today_run
    
    # Calculate seconds until next run
    delta = next_run - now
    return delta.total_seconds()


async def run_scheduler(run_once: bool = False):
    """
    Run the daily brief scheduler
    
    Args:
        run_once: If True, run once and exit. If False, run daily.
    """
    logger.info("Starting daily brief scheduler...")
    
    if run_once:
        logger.info("Running in one-time mode")
        await run_daily_brief_generation()
        return
    
    # Run continuously
    while not shutdown_requested:
        try:
            # Calculate time until next run (6 AM UTC)
            seconds_until_run = await calculate_next_run_time(hour=6, minute=0)
            
            logger.info(f"Next daily brief generation in {seconds_until_run/3600:.1f} hours")
            
            # Wait until next run time or shutdown
            wait_time = 0
            while wait_time < seconds_until_run and not shutdown_requested:
                await asyncio.sleep(min(60, seconds_until_run - wait_time))  # Check every minute
                wait_time += 60
            
            if shutdown_requested:
                break
            
            # Run the generation
            logger.info("Starting scheduled daily brief generation...")
            await run_daily_brief_generation()
            
            # Add a small delay to prevent double-runs
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}", exc_info=True)
            await asyncio.sleep(300)  # Wait 5 minutes before retrying


def main():
    """Main entry point"""
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Daily Brief Scheduler')
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (for testing or cron jobs)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force regeneration even if brief exists for today'
    )
    args = parser.parse_args()
    
    try:
        # Run the scheduler
        asyncio.run(run_scheduler(run_once=args.once))
        logger.info("Daily brief scheduler stopped.")
    except KeyboardInterrupt:
        logger.info("Daily brief scheduler interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error in daily brief scheduler: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()