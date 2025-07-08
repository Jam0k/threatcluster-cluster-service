"""
Daily Email Scheduler for ThreatCluster
Runs daily to send threat intelligence bulletins to subscribed users
"""

import logging
import asyncio
import sys
from datetime import datetime, time
import signal
from typing import Optional

from src.config.settings import settings
from src.email_service.email_service import EmailService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'logs/daily_email_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger(__name__)


class DailyEmailScheduler:
    """Scheduler for daily email bulletins"""
    
    def __init__(self):
        self.service = EmailService()
        self.is_running = False
        self.send_time = time(9, 0)  # Default: 9:00 AM UTC
        
        # Get send time from settings if available
        if hasattr(settings, 'daily_email_send_time'):
            try:
                hour, minute = map(int, settings.daily_email_send_time.split(':'))
                self.send_time = time(hour, minute)
            except:
                logger.warning("Invalid daily_email_send_time format, using default 9:00 AM UTC")
    
    async def run_once(self):
        """Run the email service once"""
        logger.info("Running daily email bulletin service once")
        try:
            await self.service.send_daily_bulletins()
            logger.info("Daily email bulletin service completed successfully")
        except Exception as e:
            logger.error(f"Error running daily email service: {e}", exc_info=True)
    
    async def run_daemon(self):
        """Run as a daemon, sending emails daily at specified time"""
        self.is_running = True
        logger.info(f"Starting daily email scheduler daemon - will send at {self.send_time} UTC daily")
        
        while self.is_running:
            try:
                # Calculate time until next send
                now = datetime.utcnow()
                next_send = datetime.combine(now.date(), self.send_time)
                
                # If we've already passed today's send time, schedule for tomorrow
                if now.time() > self.send_time:
                    next_send = datetime.combine(
                        now.date() + timedelta(days=1), 
                        self.send_time
                    )
                
                wait_seconds = (next_send - now).total_seconds()
                logger.info(f"Next email bulletin scheduled for {next_send} UTC (in {wait_seconds/3600:.1f} hours)")
                
                # Wait until send time
                await asyncio.sleep(wait_seconds)
                
                # Send emails
                logger.info("Starting daily email bulletin send")
                await self.run_once()
                
                # Wait a bit to avoid immediate re-runs
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                logger.info("Daily email scheduler daemon cancelled")
                break
            except Exception as e:
                logger.error(f"Error in daily email scheduler daemon: {e}", exc_info=True)
                # Wait before retrying
                await asyncio.sleep(300)  # 5 minutes
    
    def stop(self):
        """Stop the daemon"""
        logger.info("Stopping daily email scheduler daemon")
        self.is_running = False


async def main():
    """Main entry point"""
    import argparse
    from datetime import timedelta
    
    parser = argparse.ArgumentParser(description='Daily Email Scheduler for ThreatCluster')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--test', action='store_true', help='Run in test mode (send immediately)')
    args = parser.parse_args()
    
    scheduler = DailyEmailScheduler()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.once or args.test:
            # Run once and exit
            await scheduler.run_once()
        else:
            # Run as daemon
            await scheduler.run_daemon()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())