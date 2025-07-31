#!/usr/bin/env python3
"""
Entity Description Scheduler - Periodic generation of descriptions for entities
"""
import logging
import argparse
import time
import asyncio
from datetime import datetime

from src.ai_summary.entity_description_service import EntityDescriptionService

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_description_generation(limit: int = 50):
    """
    Run description generation for entities without descriptions.
    
    Args:
        limit: Maximum number of entities to process
    """
    logger.info(f"Starting description generation for up to {limit} entities")
    
    try:
        service = EntityDescriptionService()
        
        # Generate descriptions for entities without them
        stats = await service.process_entities_without_descriptions(limit=limit)
        
        logger.info(f"Description generation completed: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error during description generation: {e}")
        return None


async def run_scheduler_async(interval_minutes: int = 30, batch_size: int = 50):
    """
    Run description generation on a schedule.
    
    Args:
        interval_minutes: Minutes between generation runs
        batch_size: Number of entities to process per run
    """
    logger.info(f"Starting description scheduler - running every {interval_minutes} minutes")
    logger.info(f"Processing up to {batch_size} entities per run")
    
    while True:
        try:
            # Run generation
            start_time = datetime.now()
            stats = await run_description_generation(limit=batch_size)
            
            if stats:
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Generation completed in {duration:.1f} seconds")
            
            # Wait for next run
            logger.info(f"Sleeping for {interval_minutes} minutes until next run...")
            await asyncio.sleep(interval_minutes * 60)
            
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in scheduler: {e}")
            logger.info(f"Retrying in {interval_minutes} minutes...")
            await asyncio.sleep(interval_minutes * 60)


def run_scheduler(interval_minutes: int = 30, batch_size: int = 50):
    """Synchronous wrapper for the async scheduler"""
    asyncio.run(run_scheduler_async(interval_minutes, batch_size))


async def main():
    """Main function to run entity description scheduler"""
    parser = argparse.ArgumentParser(description='Entity Description Scheduler')
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit instead of continuous scheduling'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='Minutes between generation runs (default: 30)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Number of entities to process per run (default: 50)'
    )
    
    args = parser.parse_args()
    
    if args.once:
        # Run once and exit
        logger.info("Running description generation once")
        stats = await run_description_generation(limit=args.batch_size)
        if stats:
            logger.info("Description generation completed successfully")
        else:
            logger.error("Description generation failed")
    else:
        # Run on schedule
        await run_scheduler_async(
            interval_minutes=args.interval,
            batch_size=args.batch_size
        )


if __name__ == "__main__":
    asyncio.run(main())