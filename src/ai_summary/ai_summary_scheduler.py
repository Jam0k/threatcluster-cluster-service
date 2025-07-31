"""
AI Summary Scheduler - Runs AI summary generation with entity sync, linking, and description generation
"""
import asyncio
import argparse
import logging
import signal
import sys
from datetime import datetime
import time

from .ai_summary_service import AISummaryService
from .entity_sync_service import EntitySyncService
from .entity_link_service import EntityLinkService
from .entity_description_service import EntityDescriptionService
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
    
    def __init__(self, interval_seconds: int = 600, batch_size: int = 10, include_updates: bool = True):
        """
        Initialize the scheduler.
        
        Args:
            interval_seconds: Time between runs in seconds (default: 600 = 10 minutes)
            batch_size: Number of clusters to process per run
            include_updates: Whether to regenerate summaries for updated clusters
        """
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self.include_updates = include_updates
        self.service = AISummaryService()
        
        # Initialize entity services
        self.entity_sync_service = EntitySyncService()
        self.entity_link_service = EntityLinkService()
        self.entity_description_service = EntityDescriptionService()
        
        self.running = False
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.total_regenerated = 0
        self.total_new = 0
        self.entities_synced = 0
        self.entities_linked = 0
        self.descriptions_generated = 0
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        sys.exit(0)
    
    async def run_entity_maintenance(self):
        """Run entity sync, linking, and description generation for recent clusters"""
        logger.info("Running entity maintenance tasks...")
        maintenance_stats = {
            'entities_synced': 0,
            'entities_linked': 0,
            'descriptions_generated': 0
        }
        
        try:
            # 1. Sync entities from recent clusters (last 24 hours)
            logger.info("Phase 1: Syncing AI-extracted entities...")
            sync_stats = self.entity_sync_service.sync_all_recent_clusters(hours=24)
            maintenance_stats['entities_synced'] = sync_stats.get('new', 0)
            logger.info(f"  - Synced {sync_stats.get('new', 0)} new entities from {sync_stats.get('clusters_processed', 0)} clusters")
            
            # 2. Link entities to articles
            logger.info("Phase 2: Linking entities to articles...")
            link_stats = self.entity_link_service.link_all_recent_clusters(hours=24)
            maintenance_stats['entities_linked'] = link_stats.get('entities_linked', 0)
            logger.info(f"  - Linked {link_stats.get('entities_linked', 0)} entities to {link_stats.get('articles_updated', 0)} articles")
            
            # 3. Generate descriptions for new entities
            logger.info("Phase 3: Generating entity descriptions...")
            desc_stats = await self.entity_description_service.process_entities_without_descriptions(limit=50)
            maintenance_stats['descriptions_generated'] = desc_stats.get('updated', 0)
            logger.info(f"  - Generated descriptions for {desc_stats.get('updated', 0)} entities")
            
        except Exception as e:
            logger.error(f"Error during entity maintenance: {e}", exc_info=True)
        
        return maintenance_stats

    async def run_once(self):
        """Run the AI summary generation once"""
        logger.info(f"Starting AI summary generation run (batch size: {self.batch_size}, "
                   f"include updates: {self.include_updates})")
        
        run_stats = {
            'ai_summary': {},
            'entity_maintenance': {}
        }
        
        try:
            # Phase 1: Generate AI summaries
            logger.info("\n=== Phase 1: AI Summary Generation ===")
            results = await self.service.process_clusters_batch(
                limit=self.batch_size,
                include_updates=self.include_updates
            )
            
            if results:
                run_stats['ai_summary'] = results
                
                # Update totals
                self.total_processed += results['processed']
                self.total_successful += results['successful']
                self.total_failed += results['failed']
                if self.include_updates:
                    self.total_regenerated += results.get('regenerated', 0)
                    self.total_new += results.get('new', 0)
                
                logger.info(f"AI summary generation complete: {results['successful']} successful, "
                           f"{results['failed']} failed, "
                           f"{results['processing_time_seconds']:.2f} seconds")
                
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
            
            # Phase 2: Entity maintenance (sync, link, describe)
            logger.info("\n=== Phase 2: Entity Maintenance ===")
            maintenance_stats = await self.run_entity_maintenance()
            run_stats['entity_maintenance'] = maintenance_stats
            
            # Update entity stats
            self.entities_synced += maintenance_stats['entities_synced']
            self.entities_linked += maintenance_stats['entities_linked']
            self.descriptions_generated += maintenance_stats['descriptions_generated']
            
            logger.info("\n=== Run Summary ===")
            logger.info(f"AI Summaries: {run_stats['ai_summary'].get('successful', 0)} generated")
            logger.info(f"Entities Synced: {maintenance_stats['entities_synced']}")
            logger.info(f"Entities Linked: {maintenance_stats['entities_linked']}")
            logger.info(f"Descriptions Generated: {maintenance_stats['descriptions_generated']}")
            
            return run_stats
            
        except Exception as e:
            logger.error(f"Error during AI summary generation run: {e}", exc_info=True)
            return None
    
    async def run_daemon(self):
        """Run the scheduler as a daemon, processing clusters every interval"""
        logger.info(f"Starting AI Summary Scheduler daemon (interval: {self.interval_seconds}s)")
        logger.info(f"Batch size: {self.batch_size} clusters per run")
        logger.info("Entity services: sync, link, and description generation enabled")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.running = True
        run_count = 0
        
        while self.running:
            run_count += 1
            logger.info(f"\n{'='*80}")
            logger.info(f"Run #{run_count} starting at {datetime.now()}")
            logger.info("Cumulative Statistics:")
            logger.info(f"  AI Summaries: {self.total_successful} successful, "
                       f"{self.total_failed} failed, {self.total_processed} processed")
            if self.include_updates:
                logger.info(f"  - New: {self.total_new}, Regenerated: {self.total_regenerated}")
            logger.info(f"  Entities: {self.entities_synced} synced, "
                       f"{self.entities_linked} linked, "
                       f"{self.descriptions_generated} descriptions")
            
            # Run the generation
            await self.run_once()
            
            # Wait for next run
            if self.running:
                logger.info(f"\nNext run in {self.interval_seconds} seconds "
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
            'stats': {
                'total_processed': self.total_processed,
                'total_successful': self.total_successful,
                'total_failed': self.total_failed,
                'total_regenerated': self.total_regenerated,
                'total_new': self.total_new,
                'entities_synced': self.entities_synced,
                'entities_linked': self.entities_linked,
                'descriptions_generated': self.descriptions_generated
            },
            'batch_size': self.batch_size,
            'interval_seconds': self.interval_seconds,
            'include_updates': self.include_updates
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
        default=600,
        help='Interval between runs in seconds (default: 600 = 10 minutes)'
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
    parser.add_argument(
        '--entity-only',
        action='store_true',
        help='Only run entity maintenance tasks (skip AI summary generation)'
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
    
    # Run entity maintenance only if requested
    if args.entity_only:
        logger.info("Running entity maintenance only")
        await scheduler.run_entity_maintenance()
        return
    
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