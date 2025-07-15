#!/usr/bin/env python3
"""
Cluster Scheduler

Schedules periodic semantic clustering of articles with extracted entities.
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
from src.clustering.semantic_clusterer import SemanticClusterer
from src.clustering.cluster_manager import ClusterManager


logger = structlog.get_logger(__name__)


class ClusterScheduler:
    """Manages scheduled semantic clustering of articles."""
    
    def __init__(self):
        """Initialize the scheduler."""
        self.config = settings.app_config.get('pipeline', {})
        self.clustering_config = settings.app_config.get('clustering', {})
        
        self.batch_size = self.clustering_config.get('batch_size', 50)
        self.time_window_hours = self.clustering_config.get('time_window_hours', 72)
        
        self.running = False
        self.clusterer = SemanticClusterer()
        self.cluster_manager = ClusterManager()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def get_unclustered_count(self) -> int:
        """Get count of articles ready for clustering."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT COUNT(DISTINCT rfc.rss_feeds_clean_id)
                FROM cluster_data.rss_feeds_clean rfc
                WHERE rfc.rss_feeds_clean_extracted_entities IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM cluster_data.cluster_articles ca
                    JOIN cluster_data.clusters c ON ca.cluster_articles_cluster_id = c.clusters_id
                    WHERE ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                    AND c.clusters_is_active = true
                )
            """)
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()
            conn.close()
    
    def run_incremental_clustering(self) -> dict:
        """Run incremental clustering for recent articles."""
        logger.info("starting_incremental_clustering")
        
        # Use shorter time window for incremental mode
        articles = self.clusterer.get_unclustered_articles(
            time_window_hours=self.time_window_hours
        )
        
        if not articles:
            logger.info("no_articles_for_incremental_clustering")
            return {'articles_processed': 0}
        
        logger.info("articles_for_incremental_clustering", count=len(articles))
        
        # Process in batches
        all_stats = {
            'articles_processed': 0,
            'clusters_created': 0,
            'clusters_merged': 0,
            'articles_assigned': 0,
            'duplicate_clusters_prevented': 0
        }
        
        for i in range(0, len(articles), self.batch_size):
            batch = articles[i:i + self.batch_size]
            
            # Cluster the batch
            clusters, cluster_data = self.clusterer.process_batch(batch)
            
            # Store clusters with deduplication
            if cluster_data:
                stats = self.cluster_manager.process_clusters(cluster_data)
                
                # Aggregate statistics
                for key in stats:
                    all_stats[key] = all_stats.get(key, 0) + stats[key]
            
            all_stats['articles_processed'] += len(batch)
            
            logger.info("batch_completed",
                       batch_num=i//self.batch_size + 1,
                       total_batches=(len(articles) + self.batch_size - 1) // self.batch_size)
        
        return all_stats
    
    def run_full_clustering(self) -> dict:
        """Run full clustering on extended time window."""
        logger.info("starting_full_clustering")
        
        # Use extended time window for full clustering
        articles = self.clusterer.get_unclustered_articles(
            time_window_hours=self.time_window_hours * 4  # 2 weeks
        )
        
        if not articles:
            logger.info("no_articles_for_full_clustering")
            return {'articles_processed': 0}
        
        logger.info("articles_for_full_clustering", count=len(articles))
        
        # Process in larger batches for full clustering
        batch_size = self.batch_size * 2
        all_stats = {
            'articles_processed': 0,
            'clusters_created': 0,
            'clusters_merged': 0,
            'articles_assigned': 0,
            'duplicate_clusters_prevented': 0
        }
        
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            
            # Cluster the batch
            clusters, cluster_data = self.clusterer.process_batch(batch)
            
            # Store clusters with deduplication
            if cluster_data:
                stats = self.cluster_manager.process_clusters(cluster_data)
                
                # Aggregate statistics
                for key in stats:
                    all_stats[key] = all_stats.get(key, 0) + stats[key]
            
            all_stats['articles_processed'] += len(batch)
            
            logger.info("batch_completed",
                       batch_num=i//batch_size + 1,
                       total_batches=(len(articles) + batch_size - 1) // batch_size)
        
        return all_stats
    
    def run_once(self, mode: str = 'incremental') -> dict:
        """Run a single clustering cycle."""
        logger.info("starting_clustering_cycle", mode=mode)
        start_time = time.time()
        
        try:
            # Check if there are articles to cluster
            unclustered_count = self.get_unclustered_count()
            
            if unclustered_count == 0:
                logger.info("no_articles_to_cluster")
                return {'articles_processed': 0}
            
            logger.info("articles_pending_clustering", count=unclustered_count)
            
            # Run clustering based on mode
            if mode == 'full':
                stats = self.run_full_clustering()
            else:
                stats = self.run_incremental_clustering()
            
            # Calculate processing time
            processing_time = time.time() - start_time
            stats['processing_time_seconds'] = round(processing_time, 2)
            
            # Add clusterer statistics
            stats.update(self.clusterer.stats)
            
            logger.info("clustering_cycle_completed", **stats)
            return stats
            
        except Exception as e:
            logger.error("clustering_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, clustering articles periodically."""
        logger.info("starting_cluster_daemon",
                   check_interval_minutes=30)
        
        self.running = True
        
        while self.running:
            try:
                # Check for unclustered articles
                unclustered_count = self.get_unclustered_count()
                
                if unclustered_count > 0:
                    logger.info("unclustered_articles_found", count=unclustered_count)
                    
                    # Run incremental clustering
                    self.run_once(mode='incremental')
                else:
                    logger.debug("no_unclustered_articles")
                
                # Wait before next check (30 minutes)
                for _ in range(30):
                    if not self.running:
                        break
                    time.sleep(60)
                    
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                # Continue running after errors
                time.sleep(300)  # Wait 5 minutes after error


def main():
    """Run the cluster scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Semantic Clustering Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    parser.add_argument('--full', action='store_true',
                       help='Run full clustering on extended time window')
    parser.add_argument('--limit', type=int,
                       help='Limit number of articles to process')
    
    args = parser.parse_args()
    
    scheduler = ClusterScheduler()
    
    if args.limit:
        # Override batch size for this run
        scheduler.clusterer.batch_size = min(args.limit, scheduler.batch_size)
    
    if args.once:
        # Run once
        mode = 'full' if args.full else 'incremental'
        stats = scheduler.run_once(mode=mode)
        
        print("\nClustering completed!")
        print(f"Articles processed: {stats.get('articles_processed', 0)}")
        print(f"Articles added to existing clusters: {stats.get('articles_added_to_existing', 0)}")
        print(f"New clusters created: {stats.get('clusters_created', 0)}")
        print(f"Clusters merged: {stats.get('clusters_merged', 0)}")
        print(f"Total articles assigned: {stats.get('articles_assigned', 0)}")
        print(f"Duplicate clusters prevented: {stats.get('duplicate_clusters_prevented', 0)}")
        print(f"Processing time: {stats.get('processing_time_seconds', 0):.2f} seconds")
    else:
        # Run as daemon
        print("Starting cluster scheduler daemon")
        print("Checking for new articles every 30 minutes...")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()