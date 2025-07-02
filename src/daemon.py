#!/usr/bin/env python3
"""
ThreatCluster Daemon

Non-interactive daemon for running ThreatCluster continuous processing in the background.
Designed to run as a systemd service or standalone background process.

Usage:
    python -m src.daemon [--debug] [--once]
    
Options:
    --debug     Enable debug mode with verbose logging
    --once      Run pipeline once and exit (for testing)
"""
import os
import sys
import time
import signal
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import structlog

from src.config.settings import settings
from src.feeds.rss_fetcher import RSSFeedFetcher
from src.scraper.article_scraper import ArticleScraper
from src.entity_extraction.entity_extractor import EntityExtractor
from src.clustering.semantic_clusterer import SemanticClusterer
from src.clustering.cluster_manager import ClusterManager
from src.ranking.article_ranker import ArticleRanker


class ThreatClusterDaemon:
    """ThreatCluster background daemon for continuous processing."""
    
    def __init__(self, debug=False):
        """Initialize the daemon."""
        self.debug = debug
        self.running = False
        self.cycle_count = 0
        
        # Set up logging
        self.setup_logging()
        
        # Initialize components
        self.components = {
            'fetcher': RSSFeedFetcher(),
            'scraper': ArticleScraper(),
            'extractor': EntityExtractor(),
            'clusterer': SemanticClusterer(),
            'cluster_manager': ClusterManager(),
            'ranker': ArticleRanker()
        }
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        
        self.logger.info("daemon_initialized", debug=debug, pid=os.getpid())
    
    def setup_logging(self):
        """Set up file-based logging for daemon mode."""
        # Create logs directory
        log_dir = Path(settings.base_dir) / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Set up daily log file
        log_file = log_dir / f"threatcluster_daemon_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Configure file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG if self.debug else logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
        root_logger.addHandler(file_handler)
        
        # Suppress transformer warnings unless in debug mode
        if not self.debug:
            os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
            logging.getLogger('transformers').setLevel(logging.ERROR)
        
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
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        
        self.logger = structlog.get_logger(__name__)
        self.logger.info("logging_configured", log_file=str(log_file))
    
    def shutdown_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info("shutdown_signal_received", signal=signum)
        self.running = False
    
    def run_component(self, component_name, description):
        """Run a single component and return results."""
        self.logger.info("component_starting", component=component_name, description=description)
        start_time = time.time()
        
        try:
            if component_name == 'fetcher':
                result = self.components['fetcher'].process_all_feeds()
                self.logger.info("fetcher_completed", 
                               feeds_processed=result['feeds_processed'],
                               articles_stored=result.get('articles_stored', 0),
                               articles_filtered=result.get('articles_filtered', 0))
                
            elif component_name == 'scraper':
                result = self.components['scraper'].process_batch(limit=50)
                self.logger.info("scraper_completed",
                               articles_attempted=result.get('articles_attempted', 0),
                               articles_success=result.get('articles_success', 0),
                               articles_failed=result.get('articles_failed', 0))
                
            elif component_name == 'extractor':
                result = self.components['extractor'].process_batch(limit=50)
                self.logger.info("extractor_completed",
                               articles_processed=result.get('articles_processed', 0),
                               entities_extracted=result.get('entities_extracted', 0),
                               new_entities=result.get('new_entities_discovered', 0))
                
            elif component_name == 'clusterer':
                # Get unclustered articles
                articles = self.components['clusterer'].get_unclustered_articles(time_window_hours=72)
                if articles:
                    _, cluster_data = self.components['clusterer'].process_batch(articles)
                    
                    # Store clusters in database
                    if cluster_data:
                        stats = self.components['cluster_manager'].process_clusters(cluster_data)
                        result = {
                            'articles_processed': len(articles),
                            'clusters_created': stats.get('clusters_created', 0),
                            'articles_clustered': stats.get('articles_assigned', 0)
                        }
                    else:
                        result = {'articles_processed': len(articles), 'clusters_created': 0}
                else:
                    result = {'articles_processed': 0, 'clusters_created': 0}
                
                self.logger.info("clusterer_completed",
                               articles_processed=result.get('articles_processed', 0),
                               clusters_created=result.get('clusters_created', 0))
                
            elif component_name == 'ranker':
                result = self.components['ranker'].rank_articles(time_window_hours=72)
                self.logger.info("ranker_completed",
                               articles_ranked=result.get('articles_ranked', 0),
                               avg_score=result.get('avg_score', 0))
            
            elapsed = time.time() - start_time
            self.logger.info("component_completed", 
                           component=component_name, 
                           duration=elapsed,
                           success=True)
            return True
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error("component_failed", 
                            component=component_name,
                            duration=elapsed,
                            error=str(e),
                            traceback=traceback.format_exc())
            return False
    
    def run_full_pipeline(self):
        """Run the complete pipeline once."""
        self.cycle_count += 1
        pipeline_start = time.time()
        
        self.logger.info("pipeline_starting", cycle=self.cycle_count)
        
        # Pipeline components in order
        components = [
            ('fetcher', 'RSS Feed Fetcher'),
            ('scraper', 'Article Scraper'),
            ('extractor', 'Entity Extractor'),
            ('clusterer', 'Semantic Clusterer'),
            ('ranker', 'Article Ranker')
        ]
        
        success_count = 0
        error_count = 0
        
        for i, (component_name, description) in enumerate(components, 1):
            self.logger.info("pipeline_step", step=i, total=len(components), component=component_name)
            
            if self.run_component(component_name, description):
                success_count += 1
            else:
                error_count += 1
            
            # Wait after fetching to avoid rate limiting
            if component_name == 'fetcher' and i < len(components):
                wait_seconds = 30
                self.logger.info("pipeline_waiting", reason="rate_limiting", seconds=wait_seconds)
                time.sleep(wait_seconds)
        
        pipeline_elapsed = time.time() - pipeline_start
        
        self.logger.info("pipeline_completed", 
                        cycle=self.cycle_count,
                        duration=pipeline_elapsed,
                        success_count=success_count,
                        error_count=error_count)
        
        return success_count, error_count
    
    def run_once(self):
        """Run pipeline once and exit (for testing)."""
        self.logger.info("daemon_mode", mode="once")
        success_count, error_count = self.run_full_pipeline()
        
        if error_count > 0:
            self.logger.error("pipeline_had_errors", errors=error_count)
            return 1
        else:
            self.logger.info("pipeline_successful")
            return 0
    
    def run_continuous(self):
        """Run continuous processing."""
        self.logger.info("daemon_mode", mode="continuous")
        self.running = True
        
        # Get configuration
        wait_minutes = settings.app_config.get('scheduler', {}).get('components', {}).get('rss_fetcher', {}).get('interval_minutes', 60)
        
        self.logger.info("continuous_processing_started", interval_minutes=wait_minutes)
        
        while self.running:
            try:
                # Run pipeline
                success_count, error_count = self.run_full_pipeline()
                
                if not self.running:
                    break
                
                # Calculate next run time
                next_run = datetime.now() + timedelta(minutes=wait_minutes)
                self.logger.info("cycle_completed", 
                               cycle=self.cycle_count,
                               next_run=next_run.isoformat(),
                               wait_minutes=wait_minutes)
                
                # Wait for next cycle, checking every minute if we should stop
                for minute in range(wait_minutes):
                    if not self.running:
                        self.logger.info("shutdown_requested_during_wait")
                        break
                    time.sleep(60)
                    
            except Exception as e:
                self.logger.error("continuous_processing_error", 
                                error=str(e),
                                traceback=traceback.format_exc())
                # Wait 5 minutes on error before retrying
                self.logger.info("error_recovery_wait", minutes=5)
                time.sleep(300)
        
        self.logger.info("continuous_processing_stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ThreatCluster Background Daemon')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with verbose logging')
    parser.add_argument('--once', action='store_true',
                       help='Run pipeline once and exit (for testing)')
    args = parser.parse_args()
    
    try:
        daemon = ThreatClusterDaemon(debug=args.debug)
        
        if args.once:
            return daemon.run_once()
        else:
            daemon.run_continuous()
            return 0
            
    except Exception as e:
        # Emergency logging to stderr if structured logging fails
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())