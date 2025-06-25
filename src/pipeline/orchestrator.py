#!/usr/bin/env python3
"""
Pipeline Orchestrator

Manages the execution of all components in the correct order with dependency handling.
"""
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
import structlog
import psycopg2

from src.config.settings import settings
from src.feeds.rss_fetcher import RSSFeedFetcher
from src.scraper.article_scraper import ArticleScraper
from src.entity_extraction.entity_extractor import EntityExtractor
from src.clustering.semantic_clusterer import SemanticClusterer
from src.ranking.article_ranker import ArticleRanker

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """Orchestrates the execution of all pipeline components."""
    
    def __init__(self):
        """Initialize the orchestrator."""
        self.scheduler_config = settings.app_config.get('scheduler', {})
        self.components_config = self.scheduler_config.get('components', {})
        
        # Initialize components
        self.components = {
            'rss_fetcher': {
                'instance': RSSFeedFetcher(),
                'method': 'process_all_feeds',
                'dependencies': [],
                'last_run': None
            },
            'article_scraper': {
                'instance': ArticleScraper(),
                'method': 'process_batch',
                'dependencies': ['rss_fetcher'],
                'last_run': None
            },
            'entity_extractor': {
                'instance': EntityExtractor(),
                'method': 'process_unprocessed_articles',
                'dependencies': ['article_scraper'],
                'last_run': None
            },
            'semantic_clusterer': {
                'instance': SemanticClusterer(),
                'method': 'cluster_articles',
                'dependencies': ['entity_extractor'],
                'last_run': None
            },
            'article_ranker': {
                'instance': ArticleRanker(),
                'method': 'rank_articles',
                'dependencies': ['semantic_clusterer'],
                'last_run': None
            }
        }
        
        self.running = False
        self.execution_stats = {}
    
    def check_dependencies(self, component_name: str) -> bool:
        """Check if all dependencies for a component have been satisfied."""
        component = self.components.get(component_name)
        if not component:
            return False
        
        for dep in component['dependencies']:
            dep_component = self.components.get(dep)
            if not dep_component or not dep_component['last_run']:
                return False
            
            # Check if dependency was run recently enough
            dep_config = self.components_config.get(dep, {})
            if dep_config.get('enabled', True):
                interval = dep_config.get('interval_minutes', 60)
                if datetime.now() - dep_component['last_run'] > timedelta(minutes=interval * 2):
                    return False
        
        return True
    
    def run_component(self, component_name: str, force: bool = False) -> Dict:
        """Run a single component."""
        component = self.components.get(component_name)
        if not component:
            raise ValueError(f"Unknown component: {component_name}")
        
        config = self.components_config.get(component_name, {})
        if not config.get('enabled', True) and not force:
            logger.info("component_disabled", component=component_name)
            return {'status': 'disabled', 'component': component_name}
        
        # Check dependencies unless forced
        if not force and not self.check_dependencies(component_name):
            logger.warning("dependencies_not_met", component=component_name)
            return {'status': 'dependencies_not_met', 'component': component_name}
        
        logger.info("running_component", component=component_name)
        start_time = time.time()
        
        try:
            instance = component['instance']
            method_name = component['method']
            method = getattr(instance, method_name)
            
            # Special handling for components that need specific arguments
            if component_name == 'article_scraper':
                result = method(limit=100)  # Process 100 articles at a time
            elif component_name == 'semantic_clusterer':
                time_window = settings.app_config.get('clustering', {}).get('time_window_hours', 72)
                result = method(time_window_hours=time_window)
            elif component_name == 'article_ranker':
                time_window = settings.app_config.get('pipeline', {}).get('time_window_hours', 72)
                result = method(time_window_hours=time_window)
            else:
                result = method()
            
            component['last_run'] = datetime.now()
            
            elapsed = time.time() - start_time
            logger.info("component_completed", 
                       component=component_name, 
                       elapsed_seconds=elapsed,
                       result=result)
            
            return {
                'status': 'success',
                'component': component_name,
                'elapsed_seconds': elapsed,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error("component_failed", 
                        component=component_name, 
                        error=str(e),
                        elapsed_seconds=elapsed)
            
            return {
                'status': 'error',
                'component': component_name,
                'error': str(e),
                'elapsed_seconds': elapsed,
                'timestamp': datetime.now().isoformat()
            }
    
    def run_pipeline(self, components: Optional[List[str]] = None) -> Dict:
        """Run the complete pipeline or specified components."""
        if components is None:
            components = list(self.components.keys())
        
        results = {}
        start_time = time.time()
        
        logger.info("pipeline_started", components=components)
        
        for component_name in components:
            if component_name not in self.components:
                logger.warning("unknown_component", component=component_name)
                continue
            
            result = self.run_component(component_name)
            results[component_name] = result
            
            # Add delay after RSS fetcher if scraper is next
            if component_name == 'rss_fetcher' and 'article_scraper' in components:
                delay = self.components_config.get('article_scraper', {}).get('delay_after_fetch_minutes', 5)
                if delay > 0:
                    logger.info("delaying_before_scraper", delay_minutes=delay)
                    time.sleep(delay * 60)
        
        elapsed = time.time() - start_time
        logger.info("pipeline_completed", 
                   elapsed_seconds=elapsed,
                   components_run=len(results))
        
        return {
            'status': 'completed',
            'elapsed_seconds': elapsed,
            'components': results,
            'timestamp': datetime.now().isoformat()
        }
    
    async def run_daemon(self):
        """Run the orchestrator as a daemon, scheduling components based on configuration."""
        logger.info("daemon_started")
        self.running = True
        
        while self.running:
            try:
                current_time = datetime.now()
                
                for component_name, component in self.components.items():
                    config = self.components_config.get(component_name, {})
                    
                    if not config.get('enabled', True):
                        continue
                    
                    interval_minutes = config.get('interval_minutes', 60)
                    last_run = component.get('last_run')
                    
                    # Check if it's time to run this component
                    should_run = False
                    if last_run is None:
                        should_run = True
                    else:
                        time_since_last = (current_time - last_run).total_seconds() / 60
                        if time_since_last >= interval_minutes:
                            should_run = True
                    
                    if should_run and self.check_dependencies(component_name):
                        logger.info("daemon_running_component", component=component_name)
                        self.run_component(component_name)
                
                # Sleep for a minute before checking again
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                await asyncio.sleep(60)
        
        logger.info("daemon_stopped")
    
    def stop_daemon(self):
        """Stop the daemon gracefully."""
        logger.info("stopping_daemon")
        self.running = False
    
    def get_status(self) -> Dict:
        """Get the current status of all components."""
        status = {
            'orchestrator': {
                'running': self.running,
                'scheduler_enabled': self.scheduler_config.get('enabled', True)
            },
            'components': {}
        }
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            for component_name, component in self.components.items():
                config = self.components_config.get(component_name, {})
                
                component_status = {
                    'enabled': config.get('enabled', True),
                    'interval_minutes': config.get('interval_minutes', 60),
                    'last_run': component['last_run'].isoformat() if component['last_run'] else None,
                    'description': config.get('description', ''),
                    'dependencies_met': self.check_dependencies(component_name)
                }
                
                # Add component-specific metrics
                if component_name == 'rss_fetcher':
                    cursor.execute("SELECT COUNT(*) FROM cluster_data.rss_feeds_raw WHERE rss_feeds_raw_created_at > NOW() - INTERVAL '1 hour'")
                    component_status['recent_feeds'] = cursor.fetchone()[0]
                
                elif component_name == 'article_scraper':
                    cursor.execute("SELECT COUNT(*) FROM cluster_data.rss_feeds_clean WHERE rss_feeds_clean_created_at > NOW() - INTERVAL '1 hour'")
                    component_status['recent_articles'] = cursor.fetchone()[0]
                
                elif component_name == 'entity_extractor':
                    cursor.execute("SELECT COUNT(*) FROM cluster_data.rss_feeds_clean WHERE rss_feeds_clean_extracted_entities IS NOT NULL")
                    component_status['articles_with_entities'] = cursor.fetchone()[0]
                
                elif component_name == 'semantic_clusterer':
                    cursor.execute("SELECT COUNT(*) FROM cluster_data.clusters WHERE clusters_is_active = true")
                    component_status['active_clusters'] = cursor.fetchone()[0]
                
                elif component_name == 'article_ranker':
                    cursor.execute("SELECT COUNT(*) FROM cluster_data.article_rankings")
                    component_status['ranked_articles'] = cursor.fetchone()[0]
                
                status['components'][component_name] = component_status
            
        finally:
            cursor.close()
            conn.close()
        
        return status