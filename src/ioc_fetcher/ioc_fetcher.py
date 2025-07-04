"""
IOC Fetcher Module

Main module for fetching IOCs from threat intelligence feeds.
"""
import requests
import yaml
import json
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import psycopg2
from psycopg2.extras import execute_batch
import structlog

from src.config.settings import settings
from .ioc_parser import IOCParser
from .ioc_validator import IOCValidator

logger = structlog.get_logger(__name__)


class IOCFetcher:
    """Fetches and processes IOCs from threat intelligence feeds."""
    
    def __init__(self):
        """Initialize the IOC fetcher."""
        self.parser = IOCParser()
        self.validator = IOCValidator()
        
        # Load feed configuration
        self.feeds_config = self._load_feeds_config()
        self.config = self.feeds_config.get('config', {})
        
        # Configuration
        self.min_feed_count = self.config.get('min_feed_count', 2)
        self.request_timeout = self.config.get('request_timeout', 30)
        self.user_agent = self.config.get('user_agent', 'ThreatCluster IOC Fetcher/1.0')
        self.batch_size = self.config.get('batch_size', 1000)
        
        # Headers for HTTP requests
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/plain, text/html, application/octet-stream'
        }
        
    def _load_feeds_config(self) -> dict:
        """Load IOC feeds configuration from YAML file."""
        config_path = settings.config_dir / 'ioc_feeds.yaml'
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info("feeds_config_loaded", 
                          feed_count=len(config.get('feeds', [])))
                return config
        except Exception as e:
            logger.error("failed_to_load_feeds_config", error=str(e))
            return {'feeds': [], 'config': {}}
    
    def fetch_all_feeds(self) -> Dict[str, any]:
        """
        Fetch IOCs from all active feeds.
        
        Returns:
            Statistics about the fetch operation
        """
        start_time = datetime.now()
        stats = {
            'start_time': start_time.isoformat(),
            'feeds_processed': 0,
            'feeds_failed': 0,
            'total_iocs_fetched': 0,
            'total_iocs_validated': 0,
            'total_iocs_stored': 0,
            'iocs_by_type': defaultdict(int),
            'errors': []
        }
        
        # Collect all IOCs by type and track sources
        all_iocs = {
            'ip_address': defaultdict(set),
            'domain': defaultdict(set),
            'file_hash': defaultdict(set)
        }
        
        # Process each active feed
        active_feeds = [f for f in self.feeds_config.get('feeds', []) 
                       if f.get('active', False)]
        
        logger.info("starting_ioc_fetch", active_feeds=len(active_feeds))
        
        for feed in active_feeds:
            try:
                iocs = self._fetch_single_feed(feed)
                
                # Track which feeds each IOC came from
                for ioc in iocs:
                    all_iocs[feed['type']][ioc].add(feed['name'])
                
                stats['feeds_processed'] += 1
                stats['total_iocs_fetched'] += len(iocs)
                stats['iocs_by_type'][feed['type']] += len(iocs)
                
            except Exception as e:
                logger.error("feed_fetch_error", 
                           feed_name=feed['name'],
                           error=str(e))
                stats['feeds_failed'] += 1
                stats['errors'].append({
                    'feed': feed['name'],
                    'error': str(e)
                })
        
        # Filter IOCs that appear in minimum number of feeds
        filtered_iocs = self._filter_by_feed_count(all_iocs)
        
        # Store IOCs in database
        stored_count = self._store_iocs(filtered_iocs)
        
        # Update stats
        stats['total_iocs_validated'] = sum(
            len(iocs) for iocs in filtered_iocs.values()
        )
        stats['total_iocs_stored'] = stored_count
        stats['end_time'] = datetime.now().isoformat()
        stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        logger.info("ioc_fetch_complete", **stats)
        
        return stats
    
    def _fetch_single_feed(self, feed: Dict) -> List[str]:
        """
        Fetch and parse a single feed.
        
        Args:
            feed: Feed configuration dict
            
        Returns:
            List of validated IOCs from the feed
        """
        feed_name = feed['name']
        feed_url = feed['url']
        feed_type = feed['type']
        feed_format = feed['format']
        
        logger.info("fetching_feed", 
                   feed_name=feed_name,
                   feed_type=feed_type,
                   feed_format=feed_format)
        
        try:
            # Fetch feed content
            response = requests.get(
                feed_url,
                headers=self.headers,
                timeout=self.request_timeout,
                allow_redirects=True
            )
            response.raise_for_status()
            
            content = response.text
            
            # Parse IOCs from content
            raw_iocs = self.parser.parse_feed(content, feed_format, feed_type)
            
            # Validate IOCs
            valid_iocs = self.validator.validate_iocs(raw_iocs, feed_type)
            
            # Deduplicate
            unique_iocs = self.validator.deduplicate_iocs(valid_iocs)
            
            logger.info("feed_processed",
                       feed_name=feed_name,
                       raw_count=len(raw_iocs),
                       valid_count=len(valid_iocs),
                       unique_count=len(unique_iocs))
            
            return unique_iocs
            
        except requests.RequestException as e:
            logger.error("feed_fetch_failed",
                        feed_name=feed_name,
                        error=str(e))
            raise
        except Exception as e:
            logger.error("feed_processing_error",
                        feed_name=feed_name,
                        error=str(e))
            raise
    
    def _filter_by_feed_count(self, all_iocs: Dict[str, Dict[str, Set[str]]]) -> Dict[str, Dict[str, List[str]]]:
        """
        Filter IOCs that appear in minimum number of feeds.
        
        Args:
            all_iocs: Dict of IOC type -> IOC -> set of feed names
            
        Returns:
            Filtered IOCs with their source feeds
        """
        filtered = {
            'ip_address': {},
            'domain': {},
            'file_hash': {}
        }
        
        for ioc_type, iocs_dict in all_iocs.items():
            for ioc, feed_names in iocs_dict.items():
                if len(feed_names) >= self.min_feed_count:
                    filtered[ioc_type][ioc] = list(feed_names)
                    
        logger.info("iocs_filtered_by_feed_count",
                   min_feeds=self.min_feed_count,
                   ip_count=len(filtered['ip_address']),
                   domain_count=len(filtered['domain']),
                   hash_count=len(filtered['file_hash']))
        
        return filtered
    
    def _store_iocs(self, filtered_iocs: Dict[str, Dict[str, List[str]]]) -> int:
        """
        Store IOCs in the database.
        
        Args:
            filtered_iocs: Dict of IOC type -> IOC -> list of feed names
            
        Returns:
            Number of IOCs stored
        """
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        total_stored = 0
        
        try:
            # Prepare batch insert data
            insert_data = []
            
            for ioc_type, iocs_dict in filtered_iocs.items():
                for ioc, feed_names in iocs_dict.items():
                    # Calculate importance weight based on feed count
                    # 2 feeds = 50, 3 feeds = 65, 4+ feeds = 75
                    feed_count = len(feed_names)
                    if feed_count >= 4:
                        importance = 75
                    elif feed_count == 3:
                        importance = 65
                    else:
                        importance = 50
                    
                    insert_data.append((
                        ioc,                          # entities_name
                        ioc_type,                     # entities_category
                        'ioc_feeds',                  # entities_source
                        importance,                   # entities_importance_weight
                        json.dumps(feed_names)        # entities_ioc_source
                    ))
            
            # Batch insert with ON CONFLICT UPDATE
            if insert_data:
                execute_batch(
                    cursor,
                    """
                    INSERT INTO cluster_data.entities 
                    (entities_name, entities_category, entities_source, 
                     entities_importance_weight, entities_ioc_source)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (entities_name, entities_category) 
                    DO UPDATE SET
                        entities_source = EXCLUDED.entities_source,
                        entities_importance_weight = EXCLUDED.entities_importance_weight,
                        entities_ioc_source = EXCLUDED.entities_ioc_source,
                        entities_added_on = NOW()
                    """,
                    insert_data,
                    page_size=self.batch_size
                )
                
                total_stored = len(insert_data)
                conn.commit()
                
                logger.info("iocs_stored",
                           total=total_stored,
                           by_type={
                               ioc_type: len(iocs_dict) 
                               for ioc_type, iocs_dict in filtered_iocs.items()
                           })
            
        except Exception as e:
            conn.rollback()
            logger.error("database_error", error=str(e))
            raise
        finally:
            cursor.close()
            conn.close()
        
        return total_stored
    
    def get_stats(self) -> Dict[str, any]:
        """Get statistics about IOCs in the database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            # Get counts by category
            cursor.execute("""
                SELECT entities_category, COUNT(*) as count
                FROM cluster_data.entities
                WHERE entities_source = 'ioc_feeds'
                GROUP BY entities_category
            """)
            
            counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get total count
            cursor.execute("""
                SELECT COUNT(*)
                FROM cluster_data.entities
                WHERE entities_source = 'ioc_feeds'
            """)
            
            total = cursor.fetchone()[0]
            
            # Get most common IOC sources
            cursor.execute("""
                SELECT jsonb_array_elements_text(entities_ioc_source::jsonb) as source, 
                       COUNT(*) as count
                FROM cluster_data.entities
                WHERE entities_source = 'ioc_feeds'
                  AND entities_ioc_source IS NOT NULL
                GROUP BY source
                ORDER BY count DESC
                LIMIT 10
            """)
            
            top_sources = [
                {'source': row[0], 'count': row[1]} 
                for row in cursor.fetchall()
            ]
            
            return {
                'total_iocs': total,
                'by_category': counts,
                'top_sources': top_sources
            }
            
        finally:
            cursor.close()
            conn.close()