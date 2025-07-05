"""
MISP Fetcher Module

Fetches threat intelligence data from MISP galaxy feeds and stores in database.
"""
import requests
import yaml
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import psycopg2
from psycopg2.extras import Json
import structlog

from src.config.settings import settings
from .misp_parser import MISPParser

logger = structlog.get_logger(__name__)


class MISPFetcher:
    """Fetches and processes MISP galaxy threat intelligence data."""
    
    def __init__(self):
        """Initialize the MISP fetcher."""
        self.parser = MISPParser()
        
        # Load configuration
        self.config = self._load_config()
        
        # HTTP settings
        self.request_timeout = self.config.get('config', {}).get('request_timeout', 30)
        self.user_agent = self.config.get('config', {}).get('user_agent', 'ThreatCluster MISP Fetcher/1.0')
        
        # Headers for HTTP requests
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'application/json'
        }
        
        # Statistics tracking
        self.stats = {
            'feeds_processed': 0,
            'feeds_failed': 0,
            'entities_fetched': 0,
            'entities_inserted': 0,
            'entities_updated': 0,
            'entities_skipped': 0,
            'errors': []
        }
    
    def _load_config(self) -> dict:
        """Load MISP configuration from YAML file."""
        config_path = settings.config_dir / 'misp_feeds.yaml'
        
        # Default configuration if file doesn't exist
        default_config = {
            'feeds': [
                {
                    'name': 'MISP Threat Actors',
                    'url': 'https://raw.githubusercontent.com/MISP/misp-galaxy/refs/heads/main/clusters/threat-actor.json',
                    'type': 'threat_actor',
                    'active': True
                }
            ],
            'config': {
                'request_timeout': 30,
                'user_agent': 'ThreatCluster MISP Fetcher/1.0',
                'fetch_interval_hours': 24
            }
        }
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info("misp_config_loaded", 
                          feed_count=len(config.get('feeds', [])))
                return config
        except FileNotFoundError:
            logger.warning("misp_config_not_found", 
                         path=str(config_path),
                         using_defaults=True)
            return default_config
        except Exception as e:
            logger.error("failed_to_load_misp_config", error=str(e))
            return default_config
    
    def fetch_all_feeds(self) -> Dict[str, any]:
        """
        Fetch and process all active MISP feeds.
        
        Returns:
            Statistics about the fetch operation
        """
        start_time = datetime.now()
        self.stats['start_time'] = start_time.isoformat()
        
        # Get active feeds
        active_feeds = [f for f in self.config.get('feeds', []) 
                       if f.get('active', False)]
        
        logger.info("starting_misp_fetch", active_feeds=len(active_feeds))
        
        # Process each feed
        for feed in active_feeds:
            try:
                self._process_feed(feed)
                self.stats['feeds_processed'] += 1
            except Exception as e:
                logger.error("feed_processing_error",
                           feed_name=feed['name'],
                           error=str(e))
                self.stats['feeds_failed'] += 1
                self.stats['errors'].append({
                    'feed': feed['name'],
                    'error': str(e)
                })
        
        # Final statistics
        self.stats['end_time'] = datetime.now().isoformat()
        self.stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        logger.info("misp_fetch_complete", **self.stats)
        
        return self.stats
    
    def _process_feed(self, feed: Dict):
        """
        Process a single MISP feed.
        
        Args:
            feed: Feed configuration dict
        """
        feed_name = feed['name']
        feed_url = feed['url']
        feed_type = feed['type']
        
        logger.info("processing_feed", 
                   feed_name=feed_name,
                   feed_type=feed_type)
        
        # Fetch the feed content
        content = self._fetch_feed_content(feed_url)
        if not content:
            raise Exception(f"Failed to fetch content from {feed_url}")
        
        # Parse the content based on type
        if feed_type == 'threat_actor':
            entities = self.parser.parse_threat_actors(content)
        elif feed_type == 'malware_family':
            entities = self.parser.parse_malware_families(content)
        else:
            logger.warning("unsupported_feed_type", 
                         feed_type=feed_type)
            return
        
        self.stats['entities_fetched'] += len(entities)
        
        # Store entities in database
        self._store_entities(entities)
    
    def _fetch_feed_content(self, url: str) -> Optional[str]:
        """
        Fetch content from a MISP feed URL.
        
        Args:
            url: URL to fetch
            
        Returns:
            Content string or None if failed
        """
        try:
            response = requests.get(
                url, 
                headers=self.headers,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            logger.info("feed_content_fetched",
                       url=url,
                       size_bytes=len(response.content))
            
            return response.text
            
        except requests.exceptions.Timeout:
            logger.error("feed_fetch_timeout", url=url)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("feed_fetch_error", 
                        url=url,
                        error=str(e))
            return None
    
    def _store_entities(self, entities: List[Dict]):
        """
        Store entities in the database with duplicate checking.
        
        Args:
            entities: List of parsed entities
        """
        if not entities:
            logger.warning("no_entities_to_store")
            return
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            for entity in entities:
                try:
                    # Check if entity exists
                    cursor.execute("""
                        SELECT entities_id, entities_json 
                        FROM cluster_data.entities
                        WHERE entities_name = %s 
                        AND entities_category = %s
                    """, (entity['entities_name'], entity['entities_category']))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing entity
                        cursor.execute("""
                            UPDATE cluster_data.entities
                            SET entities_json = %s,
                                entities_importance_weight = %s,
                                entities_source = %s
                            WHERE entities_name = %s 
                            AND entities_category = %s
                        """, (
                            Json(entity['entities_json']),
                            entity['entities_importance_weight'],
                            entity['entities_source'],
                            entity['entities_name'],
                            entity['entities_category']
                        ))
                        
                        if cursor.rowcount > 0:
                            self.stats['entities_updated'] += 1
                            logger.debug("entity_updated", 
                                       name=entity['entities_name'])
                    else:
                        # Insert new entity
                        cursor.execute("""
                            INSERT INTO cluster_data.entities 
                            (entities_name, entities_category, entities_source, 
                             entities_importance_weight, entities_json)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            entity['entities_name'],
                            entity['entities_category'],
                            entity['entities_source'],
                            entity['entities_importance_weight'],
                            Json(entity['entities_json'])
                        ))
                        
                        self.stats['entities_inserted'] += 1
                        logger.debug("entity_inserted", 
                                   name=entity['entities_name'])
                        
                except psycopg2.IntegrityError as e:
                    # Handle duplicate key errors gracefully
                    logger.warning("entity_duplicate_error",
                                 name=entity['entities_name'],
                                 error=str(e))
                    self.stats['entities_skipped'] += 1
                    conn.rollback()
                    continue
                except Exception as e:
                    logger.error("entity_store_error",
                               name=entity['entities_name'],
                               error=str(e))
                    self.stats['errors'].append({
                        'entity': entity['entities_name'],
                        'error': str(e)
                    })
                    conn.rollback()
                    continue
            
            # Commit all changes
            conn.commit()
            logger.info("entities_stored",
                       inserted=self.stats['entities_inserted'],
                       updated=self.stats['entities_updated'])
            
        except Exception as e:
            logger.error("database_error", error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_entity_count(self) -> Dict[str, int]:
        """
        Get count of MISP entities in database.
        
        Returns:
            Dict with entity counts by category
        """
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT entities_category, COUNT(*) as count
                FROM cluster_data.entities
                WHERE entities_source = 'misp'
                GROUP BY entities_category
            """)
            
            counts = {}
            for row in cursor.fetchall():
                counts[row[0]] = row[1]
            
            return counts
            
        finally:
            cursor.close()
            conn.close()