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
from .stix_parser import STIXParser

logger = structlog.get_logger(__name__)


class MISPFetcher:
    """Fetches and processes MISP galaxy threat intelligence data."""
    
    def __init__(self):
        """Initialize the MISP fetcher."""
        self.parser = MISPParser()
        self.stix_parser = STIXParser()
        
        # Load configuration
        self.config = self._load_config()
        
        # STIX data cache
        self.stix_data_cache = {}
        self.stix_name_to_id = {}
        
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
        
        # Check if we need to fetch STIX data for MITRE techniques
        has_mitre_feed = any(f.get('type') == 'mitre' for f in active_feeds)
        if has_mitre_feed:
            logger.info("fetching_mitre_stix_data")
            self._fetch_mitre_stix_data()
        
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
        elif feed_type == 'ransomware_group':
            entities = self.parser.parse_ransomware_groups(content)
        elif feed_type == 'mitre':
            entities = self.parser.parse_mitre_techniques(content)
            
            # For MITRE, also add new techniques from STIX that don't exist in MISP
            if self.stix_data_cache:
                new_entities = self._create_new_mitre_entities_from_stix(entities)
                entities.extend(new_entities)
                logger.info("added_new_stix_techniques", count=len(new_entities))
        else:
            logger.warning("unsupported_feed_type", 
                         feed_type=feed_type)
            return
        
        self.stats['entities_fetched'] += len(entities)
        
        # Store entities in database
        self._store_entities(entities, feed_type)
    
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
    
    def _fetch_mitre_stix_data(self):
        """
        Fetch MITRE ATT&CK STIX data from official repository.
        """
        stix_url = "https://raw.githubusercontent.com/mitre-attack/attack-stix-data/refs/heads/master/enterprise-attack/enterprise-attack.json"
        
        try:
            logger.info("fetching_mitre_stix", url=stix_url)
            
            # Fetch with increased timeout for large file
            response = requests.get(
                stix_url, 
                headers=self.headers,
                timeout=60  # 60 seconds for large file
            )
            response.raise_for_status()
            
            logger.info("mitre_stix_fetched",
                       size_mb=len(response.content) / 1024 / 1024)
            
            # Parse STIX data
            self.stix_data_cache = self.stix_parser.parse_mitre_stix(response.text)
            self.stix_name_to_id = self.stix_parser.create_name_to_id_mapping(self.stix_data_cache)
            
            logger.info("mitre_stix_parsed",
                       techniques_count=len(self.stix_data_cache),
                       name_mappings=len(self.stix_name_to_id))
            
        except requests.exceptions.Timeout:
            logger.error("stix_fetch_timeout", url=stix_url)
            self.stix_data_cache = {}
            self.stix_name_to_id = {}
        except requests.exceptions.RequestException as e:
            logger.error("stix_fetch_error", 
                        url=stix_url,
                        error=str(e))
            self.stix_data_cache = {}
            self.stix_name_to_id = {}
        except Exception as e:
            logger.error("stix_processing_error", error=str(e))
            self.stix_data_cache = {}
            self.stix_name_to_id = {}
    
    def _create_misp_to_stix_mapping(self, misp_entities: List[Dict]) -> Dict[str, str]:
        """
        Create a mapping from MISP entity names to STIX technique IDs.
        
        Args:
            misp_entities: List of MISP entities
            
        Returns:
            Dict mapping entity names to technique IDs
        """
        mapping = {}
        
        for entity in misp_entities:
            name = entity.get('entities_name', '')
            json_data = entity.get('entities_json', {})
            
            # Try to extract technique ID from value field
            if isinstance(json_data, dict):
                value = json_data.get('value', '')
                technique_id = self.parser.extract_technique_id_from_value(value)
                if technique_id:
                    mapping[name] = technique_id
                    continue
            
            # Try to find ID in the name itself (some entries might have it)
            technique_id = self.parser.extract_technique_id_from_value(name)
            if technique_id:
                mapping[name] = technique_id
        
        return mapping
    
    def _create_new_mitre_entities_from_stix(self, existing_entities: List[Dict]) -> List[Dict]:
        """
        Create new MITRE entities from STIX data that don't exist in MISP.
        
        Args:
            existing_entities: List of existing MISP entities
            
        Returns:
            List of new entities to add
        """
        # Create mapping of existing techniques
        existing_ids = set()
        existing_names = set()
        
        for entity in existing_entities:
            # Add entity name to existing names
            entity_name = entity.get('entities_name', '')
            if entity_name:
                existing_names.add(entity_name.lower())
            
            # Try to extract technique ID
            json_data = entity.get('entities_json', {})
            if isinstance(json_data, dict):
                value = json_data.get('value', '')
                technique_id = self.parser.extract_technique_id_from_value(value)
                if technique_id:
                    existing_ids.add(technique_id)
            
            # Also check entity name for ID
            technique_id = self.parser.extract_technique_id_from_value(entity_name)
            if technique_id:
                existing_ids.add(technique_id)
        
        # Create new entities for STIX techniques not in MISP
        new_entities = []
        for technique_id, stix_data in self.stix_data_cache.items():
            technique_name = stix_data.get('name', '')
            technique_name_lower = technique_name.lower()
            
            # Check if this technique already exists by ID or name
            if technique_id not in existing_ids and technique_name_lower not in existing_names:
                # Create entity from STIX data
                entity = self._create_entity_from_stix(technique_id, stix_data)
                if entity:
                    new_entities.append(entity)
                    logger.debug("creating_new_entity_from_stix", 
                               technique_id=technique_id,
                               name=technique_name)
        
        return new_entities
    
    def _create_entity_from_stix(self, technique_id: str, stix_data: Dict) -> Optional[Dict]:
        """
        Create an entity dict from STIX data.
        
        Args:
            technique_id: MITRE technique ID
            stix_data: STIX attack-pattern object
            
        Returns:
            Entity dict or None
        """
        try:
            # Extract key fields
            name = stix_data.get('name', technique_id)
            description = stix_data.get('description', '')
            
            # Build MISP-compatible structure
            misp_compatible = {
                'value': technique_id,
                'uuid': stix_data.get('id', ''),
                'description': description,
                'meta': {
                    'source': 'MITRE ATT&CK (STIX)',
                    'platforms': stix_data.get('x_mitre_platforms', []),
                }
            }
            
            # Add kill chain phases
            kill_chain_phases = []
            for phase in stix_data.get('kill_chain_phases', []):
                if phase.get('kill_chain_name') == 'mitre-attack':
                    kill_chain_phases.append(phase.get('phase_name', ''))
            if kill_chain_phases:
                misp_compatible['meta']['kill_chain_phases'] = kill_chain_phases
            
            # Add data sources
            if 'x_mitre_data_sources' in stix_data:
                misp_compatible['meta']['data_sources'] = stix_data['x_mitre_data_sources']
            
            # Create full entity with STIX data included
            full_json = self.stix_parser.merge_with_misp_data(misp_compatible, stix_data)
            
            entity = {
                'entities_name': technique_id,  # Use technique ID as name
                'entities_category': 'mitre',
                'entities_source': 'misp',  # Keep as 'misp' for consistency
                'entities_importance_weight': 50,  # Default weight
                'entities_json': full_json
            }
            
            return entity
            
        except Exception as e:
            logger.error("failed_to_create_entity_from_stix",
                        technique_id=technique_id,
                        error=str(e))
            return None
    
    def _store_entities(self, entities: List[Dict], feed_type: str):
        """
        Store entities in the database with duplicate checking.
        For MITRE entities, merge with STIX data if available.
        
        Args:
            entities: List of parsed entities
            feed_type: Type of feed being processed
        """
        if not entities:
            logger.warning("no_entities_to_store")
            return
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            for entity in entities:
                try:
                    # For MITRE entities, merge with STIX data if available
                    if feed_type == 'mitre' and self.stix_data_cache:
                        # Try to find matching STIX data
                        # First try to extract technique ID from the entity
                        technique_id = None
                        entity_name = entity.get('entities_name', '')
                        entity_json = entity.get('entities_json', {})
                        
                        # Try to get ID from the value field in JSON
                        if isinstance(entity_json, dict):
                            value = entity_json.get('value', '')
                            technique_id = self.parser.extract_technique_id_from_value(value)
                        
                        # If not found, try entity name
                        if not technique_id:
                            technique_id = self.parser.extract_technique_id_from_value(entity_name)
                        
                        # If still not found, try name matching
                        if not technique_id:
                            # Try to find by name
                            entity_name_lower = entity_name.lower()
                            technique_id = self.stix_name_to_id.get(entity_name_lower)
                        
                        # Look for STIX data
                        if technique_id and technique_id in self.stix_data_cache:
                            stix_data = self.stix_data_cache[technique_id]
                            # Merge MISP and STIX data
                            original_json = entity.get('entities_json', {})
                            merged_json = self.stix_parser.merge_with_misp_data(
                                original_json, stix_data
                            )
                            entity['entities_json'] = merged_json
                            logger.debug("merged_mitre_data", 
                                       entity_name=entity_name,
                                       technique_id=technique_id,
                                       has_stix=True)
                        else:
                            logger.debug("no_stix_data_for_technique", 
                                       entity_name=entity_name,
                                       technique_id=technique_id or "not_found")
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