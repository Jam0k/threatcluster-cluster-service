"""
Entity Sync Service - Synchronizes AI-extracted entities with the entities table
"""
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Tuple
import json
import asyncio

from src.config.settings import settings

logger = logging.getLogger(__name__)


class EntitySyncService:
    """Service for synchronizing AI-extracted entities with the database"""
    
    # Map AI category names to database category names
    CATEGORY_MAPPING = {
        # Technical Indicators
        'domains': 'domain',
        'ip_addresses': 'ip_address',
        'file_hashes': 'file_hash',
        'cves': 'cve',
        
        # Threat Intelligence
        'apt_groups': 'apt_group',
        'ransomware_groups': 'ransomware_group',
        'malware_families': 'malware_family',
        'attack_types': 'attack_type',
        'mitre_attack': 'mitre',
        'security_standards': 'security_standard',
        'vulnerability_types': 'vulnerability_type',
        
        # Business Intelligence
        'companies': 'company',
        'industry_sectors': 'industry_sector',
        'security_vendors': 'security_vendor',
        'government_agencies': 'government_agency',
        'countries': 'country',
        'platforms': 'platform'
    }
    
    # Default importance weights by category
    DEFAULT_WEIGHTS = {
        'cve': 80,
        'apt_group': 85,
        'ransomware_group': 85,
        'malware_family': 80,
        'attack_type': 70,
        'mitre': 75,
        'security_standard': 60,
        'vulnerability_type': 70,
        'domain': 65,
        'ip_address': 65,
        'file_hash': 60,
        'company': 50,
        'security_vendor': 55,
        'government_agency': 60,
        'industry_sector': 50,
        'country': 45,
        'platform': 55
    }
    
    def __init__(self):
        """Initialize the Entity Sync Service"""
        pass
    
    def extract_entities_from_ai_summary(self, ai_summary: Dict) -> List[Tuple[str, str]]:
        """
        Extract entities from AI summary JSON structure.
        
        Args:
            ai_summary: The AI summary JSON containing entities
            
        Returns:
            List of (entity_name, category) tuples
        """
        entities = []
        
        if not ai_summary or 'entities' not in ai_summary:
            return entities
        
        entities_data = ai_summary['entities']
        
        # Process each category group
        for group_data in entities_data.values():
            if not isinstance(group_data, dict):
                continue
                
            # Process each category within the group
            for ai_category, entity_list in group_data.items():
                if not isinstance(entity_list, list):
                    continue
                
                # Map AI category to database category
                db_category = self.CATEGORY_MAPPING.get(ai_category)
                if not db_category:
                    logger.warning(f"Unknown entity category: {ai_category}")
                    continue
                
                # Add each entity
                for entity_name in entity_list:
                    if entity_name and isinstance(entity_name, str):
                        # Clean and validate entity name
                        entity_name = entity_name.strip()
                        if len(entity_name) > 500:
                            logger.warning(f"Entity name too long, truncating: {entity_name}")
                            entity_name = entity_name[:500]
                        
                        entities.append((entity_name, db_category))
        
        return entities
    
    def sync_entities_to_database(self, entities: List[Tuple[str, str]]) -> Dict[str, int]:
        """
        Sync entities to the database, inserting new ones.
        
        Args:
            entities: List of (entity_name, category) tuples
            
        Returns:
            Dictionary with counts of new and existing entities
        """
        if not entities:
            return {'new': 0, 'existing': 0, 'errors': 0, 'new_entity_ids': []}
        
        conn = psycopg2.connect(settings.database_url)
        stats = {'new': 0, 'existing': 0, 'errors': 0, 'new_entity_ids': []}
        
        try:
            with conn.cursor() as cur:
                for entity_name, category in entities:
                    try:
                        # First check if entity exists (case-insensitive)
                        cur.execute("""
                            SELECT entities_id, entities_name 
                            FROM cluster_data.entities 
                            WHERE LOWER(entities_name) = LOWER(%s) 
                            AND entities_category = %s
                            LIMIT 1
                        """, (entity_name, category))
                        
                        existing = cur.fetchone()
                        
                        if existing:
                            stats['existing'] += 1
                            # Log if case differs
                            if existing[1] != entity_name:
                                logger.debug(f"Entity exists with different case: '{entity_name}' -> '{existing[1]}' ({category})")
                        else:
                            # Insert new entity
                            cur.execute("""
                                INSERT INTO cluster_data.entities 
                                (entities_name, entities_category, entities_source, entities_importance_weight)
                                VALUES (%s, %s, 'ai_extracted', %s)
                                RETURNING entities_id
                            """, (
                                entity_name, 
                                category, 
                                self.DEFAULT_WEIGHTS.get(category, 50)
                            ))
                            
                            result = cur.fetchone()
                            if result:
                                stats['new'] += 1
                                stats['new_entity_ids'].append(result[0])
                                logger.info(f"Added new entity: {entity_name} ({category})")
                            
                    except Exception as e:
                        logger.error(f"Error inserting entity {entity_name} ({category}): {e}")
                        stats['errors'] += 1
                        conn.rollback()
                        continue
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Database error during entity sync: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return stats
    
    async def sync_cluster_entities(self, cluster_id: int) -> Dict[str, int]:
        """
        Sync entities from a specific cluster's AI summary.
        
        Args:
            cluster_id: The cluster ID to sync entities from
            
        Returns:
            Dictionary with sync statistics
        """
        conn = psycopg2.connect(settings.database_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get the AI summary for the cluster
                cur.execute("""
                    SELECT ai_summary
                    FROM cluster_data.clusters
                    WHERE clusters_id = %s
                    AND has_ai_summary = TRUE
                """, (cluster_id,))
                
                result = cur.fetchone()
                if not result or not result['ai_summary']:
                    logger.info(f"No AI summary found for cluster {cluster_id}")
                    return {'new': 0, 'existing': 0, 'errors': 0}
                
                # Extract entities from AI summary
                entities = self.extract_entities_from_ai_summary(result['ai_summary'])
                
                if not entities:
                    logger.info(f"No entities found in AI summary for cluster {cluster_id}")
                    return {'new': 0, 'existing': 0, 'errors': 0}
                
                logger.info(f"Found {len(entities)} entities in cluster {cluster_id}")
                
                # Sync to database
                stats = self.sync_entities_to_database(entities)
                
                # Generate descriptions for new entities
                if stats['new'] > 0 and stats.get('new_entity_ids'):
                    await self.generate_descriptions_for_new_entities(stats['new_entity_ids'])
                
                return stats
                
        except Exception as e:
            logger.error(f"Error syncing entities for cluster {cluster_id}: {e}")
            return {'new': 0, 'existing': 0, 'errors': 0}
        finally:
            conn.close()
    
    async def generate_descriptions_for_new_entities(self, new_entity_ids: List[int]):
        """
        Generate descriptions for newly created entities.
        
        Args:
            new_entity_ids: List of entity IDs that were just created
        """
        if not new_entity_ids:
            return
            
        try:
            # Import here to avoid circular imports
            from .entity_description_service import EntityDescriptionService
            
            # Get entity details
            conn = psycopg2.connect(settings.database_url)
            entities_to_process = []
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT entities_id, entities_name, entities_category
                    FROM cluster_data.entities
                    WHERE entities_id = ANY(%s)
                """, (new_entity_ids,))
                
                for row in cur.fetchall():
                    entities_to_process.append({
                        'name': row['entities_name'],
                        'category': row['entities_category']
                    })
            
            conn.close()
            
            if entities_to_process:
                # Generate descriptions
                desc_service = EntityDescriptionService()
                descriptions = await desc_service.generate_descriptions_batch(entities_to_process)
                
                # Update entities with descriptions
                if descriptions:
                    stats = desc_service.update_entity_descriptions(descriptions)
                    logger.info(f"Generated descriptions for {stats['updated']} new entities")
                    
        except Exception as e:
            logger.error(f"Error generating descriptions for new entities: {e}")
    
    def sync_all_recent_clusters(self, hours: int = 24) -> Dict[str, int]:
        """
        Sync entities from all clusters with AI summaries in the last N hours.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Aggregate statistics from all clusters
        """
        conn = psycopg2.connect(settings.database_url)
        total_stats = {'new': 0, 'existing': 0, 'errors': 0, 'clusters_processed': 0}
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get all clusters with AI summaries from the last N hours
                cur.execute("""
                    SELECT clusters_id, clusters_name
                    FROM cluster_data.clusters
                    WHERE has_ai_summary = TRUE
                    AND ai_summary_generated_at >= NOW() - INTERVAL '%s hours'
                    ORDER BY ai_summary_generated_at DESC
                """, (hours,))
                
                clusters = cur.fetchall()
                logger.info(f"Found {len(clusters)} clusters with AI summaries from the last {hours} hours")
                
                for cluster in clusters:
                    logger.info(f"Syncing entities from cluster {cluster['clusters_id']}: {cluster['clusters_name']}")
                    stats = self.sync_cluster_entities(cluster['clusters_id'])
                    
                    # Aggregate statistics
                    total_stats['new'] += stats['new']
                    total_stats['existing'] += stats['existing']
                    total_stats['errors'] += stats['errors']
                    total_stats['clusters_processed'] += 1
                
        except Exception as e:
            logger.error(f"Error during batch entity sync: {e}")
        finally:
            conn.close()
        
        logger.info(f"Entity sync complete: {total_stats}")
        return total_stats
    
    def get_entity_statistics(self) -> Dict[str, any]:
        """Get statistics about entities in the database"""
        conn = psycopg2.connect(settings.database_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Count by source
                cur.execute("""
                    SELECT 
                        entities_source,
                        COUNT(*) as count
                    FROM cluster_data.entities
                    GROUP BY entities_source
                """)
                source_counts = {row['entities_source']: row['count'] for row in cur.fetchall()}
                
                # Count by category for AI-extracted entities
                cur.execute("""
                    SELECT 
                        entities_category,
                        COUNT(*) as count
                    FROM cluster_data.entities
                    WHERE entities_source = 'ai_extracted'
                    GROUP BY entities_category
                    ORDER BY count DESC
                """)
                ai_category_counts = {row['entities_category']: row['count'] for row in cur.fetchall()}
                
                # Recent AI-extracted entities
                cur.execute("""
                    SELECT 
                        entities_name,
                        entities_category,
                        entities_added_on
                    FROM cluster_data.entities
                    WHERE entities_source = 'ai_extracted'
                    ORDER BY entities_added_on DESC
                    LIMIT 10
                """)
                recent_entities = cur.fetchall()
                
                return {
                    'source_counts': source_counts,
                    'ai_category_counts': ai_category_counts,
                    'recent_ai_entities': recent_entities
                }
                
        except Exception as e:
            logger.error(f"Error getting entity statistics: {e}")
            return {}
        finally:
            conn.close()


# Test function
def test_entity_sync():
    """Test the entity sync service"""
    service = EntitySyncService()
    
    # Test with sample AI summary data
    sample_ai_summary = {
        "entities": {
            "technical_indicators": {
                "domains": ["malicious-site.com", "c2-server.ru"],
                "cves": ["CVE-2025-1234", "CVE-2025-5678"]
            },
            "threat_intelligence": {
                "apt_groups": ["APT28", "Lazarus Group"],
                "malware_families": ["Emotet", "Cobalt Strike"]
            },
            "business_intelligence": {
                "companies": ["Microsoft", "Google"],
                "security_vendors": ["CrowdStrike", "Palo Alto Networks"]
            }
        }
    }
    
    # Extract entities
    entities = service.extract_entities_from_ai_summary(sample_ai_summary)
    print(f"Extracted {len(entities)} entities:")
    for name, category in entities:
        print(f"  - {name} ({category})")
    
    # Sync to database
    stats = service.sync_entities_to_database(entities)
    print(f"\nSync results: {stats}")
    
    # Get statistics
    entity_stats = service.get_entity_statistics()
    print(f"\nEntity statistics: {json.dumps(entity_stats, indent=2, default=str)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_entity_sync()