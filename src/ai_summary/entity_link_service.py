"""
Entity Link Service - Links AI-extracted entities to clusters and articles
"""
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Set
import json
from datetime import datetime, timezone

from src.config.settings import settings

logger = logging.getLogger(__name__)


class EntityLinkService:
    """Service for linking AI-extracted entities to their source clusters and articles"""
    
    def __init__(self):
        """Initialize the Entity Link Service"""
        pass
    
    def link_cluster_entities(self, cluster_id: int) -> Dict[str, int]:
        """
        Link AI-extracted entities from a cluster to all its articles.
        
        Args:
            cluster_id: The cluster ID to process
            
        Returns:
            Dictionary with linking statistics
        """
        conn = psycopg2.connect(settings.database_url)
        stats = {'articles_updated': 0, 'entities_linked': 0, 'errors': 0}
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get the cluster's AI summary with entities
                cur.execute("""
                    SELECT ai_summary
                    FROM cluster_data.clusters
                    WHERE clusters_id = %s
                    AND has_ai_summary = TRUE
                """, (cluster_id,))
                
                result = cur.fetchone()
                if not result or not result['ai_summary'] or 'entities' not in result['ai_summary']:
                    logger.info(f"No AI entities found for cluster {cluster_id}")
                    return stats
                
                # Extract entities from AI summary
                ai_entities = self._extract_entities_from_summary(result['ai_summary'])
                if not ai_entities:
                    logger.info(f"No entities to link for cluster {cluster_id}")
                    return stats
                
                # Get all articles in the cluster
                cur.execute("""
                    SELECT 
                        ca.cluster_articles_clean_id as article_id,
                        rfc.rss_feeds_clean_extracted_entities as current_entities
                    FROM cluster_data.cluster_articles ca
                    JOIN cluster_data.rss_feeds_clean rfc 
                        ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                    WHERE ca.cluster_articles_cluster_id = %s
                """, (cluster_id,))
                
                articles = cur.fetchall()
                
                # Update each article with AI-extracted entities
                for article in articles:
                    try:
                        # Merge AI entities with existing entities
                        merged_entities = self._merge_entities(
                            article['current_entities'], 
                            ai_entities
                        )
                        
                        # Update the article
                        cur.execute("""
                            UPDATE cluster_data.rss_feeds_clean
                            SET rss_feeds_clean_extracted_entities = %s
                            WHERE rss_feeds_clean_id = %s
                        """, (json.dumps(merged_entities), article['article_id']))
                        
                        stats['articles_updated'] += 1
                        stats['entities_linked'] += len(ai_entities)
                        
                    except Exception as e:
                        logger.error(f"Error updating article {article['article_id']}: {e}")
                        stats['errors'] += 1
                
                conn.commit()
                logger.info(f"Successfully linked entities for cluster {cluster_id}: {stats}")
                
        except Exception as e:
            logger.error(f"Error linking entities for cluster {cluster_id}: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return stats
    
    def _extract_entities_from_summary(self, ai_summary: Dict) -> List[Dict]:
        """
        Extract entities from AI summary and format for article storage.
        
        Args:
            ai_summary: The AI summary JSON containing entities
            
        Returns:
            List of entity dictionaries formatted for article storage
        """
        entities = []
        
        if 'entities' not in ai_summary:
            return entities
        
        # Category mapping (same as in entity_sync_service)
        category_mapping = {
            'domains': 'domain',
            'ip_addresses': 'ip_address',
            'file_hashes': 'file_hash',
            'cves': 'cve',
            'apt_groups': 'apt_group',
            'ransomware_groups': 'ransomware_group',
            'malware_families': 'malware_family',
            'attack_types': 'attack_type',
            'mitre_attack': 'mitre',
            'security_standards': 'security_standard',
            'vulnerability_types': 'vulnerability_type',
            'companies': 'company',
            'industry_sectors': 'industry_sector',
            'security_vendors': 'security_vendor',
            'government_agencies': 'government_agency',
            'countries': 'country',
            'platforms': 'platform'
        }
        
        # Get entity IDs from database for linking
        entity_id_map = self._get_entity_ids(ai_summary['entities'], category_mapping)
        
        # Process each entity group
        for group_data in ai_summary['entities'].values():
            if not isinstance(group_data, dict):
                continue
                
            for ai_category, entity_list in group_data.items():
                if not isinstance(entity_list, list):
                    continue
                
                db_category = category_mapping.get(ai_category)
                if not db_category:
                    continue
                
                for entity_name in entity_list:
                    if not entity_name or not isinstance(entity_name, str):
                        continue
                    
                    # Get entity ID
                    entity_key = f"{entity_name}|{db_category}"
                    entity_id = entity_id_map.get(entity_key)
                    
                    if entity_id:
                        # Format entity for article storage
                        entities.append({
                            'entity_name': entity_name,
                            'entity_category': db_category,
                            'entities_id': entity_id,
                            'confidence': 0.95,  # High confidence for AI-extracted
                            'position': 'ai_summary',  # Special position for AI-extracted
                            'extraction_method': 'ai_openai'  # Mark as AI-extracted
                        })
        
        return entities
    
    def _get_entity_ids(self, entities_data: Dict, category_mapping: Dict) -> Dict[str, int]:
        """
        Get entity IDs from database for all entities in the AI summary.
        
        Returns:
            Dictionary mapping "entity_name|category" to entity_id
        """
        entity_id_map = {}
        
        # Collect all entity name/category pairs
        entity_pairs = []
        for group_data in entities_data.values():
            if not isinstance(group_data, dict):
                continue
                
            for ai_category, entity_list in group_data.items():
                if not isinstance(entity_list, list):
                    continue
                
                db_category = category_mapping.get(ai_category)
                if not db_category:
                    continue
                
                for entity_name in entity_list:
                    if entity_name and isinstance(entity_name, str):
                        entity_pairs.append((entity_name, db_category))
        
        if not entity_pairs:
            return entity_id_map
        
        # Query database for entity IDs
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor() as cur:
                # Build query with multiple OR conditions
                conditions = []
                params = []
                for name, category in entity_pairs:
                    conditions.append("(entities_name = %s AND entities_category = %s)")
                    params.extend([name, category])
                
                query = f"""
                    SELECT entities_id, entities_name, entities_category
                    FROM cluster_data.entities
                    WHERE {' OR '.join(conditions)}
                """
                
                cur.execute(query, params)
                
                # Build the map
                for row in cur.fetchall():
                    key = f"{row[1]}|{row[2]}"  # name|category
                    entity_id_map[key] = row[0]
                    
        except Exception as e:
            logger.error(f"Error fetching entity IDs: {e}")
        finally:
            conn.close()
        
        return entity_id_map
    
    def _merge_entities(self, current_entities: Dict, ai_entities: List[Dict]) -> Dict:
        """
        Merge AI-extracted entities with existing entities.
        
        Args:
            current_entities: Current extracted entities JSON from article
            ai_entities: New AI-extracted entities
            
        Returns:
            Merged entity data
        """
        if not current_entities:
            # No existing entities, create new structure
            return {
                'entities': ai_entities,
                'extraction_timestamp': datetime.now(timezone.utc).isoformat(),
                'entity_count': len(ai_entities),
                'categories': list(set(e['entity_category'] for e in ai_entities))
            }
        
        # Get existing entities list
        existing_entities = current_entities.get('entities', [])
        
        # Create a set of existing entity keys to avoid duplicates
        existing_keys = set()
        for entity in existing_entities:
            key = f"{entity.get('entity_name')}|{entity.get('entity_category')}"
            existing_keys.add(key)
        
        # Add only new AI entities
        merged_entities = existing_entities.copy()
        new_count = 0
        
        for ai_entity in ai_entities:
            key = f"{ai_entity['entity_name']}|{ai_entity['entity_category']}"
            if key not in existing_keys:
                merged_entities.append(ai_entity)
                existing_keys.add(key)
                new_count += 1
        
        # Update metadata
        all_categories = set(e['entity_category'] for e in merged_entities)
        
        return {
            'entities': merged_entities,
            'extraction_timestamp': current_entities.get('extraction_timestamp', datetime.now(timezone.utc).isoformat()),
            'ai_enrichment_timestamp': datetime.now(timezone.utc).isoformat(),
            'entity_count': len(merged_entities),
            'ai_entities_added': new_count,
            'categories': list(all_categories)
        }
    
    def link_all_recent_clusters(self, hours: int = 24) -> Dict[str, int]:
        """
        Link AI-extracted entities for all clusters with AI summaries in the last N hours.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Aggregate statistics
        """
        conn = psycopg2.connect(settings.database_url)
        total_stats = {
            'clusters_processed': 0,
            'articles_updated': 0,
            'entities_linked': 0,
            'errors': 0
        }
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get clusters with AI summaries containing entities
                cur.execute("""
                    SELECT 
                        clusters_id, 
                        clusters_name,
                        jsonb_exists(ai_summary, 'entities') as has_entities
                    FROM cluster_data.clusters
                    WHERE has_ai_summary = TRUE
                    AND ai_summary_generated_at >= NOW() - INTERVAL '%s hours'
                    ORDER BY ai_summary_generated_at DESC
                """, (hours,))
                
                clusters = cur.fetchall()
                eligible_clusters = [c for c in clusters if c['has_entities']]
                
                logger.info(f"Found {len(eligible_clusters)} clusters with AI entities from the last {hours} hours")
                
                for cluster in eligible_clusters:
                    logger.info(f"Linking entities for cluster {cluster['clusters_id']}: {cluster['clusters_name']}")
                    stats = self.link_cluster_entities(cluster['clusters_id'])
                    
                    # Aggregate statistics
                    total_stats['articles_updated'] += stats['articles_updated']
                    total_stats['entities_linked'] += stats['entities_linked']
                    total_stats['errors'] += stats['errors']
                    total_stats['clusters_processed'] += 1
                
        except Exception as e:
            logger.error(f"Error during batch entity linking: {e}")
        finally:
            conn.close()
        
        logger.info(f"Entity linking complete: {total_stats}")
        return total_stats


# Test function
def test_entity_linking():
    """Test the entity linking service"""
    service = EntityLinkService()
    
    # Test with cluster 1450 (which we know has AI entities)
    print("Testing entity linking for cluster 1450...")
    stats = service.link_cluster_entities(1450)
    print(f"Linking results: {stats}")
    
    # Test batch linking
    print("\nTesting batch linking for recent clusters...")
    total_stats = service.link_all_recent_clusters(hours=24)
    print(f"Batch results: {total_stats}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_entity_linking()