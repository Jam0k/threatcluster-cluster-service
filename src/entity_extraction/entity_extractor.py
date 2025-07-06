#!/usr/bin/env python3
"""
Entity Extraction Module

Extracts cybersecurity-relevant entities from cleaned article content using both
regex patterns and predefined entity matching. Updates articles with extracted
entities and discovers new dynamic entities.
"""
import re
import json
import time
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict
import structlog

from src.config.settings import settings
from src.entity_extraction.entity_validator import EntityValidator


# Configure structured logging
logger = structlog.get_logger(__name__)


class EntityExtractor:
    """Extracts cybersecurity entities from article content."""
    
    def __init__(self):
        """Initialize the entity extractor with configuration."""
        self.config = settings.app_config
        self.entity_config = self.config.get('entities', {})
        self.pipeline_config = self.config.get('pipeline', {})
        
        # Processing settings
        self.batch_size = self.pipeline_config.get('processing_batch_size', 100)
        
        # Initialize validator
        self.validator = EntityValidator()
        
        # Compile regex patterns
        self.regex_patterns = self._compile_regex_patterns()
        
        # Load predefined entities
        self.predefined_entities = self._load_predefined_entities()
        
        # Dynamic entity weights
        self.dynamic_weights = self.entity_config.get('dynamic_weights', {})
        
        # Statistics tracking
        self.stats = {
            'articles_processed': 0,
            'entities_extracted': 0,
            'new_entities_discovered': 0,
            'extraction_errors': 0,
            'entity_categories': defaultdict(int)
        }
    
    def _compile_regex_patterns(self) -> Dict[str, re.Pattern]:
        """Compile regex patterns for efficient extraction."""
        patterns = {}
        
        # Load patterns from config
        config_patterns = self.entity_config.get('regex_patterns', {})
        
        # Additional patterns not in config
        additional_patterns = {
            'bitcoin_address': r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b',
            'ethereum_address': r'\b0x[a-fA-F0-9]{40}\b',
            'windows_file_path': r'[A-Za-z]:\\(?:[^<>:"|?*\n\r]+\\)*[^<>:"|?*\n\r]+',
            'unix_file_path': r'\/(?:[^\/\0]+\/)*[^\/\0]+',
            'registry_key': r'HK(?:EY_)?(?:LOCAL_MACHINE|CURRENT_USER|CLASSES_ROOT|USERS|CURRENT_CONFIG)(?:\\[^\\\s\n\r]+)+',
            'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        }
        
        # Combine all patterns
        all_patterns = {**config_patterns, **additional_patterns}
        
        # Compile patterns
        for name, pattern in all_patterns.items():
            try:
                patterns[name] = re.compile(pattern, re.IGNORECASE if name != 'file_hash' else 0)
                logger.debug("compiled_regex_pattern", pattern_name=name)
            except re.error as e:
                logger.error("regex_compilation_error", pattern_name=name, error=str(e))
        
        return patterns
    
    def _load_predefined_entities(self) -> Dict[str, List[Dict]]:
        """Load predefined entities from database including synonyms."""
        entities = defaultdict(list)
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    entities_id,
                    entities_name,
                    entities_category,
                    entities_importance_weight,
                    entities_json
                FROM cluster_data.entities
                WHERE entities_source IN ('manual', 'misp')
                ORDER BY entities_importance_weight DESC
            """)
            
            total_synonyms = 0
            for row in cursor:
                # Create patterns list for main name and synonyms
                patterns = []
                
                # Add pattern for main name
                patterns.append({
                    'pattern': re.compile(r'\b' + re.escape(row['entities_name']) + r'\b', re.IGNORECASE),
                    'matched_name': row['entities_name']
                })
                
                # Extract synonyms from entities_json if available
                if row['entities_json'] and isinstance(row['entities_json'], dict):
                    meta = row['entities_json'].get('meta', {})
                    synonyms = meta.get('synonyms', [])
                    
                    for synonym in synonyms:
                        if synonym and isinstance(synonym, str) and synonym != row['entities_name']:
                            patterns.append({
                                'pattern': re.compile(r'\b' + re.escape(synonym) + r'\b', re.IGNORECASE),
                                'matched_name': synonym
                            })
                            total_synonyms += 1
                
                entity_data = {
                    'id': row['entities_id'],
                    'name': row['entities_name'],
                    'weight': row['entities_importance_weight'],
                    'patterns': patterns  # Now contains multiple patterns
                }
                entities[row['entities_category']].append(entity_data)
            
            logger.info("loaded_predefined_entities", 
                       categories=len(entities),
                       total_entities=sum(len(v) for v in entities.values()),
                       total_synonyms=total_synonyms)
            
        finally:
            cursor.close()
            conn.close()
        
        return dict(entities)
    
    def get_unprocessed_articles(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch articles that haven't had entities extracted."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            query = """
                SELECT 
                    rfc.rss_feeds_clean_id,
                    rfc.rss_feeds_clean_title,
                    rfc.rss_feeds_clean_content,
                    rfr.rss_feeds_raw_published_date,
                    rf.rss_feeds_credibility
                FROM cluster_data.rss_feeds_clean rfc
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                WHERE rfc.rss_feeds_clean_processed = TRUE
                AND rfc.rss_feeds_clean_extracted_entities IS NULL
                ORDER BY rfr.rss_feeds_raw_published_date DESC
                LIMIT %s
            """
            cursor.execute(query, (limit or self.batch_size,))
            articles = [dict(row) for row in cursor.fetchall()]
            
            logger.info("fetched_unprocessed_articles", count=len(articles))
            return articles
            
        finally:
            cursor.close()
            conn.close()
    
    def extract_regex_entities(self, text: str) -> List[Dict[str, Any]]:
        """Extract entities using regex patterns."""
        entities = []
        
        for pattern_name, pattern in self.regex_patterns.items():
            matches = pattern.findall(text)
            
            for match in matches:
                # Determine category based on pattern name
                if pattern_name == 'cve':
                    category = 'cve'
                    confidence = 0.95
                elif pattern_name.startswith('file_hash'):
                    category = 'file_hash'
                    confidence = 0.9
                elif pattern_name == 'ip_address':
                    category = 'ip_address'
                    confidence = 0.85
                elif pattern_name == 'domain':
                    category = 'domain'
                    confidence = 0.8
                elif pattern_name in ['bitcoin_address', 'ethereum_address']:
                    category = 'cryptocurrency'
                    confidence = 0.9
                elif pattern_name in ['windows_file_path', 'unix_file_path']:
                    category = 'file_path'
                    confidence = 0.75
                elif pattern_name == 'registry_key':
                    category = 'registry_key'
                    confidence = 0.85
                elif pattern_name == 'email':
                    category = 'email'
                    confidence = 0.85
                else:
                    category = pattern_name
                    confidence = 0.7
                
                # Validate entity
                if self.validator.validate_entity(match, category):
                    entities.append({
                        'entity_name': match,
                        'entity_category': category,
                        'confidence': confidence,
                        'extraction_method': 'regex'
                    })
        
        return entities
    
    def extract_predefined_entities(self, text: str) -> List[Dict[str, Any]]:
        """Extract predefined entities from text including synonyms."""
        entities = []
        seen_entities = set()  # Track entities already found to avoid duplicates
        
        for category, entity_list in self.predefined_entities.items():
            for entity_data in entity_list:
                # Check all patterns (main name + synonyms)
                for pattern_info in entity_data['patterns']:
                    if pattern_info['pattern'].search(text):
                        # Only add if we haven't seen this entity ID yet
                        if entity_data['id'] not in seen_entities:
                            entities.append({
                                'entity_name': entity_data['name'],  # Always use primary name
                                'entity_category': category,
                                'entities_id': entity_data['id'],
                                'confidence': 0.95,
                                'extraction_method': 'predefined',
                                'matched_text': pattern_info['matched_name']  # Track what was actually matched
                            })
                            seen_entities.add(entity_data['id'])
                            break  # Move to next entity after first match
        
        return entities
    
    def calculate_position_boost(self, entity: str, title: str, content: str) -> float:
        """Calculate confidence boost based on entity position."""
        boost = 0.0
        
        # Check if entity is in title
        if entity.lower() in title.lower():
            boost += 0.05
        
        # Check if entity appears multiple times
        total_text = title + " " + content
        occurrences = len(re.findall(re.escape(entity), total_text, re.IGNORECASE))
        if occurrences > 1:
            boost += min(0.03 * (occurrences - 1), 0.1)  # Cap at 0.1
        
        return boost
    
    def extract_entities_from_article(self, article: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all entities from a single article."""
        # Extract title and content
        title_data = article['rss_feeds_clean_title']
        content_data = article['rss_feeds_clean_content']
        
        title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
        content = content_data.get('content', '') if isinstance(content_data, dict) else str(content_data)
        
        # Combine title and content for extraction
        full_text = f"{title}\n\n{content}"
        
        # Extract entities using both methods
        regex_entities = self.extract_regex_entities(full_text)
        predefined_entities = self.extract_predefined_entities(full_text)
        
        # Combine and deduplicate
        all_entities = []
        seen = set()
        
        for entity in regex_entities + predefined_entities:
            # Create unique key
            key = (entity['entity_name'].lower(), entity['entity_category'])
            
            if key not in seen:
                # Calculate position boost
                position_boost = self.calculate_position_boost(
                    entity['entity_name'], title, content
                )
                entity['confidence'] = min(entity['confidence'] + position_boost, 1.0)
                
                # Determine position
                if entity['entity_name'].lower() in title.lower():
                    entity['position'] = 'title'
                else:
                    entity['position'] = 'content'
                
                all_entities.append(entity)
                seen.add(key)
                
                # Update statistics
                self.stats['entity_categories'][entity['entity_category']] += 1
        
        self.stats['entities_extracted'] += len(all_entities)
        
        return all_entities
    
    def discover_new_dynamic_entities(self, entities: List[Dict[str, Any]], 
                                    cursor: psycopg2.extensions.cursor) -> int:
        """Discover and store new dynamic entities."""
        new_entities = 0
        
        # Filter for dynamic entities (extracted via regex)
        dynamic_entities = [e for e in entities if e.get('extraction_method') == 'regex' 
                          and 'entities_id' not in e]
        
        for entity in dynamic_entities:
            # Check if entity already exists
            cursor.execute("""
                SELECT entities_id FROM cluster_data.entities
                WHERE entities_name = %s AND entities_category = %s
            """, (entity['entity_name'], entity['entity_category']))
            
            existing = cursor.fetchone()
            
            if not existing:
                # Get importance weight for category
                weight = self.dynamic_weights.get(entity['entity_category'], 50)
                
                # Truncate entity name if too long
                entity_name = entity['entity_name']
                if len(entity_name) > 500:
                    # For file paths and registry keys, keep the end which is usually more specific
                    if entity['entity_category'] in ['file_path', 'registry_key']:
                        entity_name = '...' + entity_name[-497:]
                    else:
                        entity_name = entity_name[:497] + '...'
                    logger.warning("truncated_entity_name",
                                 original_length=len(entity['entity_name']),
                                 category=entity['entity_category'])
                
                # Insert new entity
                cursor.execute("""
                    INSERT INTO cluster_data.entities
                    (entities_name, entities_category, entities_source, entities_importance_weight)
                    VALUES (%s, %s, %s, %s)
                    RETURNING entities_id
                """, (entity_name, entity['entity_category'], 'dynamic', weight))
                
                entity_id = cursor.fetchone()[0]
                entity['entities_id'] = entity_id
                new_entities += 1
                
                logger.info("discovered_new_entity",
                          name=entity['entity_name'],
                          category=entity['entity_category'])
            else:
                entity['entities_id'] = existing[0]
        
        return new_entities
    
    def update_article_entities(self, article_id: int, entities: List[Dict[str, Any]], 
                              cursor: psycopg2.extensions.cursor):
        """Update article with extracted entities."""
        # Prepare entity data for storage
        entity_data = {
            'entities': entities,
            'extraction_timestamp': datetime.now(timezone.utc).isoformat(),
            'entity_count': len(entities),
            'categories': list(set(e['entity_category'] for e in entities))
        }
        
        # Update article
        cursor.execute("""
            UPDATE cluster_data.rss_feeds_clean
            SET rss_feeds_clean_extracted_entities = %s
            WHERE rss_feeds_clean_id = %s
        """, (json.dumps(entity_data), article_id))
    
    def process_batch(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Process a batch of articles for entity extraction."""
        logger.info("starting_batch_processing", limit=limit or self.batch_size)
        start_time = time.time()
        
        # Get unprocessed articles
        articles = self.get_unprocessed_articles(limit)
        if not articles:
            logger.info("no_unprocessed_articles")
            return self.stats
        
        # Connect to database
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            for i, article in enumerate(articles):
                # Create savepoint for this article
                cursor.execute("SAVEPOINT article_processing")
                
                try:
                    # Extract entities
                    entities = self.extract_entities_from_article(article)
                    
                    # Discover new dynamic entities
                    new_count = self.discover_new_dynamic_entities(entities, cursor)
                    self.stats['new_entities_discovered'] += new_count
                    
                    # Update article with entities
                    self.update_article_entities(article['rss_feeds_clean_id'], entities, cursor)
                    
                    # Release savepoint on success
                    cursor.execute("RELEASE SAVEPOINT article_processing")
                    
                    self.stats['articles_processed'] += 1
                    
                    # Log progress
                    if (i + 1) % 10 == 0:
                        logger.info("batch_progress",
                                  processed=i+1,
                                  total=len(articles))
                    
                except Exception as e:
                    # Rollback to savepoint on error
                    cursor.execute("ROLLBACK TO SAVEPOINT article_processing")
                    logger.error("article_processing_error",
                               article_id=article['rss_feeds_clean_id'],
                               error=str(e))
                    self.stats['extraction_errors'] += 1
            
            # Commit all successful changes
            conn.commit()
            logger.info("batch_processing_completed", articles_processed=self.stats['articles_processed'])
            
        except Exception as e:
            logger.error("batch_processing_error", error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
        
        # Calculate processing time
        self.stats['processing_time_seconds'] = round(time.time() - start_time, 2)
        
        return self.stats


def main():
    """Run entity extraction as standalone script."""
    import argparse
    
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Entity Extraction from Articles')
    parser.add_argument('--limit', type=int, help='Limit number of articles to process')
    parser.add_argument('--test', action='store_true', help='Test with single article')
    
    args = parser.parse_args()
    
    extractor = EntityExtractor()
    
    if args.test:
        # Test mode - process just one article
        articles = extractor.get_unprocessed_articles(limit=1)
        if articles:
            entities = extractor.extract_entities_from_article(articles[0])
            print("\nTest Entity Extraction Result:")
            print("=" * 50)
            print(f"Article: {articles[0]['rss_feeds_clean_title']}")
            print(f"Entities found: {len(entities)}")
            print("\nEntities by category:")
            by_category = defaultdict(list)
            for entity in entities:
                by_category[entity['entity_category']].append(entity['entity_name'])
            for category, names in by_category.items():
                print(f"  {category}: {', '.join(names[:5])}")
                if len(names) > 5:
                    print(f"    ... and {len(names) - 5} more")
    else:
        # Normal batch processing
        stats = extractor.process_batch(limit=args.limit)
        
        # Print summary
        print("\nEntity Extraction Summary:")
        print("=" * 50)
        print(f"Articles processed: {stats['articles_processed']}")
        print(f"Total entities extracted: {stats['entities_extracted']}")
        print(f"New entities discovered: {stats['new_entities_discovered']}")
        print(f"Extraction errors: {stats['extraction_errors']}")
        print(f"Processing time: {stats.get('processing_time_seconds', 0)} seconds")
        
        print("\nEntities by category:")
        for category, count in sorted(stats['entity_categories'].items(), 
                                    key=lambda x: x[1], reverse=True):
            print(f"  {category}: {count}")


if __name__ == "__main__":
    main()