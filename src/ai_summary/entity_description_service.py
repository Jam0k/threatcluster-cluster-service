"""
Entity Description Service - Generates descriptions for AI-extracted entities using OpenAI
"""
import os
import logging
import asyncio
import json
import re
from typing import Dict, List, Optional
from openai import AsyncOpenAI
import psycopg2
from psycopg2.extras import RealDictCursor

from src.config.settings import settings

logger = logging.getLogger(__name__)


class EntityDescriptionService:
    """Service for generating descriptions for AI-extracted entities"""
    
    def __init__(self):
        """Initialize the Entity Description Service"""
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
        self.model = "gpt-4o-mini-search-preview"  # Using search-enabled model for accurate verification
        
    async def generate_entity_description(self, entity_name: str, entity_category: str) -> Optional[str]:
        """
        Generate a description for a single entity.
        
        Args:
            entity_name: The entity name
            entity_category: The entity category
            
        Returns:
            Description string or None if generation fails
        """
        # Skip categories that don't need descriptions
        skip_categories = ['cve', 'domain', 'ip_address', 'file_hash']
        if entity_category in skip_categories:
            return None
            
        # Create category-specific prompts
        category_prompts = {
            'platform': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what it is and its primary purpose.",
            'company': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what the company does.",
            'attack_type': f"Use web search to verify {entity_name} is a real attack technique, then write a 1-2 sentence description of how it works.",
            'vulnerability_type': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what type of vulnerability it is.",
            'mitre': f"Use web search to verify {entity_name} is a valid MITRE ATT&CK technique, then write a 1-2 sentence description of what it does.",
            'apt_group': f"Use web search to verify {entity_name} is a known threat actor group, then write a 1-2 sentence description of their activities.",
            'ransomware_group': f"Use web search to verify {entity_name} is a real ransomware group, then write a 1-2 sentence description of their operations.",
            'malware_family': f"Use web search to verify {entity_name} is real malware, then write a 1-2 sentence description of what it does.",
            'security_vendor': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of their products or services.",
            'government_agency': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what the agency does.",
            'country': f"Write a 1-2 sentence description of {entity_name} as a country.",
            'industry_sector': f"Write a 1-2 sentence description of the {entity_name} industry sector.",
            'security_standard': f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what the standard covers."
        }
        
        prompt = category_prompts.get(entity_category, 
            f"Use web search to verify {entity_name} exists, then write a 1-2 sentence description of what it is."
        )
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert writer creating concise, factual descriptions. Use web search to verify information and ensure accuracy. Keep descriptions to 1-2 sentences maximum. Focus on what the entity is and what it does. Be clear and direct."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100
            )
            
            description = response.choices[0].message.content.strip()
            
            # Remove URL citations that the search model might include
            # Pattern matches markdown links like ([domain](url)) or just (url)
            description = re.sub(r'\s*\([^\)]*(?:https?://|www\.)[^\)]*\)', '', description)
            description = re.sub(r'\s*\[[^\]]*\]\([^\)]*\)', '', description)
            
            # Clean up any double spaces left after removal
            description = re.sub(r'\s+', ' ', description).strip()
            
            # Remove any trailing unmatched closing parentheses or brackets
            # Count opening and closing parentheses/brackets
            open_count = description.count('(')
            close_count = description.count(')')
            if close_count > open_count:
                # Remove trailing closing parentheses if unmatched
                description = description.rstrip(')')
            
            # Clean up any trailing punctuation followed by parentheses
            description = re.sub(r'\.\)$', '.', description)
            
            # Ensure description isn't too long
            if len(description) > 500:
                description = description[:497] + "..."
                
            return description
            
        except Exception as e:
            logger.error(f"Error generating description for {entity_name}: {e}")
            return None
    
    async def generate_descriptions_batch(self, entities: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Generate descriptions for multiple entities in parallel.
        
        Args:
            entities: List of dicts with 'name' and 'category' keys
            
        Returns:
            Dictionary mapping entity_name|category to description
        """
        tasks = []
        entity_keys = []
        
        for entity in entities:
            name = entity['name']
            category = entity['category']
            
            # Skip categories that don't need descriptions
            skip_categories = ['cve', 'domain', 'ip_address', 'file_hash']
            if category in skip_categories:
                continue
            
            tasks.append(self.generate_entity_description(name, category))
            entity_keys.append(f"{name}|{category}")
        
        if not tasks:
            return {}
        
        # Run all tasks in parallel
        descriptions = await asyncio.gather(*tasks)
        
        # Create result dictionary
        result = {}
        for key, desc in zip(entity_keys, descriptions):
            if desc:
                result[key] = desc
        
        return result
    
    def update_entity_descriptions(self, descriptions: Dict[str, str]) -> Dict[str, int]:
        """
        Update entities in database with generated descriptions.
        
        Args:
            descriptions: Dictionary mapping entity_name|category to description
            
        Returns:
            Statistics on updates
        """
        if not descriptions:
            return {'updated': 0, 'errors': 0}
        
        conn = psycopg2.connect(settings.database_url)
        stats = {'updated': 0, 'errors': 0}
        
        try:
            with conn.cursor() as cur:
                for key, description in descriptions.items():
                    try:
                        name, category = key.rsplit('|', 1)
                        
                        # Create minimal entities_json with just description
                        entities_json = {
                            category: {
                                "basic_info": {
                                    "name": name,
                                    "description": description
                                }
                            }
                        }
                        
                        # Update entity only if it doesn't already have a description
                        cur.execute("""
                            UPDATE cluster_data.entities
                            SET entities_json = %s
                            WHERE entities_name = %s 
                            AND entities_category = %s
                            AND entities_source = 'ai_extracted'
                            AND (
                                entities_json IS NULL 
                                OR NOT (entities_json::jsonb ? %s)
                                OR NOT (entities_json::jsonb -> %s -> 'basic_info' ? 'description')
                                OR (entities_json::jsonb -> %s -> 'basic_info' ->> 'description') IS NULL
                                OR (entities_json::jsonb -> %s -> 'basic_info' ->> 'description') = ''
                            )
                        """, (json.dumps(entities_json), name, category, category, category, category, category))
                        
                        if cur.rowcount > 0:
                            stats['updated'] += 1
                            logger.info(f"Updated description for {name} ({category})")
                            
                    except Exception as e:
                        logger.error(f"Error updating entity {key}: {e}")
                        stats['errors'] += 1
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Database error during description updates: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return stats
    
    async def process_entities_without_descriptions(self, limit: int = 50) -> Dict[str, int]:
        """
        Find AI-extracted entities without descriptions and generate them.
        
        Args:
            limit: Maximum number of entities to process
            
        Returns:
            Statistics on processing
        """
        conn = psycopg2.connect(settings.database_url)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Find AI-extracted entities without descriptions (excluding technical identifiers)
                cur.execute("""
                    SELECT 
                        entities_name,
                        entities_category
                    FROM cluster_data.entities
                    WHERE entities_source = 'ai_extracted'
                    AND entities_category NOT IN ('cve', 'domain', 'ip_address', 'file_hash')
                    AND (
                        entities_json IS NULL 
                        OR NOT (entities_json::jsonb ? entities_category)
                        OR NOT (entities_json::jsonb -> entities_category -> 'basic_info' ? 'description')
                        OR (entities_json::jsonb -> entities_category -> 'basic_info' ->> 'description') IS NULL
                        OR (entities_json::jsonb -> entities_category -> 'basic_info' ->> 'description') = ''
                    )
                    ORDER BY entities_added_on DESC
                    LIMIT %s
                """, (limit,))
                
                entities = cur.fetchall()
                
                if not entities:
                    logger.info("No AI-extracted entities need descriptions")
                    return {'processed': 0, 'updated': 0, 'errors': 0}
                
                logger.info(f"Found {len(entities)} entities needing descriptions")
                
                # Prepare entity list
                entity_list = [
                    {'name': e['entities_name'], 'category': e['entities_category']}
                    for e in entities
                ]
                
                # Generate descriptions
                descriptions = await self.generate_descriptions_batch(entity_list)
                
                # Update database
                stats = self.update_entity_descriptions(descriptions)
                stats['processed'] = len(entities)
                
                return stats
                
        except Exception as e:
            logger.error(f"Error processing entities: {e}")
            return {'processed': 0, 'updated': 0, 'errors': 0}
        finally:
            conn.close()


# Test function
async def test_description_generation():
    """Test the entity description service"""
    service = EntityDescriptionService()
    
    # Test single entity
    print("Testing single entity description...")
    description = await service.generate_entity_description("visionOS", "platform")
    print(f"visionOS: {description}")
    
    # Test batch generation
    print("\nTesting batch generation...")
    test_entities = [
        {'name': 'Safari', 'category': 'platform'},
        {'name': 'Zero-Day', 'category': 'attack_type'},
        {'name': 'T1190', 'category': 'mitre'},
        {'name': 'Apple', 'category': 'company'}
    ]
    
    descriptions = await service.generate_descriptions_batch(test_entities)
    for key, desc in descriptions.items():
        print(f"{key}: {desc}")
    
    # Process actual entities
    print("\nProcessing entities without descriptions...")
    stats = await service.process_entities_without_descriptions(limit=10)
    print(f"Results: {stats}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_description_generation())