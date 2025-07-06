"""
MISP Parser Module

Parses MISP galaxy JSON format and extracts threat actor information.
"""
import json
from typing import Dict, List, Tuple, Optional
import structlog

logger = structlog.get_logger(__name__)


class MISPParser:
    """Parser for MISP galaxy threat actor data."""
    
    def __init__(self):
        """Initialize the MISP parser."""
        pass
    
    def parse_threat_actors(self, json_content: str) -> List[Dict]:
        """
        Parse MISP threat actor JSON and extract entities.
        
        Args:
            json_content: Raw JSON content from MISP galaxy
            
        Returns:
            List of parsed threat actor entities
        """
        return self._parse_misp_json(json_content, 'apt_group')
    
    def parse_malware_families(self, json_content: str) -> List[Dict]:
        """
        Parse MISP malware family JSON and extract entities.
        
        Args:
            json_content: Raw JSON content from MISP galaxy
            
        Returns:
            List of parsed malware family entities
        """
        return self._parse_misp_json(json_content, 'malware_family')
    
    def parse_ransomware_groups(self, json_content: str) -> List[Dict]:
        """
        Parse MISP ransomware group JSON and extract entities.
        
        Args:
            json_content: Raw JSON content from MISP galaxy
            
        Returns:
            List of parsed ransomware group entities
        """
        return self._parse_misp_json(json_content, 'ransomware_group')
    
    def parse_mitre_techniques(self, json_content: str) -> List[Dict]:
        """
        Parse MISP MITRE technique JSON and extract entities.
        
        Args:
            json_content: Raw JSON content from MISP galaxy
            
        Returns:
            List of parsed MITRE technique entities
        """
        return self._parse_misp_json(json_content, 'mitre')
    
    def extract_technique_id_from_value(self, value: str) -> Optional[str]:
        """
        Extract MITRE technique ID from the value field.
        Some MISP entries have IDs like "T1548" in their value field.
        
        Args:
            value: The value field from MISP data
            
        Returns:
            Technique ID if found, None otherwise
        """
        import re
        # Look for pattern like T1234 or T1234.001
        match = re.match(r'^(T\d{4}(?:\.\d{3})?)(?:\s|$)', value)
        if match:
            return match.group(1)
        return None
    
    def _parse_misp_json(self, json_content: str, entity_category: str) -> List[Dict]:
        """
        Parse MISP JSON and extract entities.
        
        Args:
            json_content: Raw JSON content from MISP galaxy
            entity_category: The category for these entities
            
        Returns:
            List of parsed entities
        """
        try:
            data = json.loads(json_content)
            
            # Validate structure
            if not isinstance(data, dict):
                logger.error("invalid_misp_format", 
                           error="Expected dict at root level")
                return []
            
            # Get values array (contains entities)
            values = data.get('values', [])
            if not values:
                logger.warning("no_entities_found", category=entity_category)
                return []
            
            logger.info("parsing_entities", count=len(values), category=entity_category)
            
            parsed_entities = []
            for entity_data in values:
                parsed = self._parse_single_entity(entity_data, entity_category)
                if parsed:
                    parsed_entities.append(parsed)
            
            logger.info("entities_parsed", 
                       total=len(values),
                       parsed=len(parsed_entities),
                       category=entity_category)
            
            return parsed_entities
            
        except json.JSONDecodeError as e:
            logger.error("json_decode_error", error=str(e))
            return []
        except Exception as e:
            logger.error("parse_error", error=str(e))
            return []
    
    def _parse_single_entity(self, entity_data: Dict, entity_category: str) -> Optional[Dict]:
        """
        Parse a single entity entry.
        
        Args:
            entity_data: Single entity dict from MISP
            entity_category: Category for this entity
            
        Returns:
            Parsed entity dict or None if invalid
        """
        try:
            # Extract required fields
            name = entity_data.get('value')
            if not name:
                logger.warning("missing_entity_name", data=entity_data, category=entity_category)
                return None
            
            # Extract metadata
            meta = entity_data.get('meta', {})
            
            # Default importance weight
            importance = 50
            
            # Build the entity
            entity = {
                'entities_name': name,
                'entities_category': entity_category,
                'entities_source': 'misp',
                'entities_importance_weight': importance,
                'entities_json': entity_data  # Store complete JSON
            }
            
            return entity
            
        except Exception as e:
            logger.error("entity_parse_error", 
                        error=str(e),
                        entity=entity_data.get('value', 'unknown'),
                        category=entity_category)
            return None
    
    
    def validate_actor(self, actor_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate a threat actor entry.
        
        Args:
            actor_data: Actor data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(actor_data, dict):
            return False, "Actor data must be a dictionary"
        
        if not actor_data.get('value'):
            return False, "Missing required 'value' field"
        
        # Validate name length
        name = actor_data.get('value', '')
        if len(name) > 500:  # Match entities_name column limit
            return False, f"Actor name too long: {len(name)} chars"
        
        return True, None