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
        try:
            data = json.loads(json_content)
            
            # Validate structure
            if not isinstance(data, dict):
                logger.error("invalid_misp_format", 
                           error="Expected dict at root level")
                return []
            
            # Get values array (contains threat actors)
            values = data.get('values', [])
            if not values:
                logger.warning("no_threat_actors_found")
                return []
            
            logger.info("parsing_threat_actors", count=len(values))
            
            parsed_entities = []
            for actor_data in values:
                parsed = self._parse_single_actor(actor_data)
                if parsed:
                    parsed_entities.append(parsed)
            
            logger.info("threat_actors_parsed", 
                       total=len(values),
                       parsed=len(parsed_entities))
            
            return parsed_entities
            
        except json.JSONDecodeError as e:
            logger.error("json_decode_error", error=str(e))
            return []
        except Exception as e:
            logger.error("parse_error", error=str(e))
            return []
    
    def _parse_single_actor(self, actor_data: Dict) -> Optional[Dict]:
        """
        Parse a single threat actor entry.
        
        Args:
            actor_data: Single threat actor dict from MISP
            
        Returns:
            Parsed entity dict or None if invalid
        """
        try:
            # Extract required fields
            name = actor_data.get('value')
            if not name:
                logger.warning("missing_actor_name", data=actor_data)
                return None
            
            # Extract metadata
            meta = actor_data.get('meta', {})
            
            # Calculate importance weight based on available data
            importance = self._calculate_importance(actor_data)
            
            # Build the entity
            entity = {
                'entities_name': name,
                'entities_category': 'apt_group',
                'entities_source': 'misp',
                'entities_importance_weight': importance,
                'entities_json': actor_data  # Store complete JSON
            }
            
            return entity
            
        except Exception as e:
            logger.error("actor_parse_error", 
                        error=str(e),
                        actor=actor_data.get('value', 'unknown'))
            return None
    
    def _calculate_importance(self, actor_data: Dict) -> int:
        """
        Calculate importance weight for a threat actor.
        
        Based on:
        - Number of synonyms (aliases)
        - Number of references
        - Number of targeted sectors
        - Attribution confidence
        - Related actors
        
        Args:
            actor_data: Threat actor data
            
        Returns:
            Importance weight (1-100)
        """
        weight = 50  # Base weight
        meta = actor_data.get('meta', {})
        
        # More synonyms = more notable
        synonyms = meta.get('synonyms', [])
        if synonyms:
            weight += min(len(synonyms) * 3, 15)  # Max +15
        
        # More references = more documented
        refs = meta.get('refs', [])
        if refs:
            weight += min(len(refs) * 2, 10)  # Max +10
        
        # More targeted sectors = broader impact
        sectors = meta.get('targeted-sector', [])
        if sectors:
            weight += min(len(sectors) * 2, 10)  # Max +10
        
        # Higher attribution confidence
        attr_confidence = meta.get('attribution-confidence', '0')
        try:
            confidence = int(attr_confidence)
            if confidence >= 90:
                weight += 10
            elif confidence >= 70:
                weight += 5
        except (ValueError, TypeError):
            pass
        
        # Has related actors
        related = actor_data.get('related', [])
        if related:
            weight += 5
        
        # Cap at 100
        return min(weight, 100)
    
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