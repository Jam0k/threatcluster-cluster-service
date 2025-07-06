"""
STIX Parser Module

Parses MITRE ATT&CK STIX data and extracts technique information.
"""
import json
from typing import Dict, Optional
import structlog

logger = structlog.get_logger(__name__)


class STIXParser:
    """Parser for MITRE ATT&CK STIX data."""
    
    def __init__(self):
        """Initialize the STIX parser."""
        pass
    
    def parse_mitre_stix(self, json_content: str) -> Dict[str, Dict]:
        """
        Parse MITRE ATT&CK STIX JSON and extract technique data.
        Returns a dictionary keyed by technique ID.
        
        Args:
            json_content: Raw JSON content from MITRE ATT&CK STIX
            
        Returns:
            Dict mapping technique IDs to their STIX data
        """
        try:
            data = json.loads(json_content)
            
            # Validate structure
            if not isinstance(data, dict) or data.get('type') != 'bundle':
                logger.error("invalid_stix_format", 
                           error="Expected STIX bundle at root level")
                return {}
            
            # Get objects array
            objects = data.get('objects', [])
            if not objects:
                logger.warning("no_objects_found")
                return {}
            
            # Extract techniques (attack-pattern objects)
            techniques = {}
            for obj in objects:
                if obj.get('type') == 'attack-pattern' and not obj.get('revoked', False):
                    technique_id = self._extract_technique_id(obj)
                    if technique_id:
                        techniques[technique_id] = obj
            
            logger.info("stix_techniques_parsed", 
                       total_objects=len(objects),
                       techniques_found=len(techniques))
            
            return techniques
            
        except json.JSONDecodeError as e:
            logger.error("json_decode_error", error=str(e))
            return {}
        except Exception as e:
            logger.error("parse_error", error=str(e))
            return {}
    
    def create_name_to_id_mapping(self, techniques: Dict[str, Dict]) -> Dict[str, str]:
        """
        Create a mapping from technique names to IDs.
        
        Args:
            techniques: Dict of technique IDs to STIX data
            
        Returns:
            Dict mapping technique names to IDs
        """
        name_to_id = {}
        for technique_id, stix_data in techniques.items():
            name = stix_data.get('name', '')
            if name:
                name_to_id[name.lower()] = technique_id
        return name_to_id
    
    def _extract_technique_id(self, technique_obj: Dict) -> Optional[str]:
        """
        Extract MITRE technique ID from STIX object.
        
        Args:
            technique_obj: STIX attack-pattern object
            
        Returns:
            Technique ID (e.g., "T1055.011") or None
        """
        try:
            # Look for MITRE ATT&CK external reference
            external_refs = technique_obj.get('external_references', [])
            for ref in external_refs:
                if ref.get('source_name') == 'mitre-attack' and 'external_id' in ref:
                    return ref['external_id']
            
            return None
            
        except Exception as e:
            logger.error("technique_id_extraction_error", 
                        error=str(e),
                        technique_name=technique_obj.get('name', 'unknown'))
            return None
    
    def extract_stix_fields(self, stix_obj: Dict) -> Dict:
        """
        Extract key fields from STIX object for easier access.
        
        Args:
            stix_obj: STIX attack-pattern object
            
        Returns:
            Dict with extracted fields
        """
        extracted = {
            'name': stix_obj.get('name', ''),
            'description': stix_obj.get('description', ''),
            'created': stix_obj.get('created'),
            'modified': stix_obj.get('modified'),
            'kill_chain_phases': [],
            'platforms': stix_obj.get('x_mitre_platforms', []),
            'data_sources': stix_obj.get('x_mitre_data_sources', []),
            'detection': stix_obj.get('x_mitre_detection', ''),
            'is_subtechnique': stix_obj.get('x_mitre_is_subtechnique', False),
            'version': stix_obj.get('x_mitre_version', ''),
            'references': []
        }
        
        # Extract kill chain phases
        for phase in stix_obj.get('kill_chain_phases', []):
            if phase.get('kill_chain_name') == 'mitre-attack':
                extracted['kill_chain_phases'].append(phase.get('phase_name', ''))
        
        # Extract external references
        for ref in stix_obj.get('external_references', []):
            if ref.get('source_name') != 'mitre-attack':  # Skip the main MITRE ref
                extracted['references'].append({
                    'source': ref.get('source_name', ''),
                    'description': ref.get('description', ''),
                    'url': ref.get('url', '')
                })
        
        return extracted
    
    def merge_with_misp_data(self, misp_data: Dict, stix_data: Dict) -> Dict:
        """
        Merge STIX data with existing MISP data.
        
        Args:
            misp_data: Existing MISP data
            stix_data: STIX data for the same technique
            
        Returns:
            Merged data with both sources
        """
        # Create a merged structure
        merged = {
            'sources': {
                'misp': misp_data,
                'stix': stix_data
            },
            # Use MISP data as primary, but include STIX fields
            'value': misp_data.get('value', ''),
            'uuid': misp_data.get('uuid', ''),
            'description': misp_data.get('description', stix_data.get('description', '')),
            'meta': misp_data.get('meta', {}),
            # Add STIX-specific data
            'stix_data': self.extract_stix_fields(stix_data) if stix_data else None
        }
        
        # If MISP doesn't have a description but STIX does, use STIX
        if not merged['description'] and stix_data:
            merged['description'] = stix_data.get('description', '')
        
        # Merge platforms if available from both sources
        misp_platforms = merged['meta'].get('platforms', [])
        stix_platforms = stix_data.get('x_mitre_platforms', []) if stix_data else []
        if stix_platforms and not misp_platforms:
            merged['meta']['platforms'] = stix_platforms
        
        # Add STIX-only fields to meta if not present
        if stix_data:
            if 'kill_chain_phases' not in merged['meta'] and 'kill_chain_phases' in stix_data:
                phases = []
                for phase in stix_data.get('kill_chain_phases', []):
                    if phase.get('kill_chain_name') == 'mitre-attack':
                        phases.append(phase.get('phase_name', ''))
                if phases:
                    merged['meta']['kill_chain_phases'] = phases
            
            if 'data_sources' not in merged['meta'] and 'x_mitre_data_sources' in stix_data:
                merged['meta']['data_sources'] = stix_data.get('x_mitre_data_sources', [])
            
            if 'detection' not in merged['meta'] and 'x_mitre_detection' in stix_data:
                merged['meta']['detection'] = stix_data.get('x_mitre_detection', '')
        
        return merged