"""
IOC Parser Module

Parses different feed formats to extract IOCs.
Supports:
- Zeek Intel format (tab-separated with headers)
- Plain text format (one IOC per line)
"""
import re
from typing import List, Dict, Optional, Set
import structlog

logger = structlog.get_logger(__name__)


class IOCParser:
    """Parses IOCs from various feed formats."""
    
    def __init__(self):
        """Initialize the parser."""
        # Zeek Intel format fields we care about
        self.zeek_fields = ['indicator', 'indicator_type', 'meta.desc', 'meta.source']
        
    def parse_feed(self, content: str, format_type: str, feed_type: str) -> List[str]:
        """
        Parse feed content based on format type.
        
        Args:
            content: Raw feed content
            format_type: Format type ('zeek_intel' or 'plain')
            feed_type: IOC type ('ip_address', 'domain', 'file_hash')
            
        Returns:
            List of extracted IOCs
        """
        if not content or not content.strip():
            logger.warning("empty_feed_content", format_type=format_type)
            return []
            
        if format_type == 'zeek_intel':
            return self._parse_zeek_intel(content, feed_type)
        elif format_type == 'plain':
            return self._parse_plain_text(content, feed_type)
        else:
            logger.error("unknown_format_type", format_type=format_type)
            return []
    
    def _parse_zeek_intel(self, content: str, feed_type: str) -> List[str]:
        """
        Parse Zeek Intel format.
        
        Format:
        #fields	indicator	indicator_type	meta.desc	meta.source
        192.168.1.1	Intel::ADDR	Malicious IP	abuse.ch
        """
        iocs = []
        lines = content.strip().split('\n')
        
        # Find the header line
        header_index = -1
        for i, line in enumerate(lines):
            if line.startswith('#fields'):
                header_index = i
                break
        
        if header_index == -1:
            logger.warning("zeek_intel_no_header_found")
            return []
        
        # Extract IOCs from data lines
        for line in lines[header_index + 1:]:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
                
            # Split by tabs
            fields = line.split('\t')
            if len(fields) >= 2:
                indicator = fields[0].strip()
                indicator_type = fields[1].strip() if len(fields) > 1 else ''
                
                # Validate indicator matches expected type
                if self._validate_zeek_indicator_type(indicator, indicator_type, feed_type):
                    iocs.append(indicator)
                else:
                    logger.debug("zeek_intel_type_mismatch", 
                               indicator=indicator, 
                               indicator_type=indicator_type,
                               expected_type=feed_type)
        
        logger.info("zeek_intel_parsed", 
                   total_lines=len(lines), 
                   extracted_iocs=len(iocs))
        return iocs
    
    def _parse_plain_text(self, content: str, feed_type: str) -> List[str]:
        """
        Parse plain text format (one IOC per line).
        """
        iocs = []
        lines = content.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#') or line.startswith('//'):
                continue
            
            # Some feeds may have additional data after the IOC
            # Extract just the IOC part
            ioc = line.split()[0] if ' ' in line else line
            ioc = ioc.split(',')[0] if ',' in ioc else ioc
            
            # Remove any quotes
            ioc = ioc.strip('"\'')
            
            if ioc:
                iocs.append(ioc)
        
        logger.info("plain_text_parsed", 
                   total_lines=len(lines), 
                   extracted_iocs=len(iocs))
        return iocs
    
    def _validate_zeek_indicator_type(self, indicator: str, 
                                    indicator_type: str, 
                                    expected_type: str) -> bool:
        """
        Validate that Zeek indicator type matches expected feed type.
        """
        type_mapping = {
            'ip_address': ['Intel::ADDR', 'Intel::SUBNET'],
            'domain': ['Intel::DOMAIN', 'Intel::URL', 'Intel::EMAIL'],
            'file_hash': ['Intel::FILE_HASH', 'Intel::MD5', 'Intel::SHA1', 'Intel::SHA256']
        }
        
        expected_types = type_mapping.get(expected_type, [])
        
        # If no indicator_type specified, do basic validation
        if not indicator_type:
            if expected_type == 'ip_address':
                return self._looks_like_ip(indicator)
            elif expected_type == 'domain':
                return self._looks_like_domain(indicator)
            elif expected_type == 'file_hash':
                return self._looks_like_hash(indicator)
        
        return indicator_type in expected_types
    
    def _looks_like_ip(self, value: str) -> bool:
        """Basic IP address pattern check."""
        # Simple pattern for IPv4
        ipv4_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        return bool(re.match(ipv4_pattern, value))
    
    def _looks_like_domain(self, value: str) -> bool:
        """Basic domain pattern check."""
        # Must have at least one dot and valid characters
        domain_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9-_.]*[a-zA-Z0-9]\.[a-zA-Z]{2,}$'
        return bool(re.match(domain_pattern, value))
    
    def _looks_like_hash(self, value: str) -> bool:
        """Basic hash pattern check."""
        # Check for common hash lengths (MD5, SHA1, SHA256)
        value = value.upper()
        if not re.match(r'^[A-F0-9]+$', value):
            return False
        return len(value) in [32, 40, 64]  # MD5, SHA1, SHA256