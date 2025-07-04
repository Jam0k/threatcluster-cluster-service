"""
IOC Validator Module

Validates and filters IOCs to reduce noise and false positives.
"""
import re
import ipaddress
from typing import List, Set, Tuple, Optional
import structlog

logger = structlog.get_logger(__name__)


class IOCValidator:
    """Validates and filters IOCs."""
    
    def __init__(self):
        """Initialize validator with filtering rules."""
        # Private/local IP ranges to filter out
        self.private_networks = [
            ipaddress.ip_network('10.0.0.0/8'),
            ipaddress.ip_network('172.16.0.0/12'),
            ipaddress.ip_network('192.168.0.0/16'),
            ipaddress.ip_network('127.0.0.0/8'),
            ipaddress.ip_network('169.254.0.0/16'),  # Link-local
            ipaddress.ip_network('224.0.0.0/4'),     # Multicast
            ipaddress.ip_network('255.255.255.255/32'),  # Broadcast
            ipaddress.ip_network('0.0.0.0/8'),       # Reserved
        ]
        
        # Common internal/test domains to filter
        self.filtered_domains = {
            'localhost', 'local', 'internal', 'test', 'example.com',
            'example.org', 'example.net', 'invalid', 'localhost.localdomain',
            'broadcasthost', 'ip6-localhost', 'ip6-loopback',
            '0.0.0.0', '127.0.0.1', '255.255.255.255'
        }
        
        # Top-level domains that are typically not malicious
        self.filtered_tlds = {
            'local', 'localhost', 'home', 'corp', 'lan', 'internal'
        }
        
        # Valid hash patterns
        self.hash_patterns = {
            'md5': re.compile(r'^[a-fA-F0-9]{32}$'),
            'sha1': re.compile(r'^[a-fA-F0-9]{40}$'),
            'sha256': re.compile(r'^[a-fA-F0-9]{64}$')
        }
    
    def validate_iocs(self, iocs: List[str], ioc_type: str) -> List[str]:
        """
        Validate and filter a list of IOCs.
        
        Args:
            iocs: List of IOCs to validate
            ioc_type: Type of IOCs ('ip_address', 'domain', 'file_hash')
            
        Returns:
            List of valid IOCs
        """
        if ioc_type == 'ip_address':
            return self._validate_ips(iocs)
        elif ioc_type == 'domain':
            return self._validate_domains(iocs)
        elif ioc_type == 'file_hash':
            return self._validate_hashes(iocs)
        else:
            logger.error("unknown_ioc_type", ioc_type=ioc_type)
            return []
    
    def _validate_ips(self, ips: List[str]) -> List[str]:
        """Validate IP addresses and filter out private/local IPs."""
        valid_ips = []
        
        for ip_str in ips:
            ip_str = ip_str.strip()
            
            try:
                # Parse IP address
                ip = ipaddress.ip_address(ip_str)
                
                # Skip IPv6 for now (can be enabled later)
                if isinstance(ip, ipaddress.IPv6Address):
                    logger.debug("skipping_ipv6", ip=str(ip))
                    continue
                
                # Check if it's a private/local IP
                is_private = any(ip in network for network in self.private_networks)
                
                if is_private:
                    logger.debug("filtered_private_ip", ip=str(ip))
                    continue
                
                # Additional checks
                if ip.is_multicast or ip.is_reserved or ip.is_loopback:
                    logger.debug("filtered_special_ip", 
                               ip=str(ip),
                               multicast=ip.is_multicast,
                               reserved=ip.is_reserved,
                               loopback=ip.is_loopback)
                    continue
                
                valid_ips.append(str(ip))
                
            except ValueError as e:
                logger.debug("invalid_ip_format", ip=ip_str, error=str(e))
                continue
        
        logger.info("ip_validation_complete", 
                   input_count=len(ips), 
                   valid_count=len(valid_ips),
                   filtered_count=len(ips) - len(valid_ips))
        
        return valid_ips
    
    def _validate_domains(self, domains: List[str]) -> List[str]:
        """Validate domains and filter out internal/test domains."""
        valid_domains = []
        
        # Domain validation regex
        domain_regex = re.compile(
            r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*'
            r'[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'
        )
        
        for domain in domains:
            domain = domain.strip().lower()
            
            # Remove protocol if present
            domain = re.sub(r'^https?://', '', domain)
            # Remove path if present
            domain = domain.split('/')[0]
            # Remove port if present
            domain = domain.split(':')[0]
            
            # Skip empty
            if not domain:
                continue
            
            # Check against filtered list
            if domain in self.filtered_domains:
                logger.debug("filtered_domain_blacklist", domain=domain)
                continue
            
            # Check TLD
            parts = domain.split('.')
            if len(parts) >= 2:
                tld = parts[-1]
                if tld in self.filtered_tlds:
                    logger.debug("filtered_domain_tld", domain=domain, tld=tld)
                    continue
            
            # Validate format
            if not domain_regex.match(domain):
                logger.debug("invalid_domain_format", domain=domain)
                continue
            
            # Check minimum length and structure
            if len(domain) < 3 or '.' not in domain:
                logger.debug("invalid_domain_structure", domain=domain)
                continue
            
            # Filter out IP addresses mistaken as domains
            if self._looks_like_ip(domain):
                logger.debug("filtered_ip_as_domain", domain=domain)
                continue
            
            valid_domains.append(domain)
        
        logger.info("domain_validation_complete", 
                   input_count=len(domains), 
                   valid_count=len(valid_domains),
                   filtered_count=len(domains) - len(valid_domains))
        
        return valid_domains
    
    def _validate_hashes(self, hashes: List[str]) -> List[str]:
        """Validate file hashes."""
        valid_hashes = []
        
        for hash_str in hashes:
            hash_str = hash_str.strip().upper()
            
            # Skip empty
            if not hash_str:
                continue
            
            # Check against known patterns
            hash_type = None
            for h_type, pattern in self.hash_patterns.items():
                if pattern.match(hash_str):
                    hash_type = h_type
                    break
            
            if not hash_type:
                logger.debug("invalid_hash_format", 
                           hash=hash_str[:10] + "...", 
                           length=len(hash_str))
                continue
            
            # Filter out all zeros or all F's hashes (common test values)
            if hash_str == '0' * len(hash_str) or hash_str == 'F' * len(hash_str):
                logger.debug("filtered_test_hash", hash=hash_str[:10] + "...")
                continue
            
            valid_hashes.append(hash_str)
        
        logger.info("hash_validation_complete", 
                   input_count=len(hashes), 
                   valid_count=len(valid_hashes),
                   filtered_count=len(hashes) - len(valid_hashes))
        
        return valid_hashes
    
    def _looks_like_ip(self, value: str) -> bool:
        """Check if a string looks like an IP address."""
        # Simple check for IPv4 pattern
        parts = value.split('.')
        if len(parts) == 4:
            try:
                return all(0 <= int(part) <= 255 for part in parts)
            except ValueError:
                pass
        return False
    
    def deduplicate_iocs(self, iocs: List[str]) -> List[str]:
        """Remove duplicate IOCs while preserving order."""
        seen = set()
        unique = []
        
        for ioc in iocs:
            ioc_normalized = ioc.upper() if self._is_hash(ioc) else ioc.lower()
            if ioc_normalized not in seen:
                seen.add(ioc_normalized)
                unique.append(ioc)
        
        return unique
    
    def _is_hash(self, value: str) -> bool:
        """Check if value appears to be a hash."""
        return any(pattern.match(value.upper()) for pattern in self.hash_patterns.values())