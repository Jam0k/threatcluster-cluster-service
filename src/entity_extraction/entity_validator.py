#!/usr/bin/env python3
"""
Entity Validator Module

Validates and filters extracted entities to ensure quality and relevance.
"""
import re
import ipaddress
from typing import Optional, Set
import tldextract


class EntityValidator:
    """Validates extracted entities for quality and relevance."""
    
    def __init__(self):
        """Initialize the entity validator."""
        # Common false positive patterns
        self.false_positive_patterns = {
            'domain': [
                r'^example\.com$',
                r'^test\.com$',
                r'^localhost$',
                r'^domain\.com$',
                r'^\d+\.\d+\.\d+$',  # Version numbers
                # Popular domains that are not IOCs
                r'^google\.com$',
                r'^microsoft\.com$',
                r'^apple\.com$',
                r'^amazon\.com$',
                r'^github\.com$',
                r'^stackoverflow\.com$',
                # Social media
                r'^facebook\.com$',
                r'^twitter\.com$',
                r'^instagram\.com$',
                r'^linkedin\.com$',
                r'^youtube\.com$',
                r'^tiktok\.com$',
                r'^reddit\.com$',
                # CDNs and infrastructure
                r'^cloudflare\.com$',
                r'^akamai\.com$',
                r'^fastly\.com$',
                r'^amazonaws\.com$',
                # Email providers
                r'^gmail\.com$',
                r'^outlook\.com$',
                r'^yahoo\.com$',
                r'^hotmail\.com$',
                # Other common services
                r'^wikipedia\.org$',
                r'^wordpress\.com$',
                r'^adobe\.com$',
                r'^oracle\.com$'
            ],
            'ip_address': [
                r'^0\.0\.0\.0$',
                r'^127\.0\.0\.1$',
                r'^255\.255\.255\.255$',
                r'^1\.1\.1\.1$'  # Cloudflare DNS
            ],
            'file_hash': [
                r'^0{32}$',  # All zeros MD5
                r'^0{40}$',  # All zeros SHA1
                r'^0{64}$',  # All zeros SHA256
                r'^[fF]{32}$',  # All F's MD5
                r'^[fF]{40}$',  # All F's SHA1
                r'^[fF]{64}$'   # All F's SHA256
            ]
        }
        
        # Compile false positive patterns
        self.compiled_false_positives = {}
        for category, patterns in self.false_positive_patterns.items():
            self.compiled_false_positives[category] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Private IP ranges
        self.private_ip_networks = [
            ipaddress.ip_network('10.0.0.0/8'),
            ipaddress.ip_network('172.16.0.0/12'),
            ipaddress.ip_network('192.168.0.0/16'),
            ipaddress.ip_network('169.254.0.0/16'),  # Link-local
            ipaddress.ip_network('127.0.0.0/8'),     # Loopback
            ipaddress.ip_network('224.0.0.0/4'),     # Multicast
            ipaddress.ip_network('240.0.0.0/4')      # Reserved
        ]
        
        # Common code/documentation patterns to exclude
        self.code_context_patterns = [
            r'```[\s\S]*?```',  # Markdown code blocks
            r'<code>[\s\S]*?</code>',  # HTML code blocks
            r'<pre>[\s\S]*?</pre>',  # Preformatted text
            r'example:[\s\S]*?\n\n',  # Example sections
            r'demo:[\s\S]*?\n\n'  # Demo sections
        ]
    
    def validate_entity(self, entity: str, category: str) -> bool:
        """Validate an entity based on its category."""
        if not entity or not isinstance(entity, str):
            return False
        
        # Remove surrounding whitespace
        entity = entity.strip()
        
        # Category-specific validation
        validators = {
            'cve': self.validate_cve,
            'ip_address': self.validate_ip_address,
            'domain': self.validate_domain,
            'file_hash': self.validate_file_hash,
            'email': self.validate_email,
            'bitcoin_address': self.validate_bitcoin_address,
            'ethereum_address': self.validate_ethereum_address,
            'file_path': self.validate_file_path,
            'registry_key': self.validate_registry_key
        }
        
        validator = validators.get(category)
        if validator:
            return validator(entity)
        
        # Default validation for other categories
        return self.validate_default(entity)
    
    def validate_cve(self, cve: str) -> bool:
        """Validate CVE identifier format."""
        # Standard CVE format: CVE-YYYY-NNNN(N)
        pattern = r'^CVE-\d{4}-\d{4,}$'
        if not re.match(pattern, cve, re.IGNORECASE):
            return False
        
        # Extract year
        try:
            year = int(cve.split('-')[1])
            # CVEs started in 1999
            if year < 1999 or year > 2030:  # Allow some future years
                return False
        except (IndexError, ValueError):
            return False
        
        return True
    
    def validate_ip_address(self, ip: str) -> bool:
        """Validate IP address and filter out private/reserved ranges."""
        try:
            # First check if it's a valid IP format
            ip_obj = ipaddress.ip_address(ip)
            
            # Check if it's IPv4 (IPv6 support could be added later)
            if not isinstance(ip_obj, ipaddress.IPv4Address):
                return False
            
            # Check against false positives
            for pattern in self.compiled_false_positives.get('ip_address', []):
                if pattern.match(ip):
                    return False
            
            # Check if it's a private IP
            for private_network in self.private_ip_networks:
                if ip_obj in private_network:
                    return False
            
            return True
            
        except (ipaddress.AddressValueError, ValueError):
            # Invalid IP address format (e.g., "999.999.999.999")
            return False
    
    def validate_domain(self, domain: str) -> bool:
        """Validate domain name format and TLD."""
        # Basic format check
        if len(domain) < 4 or len(domain) > 253:
            return False
        
        # Check against false positives
        for pattern in self.compiled_false_positives.get('domain', []):
            if pattern.match(domain):
                return False
        
        # Use tldextract for proper domain parsing
        extracted = tldextract.extract(domain)
        
        # Must have both domain and TLD
        if not extracted.domain or not extracted.suffix:
            return False
        
        # Check for valid TLD (suffix)
        if len(extracted.suffix) < 2:
            return False
        
        # Exclude domains that are just numbers
        if extracted.domain.isdigit():
            return False
        
        # Exclude single-letter domains (likely false positives)
        if len(extracted.domain) == 1:
            return False
        
        return True
    
    def validate_file_hash(self, hash_str: str) -> bool:
        """Validate file hash format."""
        # Check length and hexadecimal format
        hash_lengths = {
            32: 'md5',
            40: 'sha1',
            64: 'sha256'
        }
        
        if len(hash_str) not in hash_lengths:
            return False
        
        # Must be hexadecimal
        try:
            int(hash_str, 16)
        except ValueError:
            return False
        
        # Check against false positives
        for pattern in self.compiled_false_positives.get('file_hash', []):
            if pattern.match(hash_str):
                return False
        
        # Exclude hashes that are all the same character
        if len(set(hash_str.lower())) == 1:
            return False
        
        return True
    
    def validate_email(self, email: str) -> bool:
        """Validate email address format."""
        # Basic email pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            return False
        
        # Exclude common false positives
        false_positives = ['user@example.com', 'test@test.com', 'admin@domain.com']
        if email.lower() in false_positives:
            return False
        
        return True
    
    def validate_bitcoin_address(self, address: str) -> bool:
        """Validate Bitcoin address format."""
        # Bitcoin addresses are 26-35 characters, start with 1, 3, or bc1
        if len(address) < 26 or len(address) > 42:
            return False
        
        # Legacy addresses start with 1 or 3
        if address[0] in ['1', '3']:
            # Base58 characters only
            base58_pattern = r'^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]+$'
            return bool(re.match(base58_pattern, address))
        
        # Bech32 addresses start with bc1
        elif address.startswith('bc1'):
            # Bech32 characters only
            bech32_pattern = r'^bc1[023456789acdefghjklmnpqrstuvwxyz]+$'
            return bool(re.match(bech32_pattern, address))
        
        return False
    
    def validate_ethereum_address(self, address: str) -> bool:
        """Validate Ethereum address format."""
        # Ethereum addresses are 42 characters (0x + 40 hex chars)
        if len(address) != 42:
            return False
        
        if not address.startswith('0x'):
            return False
        
        # Check if remaining characters are hexadecimal
        try:
            int(address[2:], 16)
        except ValueError:
            return False
        
        # Exclude all zeros address
        if address[2:] == '0' * 40:
            return False
        
        return True
    
    def validate_file_path(self, path: str) -> bool:
        """Validate file path format."""
        # Length checks
        if len(path) < 3 or len(path) > 500:
            return False
        
        # Windows path
        if re.match(r'^[A-Za-z]:\\', path):
            # Check for invalid characters
            invalid_chars = '<>"|?*'
            if any(char in path for char in invalid_chars):
                return False
            return True
        
        # Unix path
        elif path.startswith('/'):
            # Check for null bytes
            if '\0' in path:
                return False
            return True
        
        return False
    
    def validate_registry_key(self, key: str) -> bool:
        """Validate Windows registry key format."""
        # Length check
        if len(key) > 500:
            return False
            
        # Must start with a valid hive
        valid_hives = [
            'HKEY_LOCAL_MACHINE', 'HKEY_CURRENT_USER', 'HKEY_CLASSES_ROOT',
            'HKEY_USERS', 'HKEY_CURRENT_CONFIG', 'HKLM', 'HKCU', 'HKCR'
        ]
        
        if not any(key.upper().startswith(hive) for hive in valid_hives):
            return False
        
        # Must have at least one subkey
        if '\\' not in key:
            return False
        
        return True
    
    def validate_default(self, entity: str) -> bool:
        """Default validation for entities without specific validators."""
        # Basic length check - maximum 500 chars to fit database column
        if len(entity) < 2 or len(entity) > 500:
            return False
        
        # Must contain at least one alphanumeric character
        if not re.search(r'[a-zA-Z0-9]', entity):
            return False
        
        return True
    
    def is_in_code_context(self, entity: str, surrounding_text: str) -> bool:
        """Check if entity appears within code or example context."""
        for pattern in self.code_context_patterns:
            matches = re.finditer(pattern, surrounding_text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                if entity in match.group():
                    return True
        return False