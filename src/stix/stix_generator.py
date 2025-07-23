"""
STIX 2.1 generator for ThreatCluster clusters
"""
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
import re
from stix2 import (
    Bundle, Report, Indicator, Malware, AttackPattern, 
    ThreatActor, Vulnerability, Identity, Relationship,
    ExternalReference, TLP_WHITE, TLP_GREEN
)

class STIXGenerator:
    """Generate STIX 2.1 bundles from cluster data"""
    
    def __init__(self):
        # MITRE ATT&CK pattern to extract technique IDs
        self.mitre_pattern = re.compile(r'T\d{4}(?:\.\d{3})?')
        
        # ThreatCluster identity
        self.threatcluster_identity = Identity(
            name="ThreatCluster",
            identity_class="system",
            description="ThreatCluster Threat Intelligence Platform",
            created_by_ref="identity--" + str(uuid.uuid4())
        )
    
    def generate_cluster_bundle(self, cluster_data: Dict[str, Any]) -> str:
        """
        Generate a STIX 2.1 bundle from cluster data
        
        Args:
            cluster_data: Dictionary containing cluster information including:
                - cluster_id
                - cluster_name
                - ai_summary (with ttps, key_insights, summary)
                - articles (list of articles in the cluster)
                - entities (extracted entities)
                - created_at
                
        Returns:
            JSON string of STIX 2.1 bundle
        """
        stix_objects = [self.threatcluster_identity]
        relationships = []
        
        # Create report object for the cluster
        report_id = f"report--{uuid.uuid4()}"
        
        # Extract external references from articles
        external_refs = []
        if 'articles' in cluster_data:
            for article in cluster_data['articles'][:5]:  # Limit to first 5 articles
                if article.get('url'):
                    external_refs.append(ExternalReference(
                        source_name=article.get('source', 'Unknown Source'),
                        url=article['url'],
                        description=article.get('title', '')[:200]
                    ))
        
        # Build description from AI summary
        description = ""
        if 'ai_summary' in cluster_data and cluster_data['ai_summary']:
            ai_summary = cluster_data['ai_summary']
            description = ai_summary.get('summary', '')
            
            # Add key insights to description
            if 'key_insights' in ai_summary:
                description += "\n\nKey Insights:\n"
                for insight in ai_summary['key_insights']:
                    description += f"• {insight}\n"
        
        # Create the main report
        report = Report(
            id=report_id,
            name=cluster_data.get('cluster_name', 'Unnamed Cluster'),
            description=description,
            published=cluster_data.get('created_at', datetime.utcnow()),
            object_refs=[],  # Will be populated with related objects
            external_references=external_refs,
            confidence=80,  # High confidence for AI-analyzed clusters
            lang="en",
            object_marking_refs=[TLP_WHITE]  # Can be adjusted based on content
        )
        
        # Process TTPs if available
        if 'ai_summary' in cluster_data and 'ttps' in cluster_data['ai_summary']:
            for ttp_desc in cluster_data['ai_summary']['ttps']:
                # Extract MITRE technique IDs
                mitre_ids = self.mitre_pattern.findall(ttp_desc)
                
                for mitre_id in mitre_ids:
                    attack_pattern = AttackPattern(
                        name=f"MITRE ATT&CK {mitre_id}",
                        description=ttp_desc,
                        external_references=[
                            ExternalReference(
                                source_name="mitre-attack",
                                external_id=mitre_id,
                                url=f"https://attack.mitre.org/techniques/{mitre_id.replace('.', '/')}/"
                            )
                        ]
                    )
                    stix_objects.append(attack_pattern)
                    report.object_refs.append(attack_pattern.id)
        
        # Process entities
        if 'entities' in cluster_data:
            entities = cluster_data['entities']
            
            # Process CVEs
            cves = [e for e in entities if e['category'] == 'cve']
            for cve in cves[:10]:  # Limit to 10 CVEs
                vulnerability = Vulnerability(
                    name=cve['name'],
                    description=f"Vulnerability mentioned in cluster: {cve['name']}",
                    external_references=[
                        ExternalReference(
                            source_name="cve",
                            external_id=cve['name'],
                            url=f"https://nvd.nist.gov/vuln/detail/{cve['name']}"
                        )
                    ]
                )
                stix_objects.append(vulnerability)
                report.object_refs.append(vulnerability.id)
            
            # Process threat actors
            threat_actors = [e for e in entities if e['category'] in ['apt_group', 'ransomware_group']]
            for actor in threat_actors[:5]:  # Limit to 5 actors
                threat_actor = ThreatActor(
                    name=actor['name'],
                    description=f"{actor['category'].replace('_', ' ').title()}: {actor['name']}",
                    threat_actor_types=["hacker"] if actor['category'] == 'apt_group' else ["criminal"],
                    sophistication="advanced" if actor['category'] == 'apt_group' else "intermediate"
                )
                stix_objects.append(threat_actor)
                report.object_refs.append(threat_actor.id)
            
            # Process malware
            malware_entities = [e for e in entities if e['category'] == 'malware_family']
            for malware_entity in malware_entities[:5]:  # Limit to 5 malware
                malware = Malware(
                    name=malware_entity['name'],
                    description=f"Malware family: {malware_entity['name']}",
                    malware_types=["unknown"],  # Could be enhanced with more specific types
                    is_family=True
                )
                stix_objects.append(malware)
                report.object_refs.append(malware.id)
            
            # Process indicators (IPs, domains, hashes)
            indicators = [e for e in entities if e['category'] in ['ip_address', 'domain', 'file_hash']]
            for indicator_entity in indicators[:10]:  # Limit to 10 indicators
                pattern = self._create_stix_pattern(indicator_entity)
                if pattern:
                    indicator = Indicator(
                        name=f"{indicator_entity['category']}: {indicator_entity['name']}",
                        description=f"Indicator extracted from cluster",
                        pattern=pattern,
                        pattern_type="stix",
                        valid_from=cluster_data.get('created_at', datetime.utcnow())
                    )
                    stix_objects.append(indicator)
                    report.object_refs.append(indicator.id)
        
        # Add the report
        stix_objects.append(report)
        
        # Create relationships between threat actors and malware/attack patterns
        # This is simplified - could be enhanced with more sophisticated relationship detection
        threat_actor_ids = [obj.id for obj in stix_objects if obj.type == "threat-actor"]
        malware_ids = [obj.id for obj in stix_objects if obj.type == "malware"]
        attack_pattern_ids = [obj.id for obj in stix_objects if obj.type == "attack-pattern"]
        
        for actor_id in threat_actor_ids:
            for malware_id in malware_ids:
                rel = Relationship(
                    relationship_type="uses",
                    source_ref=actor_id,
                    target_ref=malware_id,
                    description="Threat actor uses malware"
                )
                relationships.append(rel)
                report.object_refs.append(rel.id)
            
            for pattern_id in attack_pattern_ids:
                rel = Relationship(
                    relationship_type="uses",
                    source_ref=actor_id,
                    target_ref=pattern_id,
                    description="Threat actor uses technique"
                )
                relationships.append(rel)
                report.object_refs.append(rel.id)
        
        # Add relationships to objects
        stix_objects.extend(relationships)
        
        # Create bundle
        bundle = Bundle(objects=stix_objects)
        
        return bundle.serialize(pretty=True)
    
    def _create_stix_pattern(self, entity: Dict[str, str]) -> Optional[str]:
        """Create STIX pattern from entity"""
        entity_type = entity['category']
        value = entity['name']
        
        if entity_type == 'ip_address':
            # Determine if IPv4 or IPv6
            if ':' in value:
                return f"[ipv6-addr:value = '{value}']"
            else:
                return f"[ipv4-addr:value = '{value}']"
        elif entity_type == 'domain':
            return f"[domain-name:value = '{value}']"
        elif entity_type == 'file_hash':
            # Try to determine hash type by length
            hash_length = len(value)
            if hash_length == 32:
                return f"[file:hashes.MD5 = '{value}']"
            elif hash_length == 40:
                return f"[file:hashes.SHA1 = '{value}']"
            elif hash_length == 64:
                return f"[file:hashes.SHA256 = '{value}']"
            else:
                return f"[file:hashes.UNKNOWN = '{value}']"
        
        return None