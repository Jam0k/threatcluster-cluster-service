#!/usr/bin/env python3
"""
Test script to verify MITRE MISP + STIX data merging.

Usage:
    cd /home/james/Desktop/Threatcluster-2/cluster-service
    python -m tests.test_mitre_stix_merge
"""
import json
from src.misp.misp_fetcher import MISPFetcher
from src.config.settings import settings
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(colors=True)
    ],
)

logger = structlog.get_logger()


def test_mitre_merge():
    """Test fetching and merging MITRE data from MISP and STIX sources."""
    
    print("\n=== Testing MITRE MISP + STIX Data Merge ===\n")
    
    # Initialize fetcher
    fetcher = MISPFetcher()
    
    # Override config to only process MITRE feed
    fetcher.config['feeds'] = [
        {
            'name': 'MISP MITRE Techniques',
            'url': 'https://raw.githubusercontent.com/MISP/misp-galaxy/refs/heads/main/clusters/tidal-technique.json',
            'type': 'mitre',
            'active': True
        }
    ]
    
    # Fetch all feeds (will fetch STIX data and process MITRE feed)
    print("Fetching MITRE data from both sources...")
    stats = fetcher.fetch_all_feeds()
    
    print(f"\nFetch Statistics:")
    print(f"- Feeds processed: {stats['feeds_processed']}")
    print(f"- Entities fetched: {stats['entities_fetched']}")
    print(f"- Entities inserted: {stats['entities_inserted']}")
    print(f"- Entities updated: {stats['entities_updated']}")
    print(f"- Duration: {stats.get('duration_seconds', 0):.2f} seconds")
    
    # Query a few sample techniques to show the merged data
    import psycopg2
    conn = psycopg2.connect(settings.database_url)
    cursor = conn.cursor()
    
    print("\n=== Sample Merged MITRE Techniques ===\n")
    
    # Get a few MITRE techniques
    cursor.execute("""
        SELECT entities_name, entities_json 
        FROM cluster_data.entities
        WHERE entities_category = 'mitre' 
        AND entities_source = 'misp'
        AND entities_json IS NOT NULL
        ORDER BY entities_name
        LIMIT 3
    """)
    
    for row in cursor.fetchall():
        technique_id = row[0]
        json_data = row[1]
        
        print(f"\nTechnique: {technique_id}")
        print("-" * 50)
        
        # Check if it has merged data
        if isinstance(json_data, dict) and 'sources' in json_data:
            print("✓ Has merged MISP + STIX data")
            
            # Show some key fields
            if 'stix_data' in json_data and json_data['stix_data']:
                stix_info = json_data['stix_data']
                print(f"  Name: {stix_info.get('name', 'N/A')}")
                print(f"  Platforms: {', '.join(stix_info.get('platforms', []))}")
                print(f"  Kill Chain Phases: {', '.join(stix_info.get('kill_chain_phases', []))}")
                print(f"  Is Subtechnique: {stix_info.get('is_subtechnique', False)}")
                print(f"  Data Sources: {len(stix_info.get('data_sources', []))} sources")
                print(f"  References: {len(stix_info.get('references', []))} references")
                
                # Show detection if available
                detection = stix_info.get('detection', '')
                if detection:
                    print(f"  Detection preview: {detection[:150]}...")
        else:
            print("✗ Only has MISP data (no STIX match)")
            
        # Show JSON structure
        print("\nJSON Structure:")
        if isinstance(json_data, dict):
            def show_structure(data, indent=0):
                for key in sorted(data.keys()):
                    value = data[key]
                    if isinstance(value, dict):
                        print("  " * indent + f"- {key}: <dict with {len(value)} keys>")
                        if indent < 1:  # Only go one level deep
                            show_structure(value, indent + 1)
                    elif isinstance(value, list):
                        print("  " * indent + f"- {key}: <list with {len(value)} items>")
                    else:
                        print("  " * indent + f"- {key}: {type(value).__name__}")
            
            show_structure(json_data)
    
    cursor.close()
    conn.close()
    
    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    test_mitre_merge()