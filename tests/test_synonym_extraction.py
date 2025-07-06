#!/usr/bin/env python3
"""
Test Synonym Extraction

Tests that entity extraction properly handles synonyms from MISP data.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.entity_extraction.entity_extractor import EntityExtractor
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


def test_synonym_extraction():
    """Test that synonyms are properly extracted."""
    extractor = EntityExtractor()
    
    # Test texts with various names for APT1
    test_cases = [
        {
            'text': "APT1 was observed targeting infrastructure sectors.",
            'expected_entity': 'APT1',
            'description': 'Main entity name'
        },
        {
            'text': "COMMENT PANDA has been active in cyber espionage campaigns.",
            'expected_entity': 'APT1', 
            'description': 'Synonym (COMMENT PANDA)'
        },
        {
            'text': "PLA Unit 61398 is linked to Chinese military operations.",
            'expected_entity': 'APT1',
            'description': 'Synonym (PLA Unit 61398)'
        },
        {
            'text': "The Comment Crew group has stolen intellectual property.",
            'expected_entity': 'APT1',
            'description': 'Synonym (Comment Crew)'
        },
        {
            'text': "Byzantine Candor attacks have targeted US companies.",
            'expected_entity': 'APT1',
            'description': 'Synonym (Byzantine Candor)'
        }
    ]
    
    print("\n" + "="*60)
    print("Testing Entity Extraction with Synonyms")
    print("="*60)
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['description']}")
        print(f"Text: {test_case['text']}")
        
        # Extract entities
        predefined = extractor.extract_predefined_entities(test_case['text'])
        apt_entities = [e for e in predefined if e['entity_category'] == 'apt_group']
        
        if apt_entities:
            entity = apt_entities[0]
            print(f"✓ Found: {entity['entity_name']} (ID: {entity.get('entities_id', 'N/A')})")
            if 'matched_text' in entity:
                print(f"  Matched text: '{entity['matched_text']}'")
            
            if entity['entity_name'] == test_case['expected_entity']:
                print("  ✓ Correctly resolved to primary entity name")
            else:
                print(f"  ✗ Expected: {test_case['expected_entity']}, Got: {entity['entity_name']}")
        else:
            print("✗ No APT group entity found")
    
    # Test with multiple synonyms in same text
    multi_text = "APT1, also known as COMMENT PANDA and PLA Unit 61398, is a sophisticated threat actor."
    print(f"\n\nMulti-synonym test:")
    print(f"Text: {multi_text}")
    
    predefined = extractor.extract_predefined_entities(multi_text)
    apt_entities = [e for e in predefined if e['entity_category'] == 'apt_group']
    
    print(f"Found {len(apt_entities)} APT entity(ies)")
    for entity in apt_entities:
        print(f"  - {entity['entity_name']} (ID: {entity.get('entities_id', 'N/A')})")
    
    # Show statistics
    print("\n" + "="*60)
    print("Entity Extraction Statistics:")
    print(f"Total predefined entities loaded: {sum(len(entities) for entities in extractor.predefined_entities.values())}")
    
    # Count entities with synonyms
    total_with_synonyms = 0
    total_synonyms = 0
    
    for category, entities in extractor.predefined_entities.items():
        for entity in entities:
            if len(entity['patterns']) > 1:
                total_with_synonyms += 1
                total_synonyms += len(entity['patterns']) - 1
    
    print(f"Entities with synonyms: {total_with_synonyms}")
    print(f"Total synonyms loaded: {total_synonyms}")
    print("="*60 + "\n")


if __name__ == "__main__":
    test_synonym_extraction()