# Entity and Synonym Handling Strategy

## Overview

ThreatCluster uses a sophisticated entity extraction system that supports synonyms for threat actors, malware families, and ransomware groups. This document describes how the system handles entities with multiple names/aliases.

## Current Implementation

### 1. Entity Storage

Entities are stored in the `cluster_data.entities` table with the following key fields:
- `entities_id`: Primary key used for all references
- `entities_name`: Primary/canonical name of the entity
- `entities_json`: JSONB field containing full MISP data including synonyms
- `entities_source`: 'misp' for MISP-imported entities

### 2. MISP Import Process

The MISP import service (`src/misp/`) fetches data from GitHub-hosted MISP galaxy JSON files:
- APT Groups: `threat-actor.json` → mapped to `apt_group` category
- Malware Families: `malpedia.json` → mapped to `malware_family` category  
- Ransomware Groups: `ransomware.json` → mapped to `ransomware_group` category

Each MISP entity contains:
```json
{
  "value": "APT1",  // Primary name stored in entities_name
  "meta": {
    "synonyms": ["COMMENT PANDA", "PLA Unit 61398", "Comment Crew"],
    "refs": ["https://..."],
    // other metadata
  }
}
```

### 3. Entity Extraction Process

The entity extractor (`src/entity_extraction/entity_extractor.py`) handles synonyms as follows:

1. **Loading Phase** (`_load_predefined_entities`):
   - Loads all entities from database where `entities_source IN ('manual', 'misp')`
   - For each entity, creates regex patterns for:
     - Primary name (entities_name)
     - All synonyms from entities_json.meta.synonyms
   - Each pattern tracks what text it matches

2. **Extraction Phase** (`extract_predefined_entities`):
   - Searches text for all patterns (primary + synonyms)
   - When any pattern matches:
     - Returns the primary entity name (not the matched synonym)
     - Includes the entity ID for database relationships
     - Tracks the actual matched text for debugging
   - Prevents duplicates when multiple synonyms match

### 4. Article Storage

Extracted entities are stored in `rss_feeds_clean_extracted_entities` as:
```json
{
  "entities": [
    {
      "entity_name": "APT1",         // Always the primary name
      "entity_category": "apt_group",
      "entities_id": 1,              // Database ID for joins
      "confidence": 0.95,
      "extraction_method": "predefined",
      "matched_text": "COMMENT PANDA" // What was actually found
    }
  ]
}
```

### 5. API and Frontend

- API queries use `entities_id` to join articles with entities
- Frontend displays the primary entity name
- All synonyms link to the same entity page

## Benefits

1. **Comprehensive Coverage**: Articles mentioning any known alias are properly attributed
2. **Consistency**: All references resolve to a single canonical entity
3. **No Duplication**: One entity record serves all aliases
4. **Maintainability**: Adding new synonyms only requires updating entities_json

## Statistics (as of last test)

- Total entities with synonyms: 1,326
- Total synonyms loaded: 2,737
- Categories with synonyms:
  - APT Groups: 855 entities
  - Malware Families: 3,260 entities  
  - Ransomware Groups: 2,003 entities

## Example

An article containing "COMMENT PANDA" will be:
1. Matched by the synonym pattern for APT1
2. Stored with entity_name="APT1" and entities_id=1
3. Displayed as related to "APT1" in the UI
4. Counted in APT1's article count

This ensures complete coverage while maintaining data consistency.