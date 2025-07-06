-- Create an optimized entity synonym search solution
-- This uses a materialized view for fast synonym lookups

SET search_path TO cluster_data;

-- Create a normalized synonym lookup table
CREATE TABLE IF NOT EXISTS entity_synonyms (
    synonym_id SERIAL PRIMARY KEY,
    entities_id INTEGER NOT NULL REFERENCES entities(entities_id) ON DELETE CASCADE,
    synonym_text VARCHAR(500) NOT NULL,
    synonym_lower VARCHAR(500) GENERATED ALWAYS AS (lower(synonym_text)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_entity_synonyms_entity_id ON entity_synonyms(entities_id);
CREATE INDEX IF NOT EXISTS idx_entity_synonyms_lower ON entity_synonyms(synonym_lower);
CREATE INDEX IF NOT EXISTS idx_entity_synonyms_text_trgm ON entity_synonyms USING gin(synonym_lower gin_trgm_ops);

-- Populate the synonym table from existing MISP data
INSERT INTO entity_synonyms (entities_id, synonym_text)
SELECT 
    e.entities_id,
    jsonb_array_elements_text(e.entities_json->'meta'->'synonyms') as synonym_text
FROM entities e
WHERE e.entities_source = 'misp'
    AND e.entities_json->'meta'->'synonyms' IS NOT NULL
    AND jsonb_array_length(e.entities_json->'meta'->'synonyms') > 0
ON CONFLICT DO NOTHING;

-- Also add the primary entity names as "synonyms" for unified search
INSERT INTO entity_synonyms (entities_id, synonym_text)
SELECT 
    e.entities_id,
    e.entities_name
FROM entities e
WHERE e.entities_source = 'misp'
ON CONFLICT DO NOTHING;

-- Create a function to refresh synonyms (call this after MISP updates)
CREATE OR REPLACE FUNCTION refresh_entity_synonyms() RETURNS void AS $$
BEGIN
    -- Clear existing synonyms
    TRUNCATE entity_synonyms;
    
    -- Re-populate from MISP data
    INSERT INTO entity_synonyms (entities_id, synonym_text)
    SELECT 
        e.entities_id,
        jsonb_array_elements_text(e.entities_json->'meta'->'synonyms') as synonym_text
    FROM entities e
    WHERE e.entities_source = 'misp'
        AND e.entities_json->'meta'->'synonyms' IS NOT NULL
        AND jsonb_array_length(e.entities_json->'meta'->'synonyms') > 0;
    
    -- Add primary names
    INSERT INTO entity_synonyms (entities_id, synonym_text)
    SELECT 
        e.entities_id,
        e.entities_name
    FROM entities e
    WHERE e.entities_source = 'misp';
END;
$$ LANGUAGE plpgsql;

-- Analyze tables for query optimization
ANALYZE entities;
ANALYZE entity_synonyms;