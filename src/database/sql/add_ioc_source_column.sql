-- Add IOC source column to entities table
-- This column tracks which threat intelligence feeds provided each IOC

-- Add the column if it doesn't exist
ALTER TABLE cluster_data.entities 
ADD COLUMN IF NOT EXISTS entities_ioc_source JSONB;

-- Add an index for better query performance
CREATE INDEX IF NOT EXISTS idx_entities_ioc_source 
ON cluster_data.entities USING GIN (entities_ioc_source);

-- Update the comment on the entities table
COMMENT ON TABLE cluster_data.entities IS 'Dictionary of cybersecurity entities with importance weights and IOC source tracking';
COMMENT ON COLUMN cluster_data.entities.entities_ioc_source IS 'JSON array of threat intelligence feed sources that provided this IOC';

-- Example of how the entities_ioc_source column will be used:
-- For an IP that appears in multiple feeds:
-- ["ThreatFox IPs", "SANS Intel IPs", "Cobalt Strike IPs"]
-- 
-- For a domain from a single feed:
-- ["AlienVault Banking Phishing"]