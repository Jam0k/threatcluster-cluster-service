-- Add AI summary columns to clusters table
-- This migration adds support for OpenAI-generated cluster summaries

-- Add ai_summary column to store the 3 briefs as JSONB
ALTER TABLE cluster_data.clusters 
ADD COLUMN IF NOT EXISTS ai_summary JSONB;

-- Add has_ai_summary flag to track which clusters have been processed
ALTER TABLE cluster_data.clusters 
ADD COLUMN IF NOT EXISTS has_ai_summary BOOLEAN DEFAULT FALSE;

-- Create index on has_ai_summary for efficient querying of unprocessed clusters
CREATE INDEX IF NOT EXISTS idx_clusters_has_ai_summary 
ON cluster_data.clusters(has_ai_summary) 
WHERE has_ai_summary = FALSE;

-- Add comment explaining the structure
COMMENT ON COLUMN cluster_data.clusters.ai_summary IS 'JSON object containing three AI-generated briefs: executive_brief, technical_brief, and remediation_brief';
COMMENT ON COLUMN cluster_data.clusters.has_ai_summary IS 'Flag indicating whether AI summary has been generated for this cluster';