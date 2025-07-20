-- Add tracking for cluster modifications to trigger AI summary regeneration

-- Add last_modified timestamp to track when cluster content changes
ALTER TABLE cluster_data.clusters 
ADD COLUMN IF NOT EXISTS clusters_last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add article count to track when new articles are added
ALTER TABLE cluster_data.clusters 
ADD COLUMN IF NOT EXISTS clusters_article_count INTEGER DEFAULT 0;

-- Add last AI summary generation timestamp
ALTER TABLE cluster_data.clusters 
ADD COLUMN IF NOT EXISTS ai_summary_generated_at TIMESTAMP;

-- Create an index for efficient querying of clusters needing regeneration
CREATE INDEX IF NOT EXISTS idx_clusters_ai_regeneration 
ON cluster_data.clusters(has_ai_summary, clusters_last_modified, ai_summary_generated_at)
WHERE clusters_is_active = TRUE;

-- Update existing clusters with current article counts
UPDATE cluster_data.clusters c
SET clusters_article_count = (
    SELECT COUNT(*) 
    FROM cluster_data.cluster_articles ca
    WHERE ca.cluster_articles_cluster_id = c.clusters_id
)
WHERE clusters_is_active = TRUE;

-- Create a function to automatically update last_modified when articles are added/removed
CREATE OR REPLACE FUNCTION cluster_data.update_cluster_modified()
RETURNS TRIGGER AS $$
BEGIN
    -- Update the cluster's last_modified timestamp and article count
    UPDATE cluster_data.clusters
    SET clusters_last_modified = CURRENT_TIMESTAMP,
        clusters_article_count = (
            SELECT COUNT(*) 
            FROM cluster_data.cluster_articles 
            WHERE cluster_articles_cluster_id = COALESCE(NEW.cluster_articles_cluster_id, OLD.cluster_articles_cluster_id)
        )
    WHERE clusters_id = COALESCE(NEW.cluster_articles_cluster_id, OLD.cluster_articles_cluster_id);
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create trigger for cluster_articles changes
DROP TRIGGER IF EXISTS trigger_update_cluster_modified ON cluster_data.cluster_articles;
CREATE TRIGGER trigger_update_cluster_modified
AFTER INSERT OR DELETE OR UPDATE ON cluster_data.cluster_articles
FOR EACH ROW
EXECUTE FUNCTION cluster_data.update_cluster_modified();

-- View to identify clusters needing AI summary regeneration
CREATE OR REPLACE VIEW cluster_data.clusters_needing_ai_regeneration AS
SELECT 
    c.clusters_id,
    c.clusters_name,
    c.clusters_last_modified,
    c.ai_summary_generated_at,
    c.clusters_article_count,
    ca_count.current_article_count,
    CASE 
        WHEN c.has_ai_summary = FALSE THEN 'never_generated'
        WHEN c.ai_summary_generated_at IS NULL THEN 'generated_but_no_timestamp'
        WHEN c.clusters_last_modified > c.ai_summary_generated_at THEN 'content_updated'
        WHEN ca_count.current_article_count != c.clusters_article_count THEN 'article_count_mismatch'
        ELSE 'up_to_date'
    END as regeneration_reason
FROM cluster_data.clusters c
LEFT JOIN (
    SELECT cluster_articles_cluster_id, COUNT(*) as current_article_count
    FROM cluster_data.cluster_articles
    GROUP BY cluster_articles_cluster_id
) ca_count ON c.clusters_id = ca_count.cluster_articles_cluster_id
WHERE c.clusters_is_active = TRUE
  AND (
    c.has_ai_summary = FALSE  -- Never generated
    OR c.ai_summary_generated_at IS NULL  -- Old summaries without timestamp
    OR c.clusters_last_modified > c.ai_summary_generated_at  -- Content changed
    OR ca_count.current_article_count != c.clusters_article_count  -- Article count mismatch
  );