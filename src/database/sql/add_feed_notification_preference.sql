-- Add notification preference to user_entity_feeds table
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'cluster_user' 
        AND table_name = 'user_entity_feeds' 
        AND column_name = 'user_entity_feeds_notifications_enabled'
    ) THEN
        ALTER TABLE cluster_user.user_entity_feeds 
        ADD COLUMN user_entity_feeds_notifications_enabled BOOLEAN DEFAULT TRUE;
        
        -- Create index for notification preference
        CREATE INDEX idx_user_entity_feeds_notifications 
        ON cluster_user.user_entity_feeds(user_entity_feeds_notifications_enabled)
        WHERE user_entity_feeds_notifications_enabled = TRUE;
        
        COMMENT ON COLUMN cluster_user.user_entity_feeds.user_entity_feeds_notifications_enabled 
        IS 'Whether to send email notifications for new articles in this feed';
    END IF;
END$$;

-- Update the view to include notification preference
DROP VIEW IF EXISTS cluster_user.user_entity_feeds_with_counts;

CREATE VIEW cluster_user.user_entity_feeds_with_counts AS
SELECT 
    f.*,
    COUNT(DISTINCT fe.user_followed_entities_id) as entity_count
FROM cluster_user.user_entity_feeds f
LEFT JOIN cluster_user.user_followed_entities fe 
    ON f.user_entity_feeds_id = fe.user_followed_entities_feed_id
WHERE f.user_entity_feeds_is_active = TRUE
GROUP BY f.user_entity_feeds_id;