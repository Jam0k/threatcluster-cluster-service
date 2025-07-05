-- Simple query to view all MITRE techniques
SELECT 
    entities_id,
    entities_name,
    entities_source,
    entities_importance_weight,
    entities_added_on,
    entities_json
FROM cluster_data.entities
WHERE entities_category = 'mitre'
ORDER BY entities_importance_weight DESC, entities_name;