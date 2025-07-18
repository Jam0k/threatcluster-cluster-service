"""
Check entity notification history
"""
import asyncio
import asyncpg
from src.config.settings import settings

async def check_history():
    """Check the entity notification history"""
    conn = await asyncpg.connect(settings.user_database_url)
    
    try:
        # Check notification history
        query = """
        SELECT 
            h.*,
            u.users_email,
            f.user_entity_feeds_name
        FROM cluster_user.entity_notification_history h
        JOIN cluster_user.users u ON h.user_id = u.users_id
        JOIN cluster_user.user_entity_feeds f ON h.feed_id = f.user_entity_feeds_id
        ORDER BY h.created_at DESC
        LIMIT 10;
        """
        
        rows = await conn.fetch(query)
        print(f"Found {len(rows)} notification history records:\n")
        
        for row in rows:
            print(f"User: {row['users_email']}")
            print(f"Feed: {row['user_entity_feeds_name']}")
            print(f"Status: {row['email_status']}")
            print(f"Articles: {row['article_count']}")
            print(f"Entities: {row['entity_ids']}")
            print(f"Error: {row['error_message'] or 'None'}")
            print(f"Created: {row['created_at']}")
            print("-" * 50)
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_history())