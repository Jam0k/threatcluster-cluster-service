"""
Test script for entity notification service
"""

import asyncio
import logging
from datetime import datetime, timedelta

from src.email_service.entity_notification_service import EntityNotificationService
from src.config.settings import settings
import asyncpg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_entity_notifications():
    """Test the entity notification service"""
    logger.info("Testing entity notification service...")
    
    # Initialize service
    service = EntityNotificationService()
    
    # Connect to databases
    cluster_conn = await asyncpg.connect(settings.database_url)
    user_conn = await asyncpg.connect(settings.user_database_url)
    
    try:
        # Get users with entity notifications
        users = await service.get_users_with_entity_notifications(user_conn)
        logger.info(f"Found {len(users)} users with entity notifications enabled")
        
        if not users:
            logger.info("No users have entity notifications enabled")
            return
        
        # Test with first user
        user = users[0]
        logger.info(f"Testing with user: {user['users_email']}")
        
        # Get user's followed entities by feed
        feeds = await service.get_user_followed_entities_by_feed(user_conn, user['users_id'])
        logger.info(f"User has {len(feeds)} feeds with followed entities")
        
        for feed_id, feed_data in feeds.items():
            logger.info(f"Feed '{feed_data['feed_name']}' has {len(feed_data['entities'])} entities")
            
            # Get entity details
            entity_ids = [e['entity_id'] for e in feed_data['entities']]
            entity_details = await service.get_entity_details(cluster_conn, entity_ids)
            
            for entity_id, details in entity_details.items():
                logger.info(f"  - {details['entities_name']} ({details['entities_category']})")
            
            # Check for new articles (last 7 days for testing)
            since = datetime.now() - timedelta(days=7)
            articles = await service.get_new_articles_with_entities(
                cluster_conn,
                entity_ids,
                since,
                limit=5
            )
            
            logger.info(f"Found {len(articles)} articles mentioning these entities")
            for article in articles[:3]:
                logger.info(f"  - {article['title'][:80]}...")
                logger.info(f"    Published: {article['published_date']}")
                logger.info(f"    Entities: {article['matching_entity_ids']}")
        
        # Test notification for one user
        logger.info("\nTesting notification send...")
        await service.check_and_notify_user_entities(
            cluster_conn,
            user_conn,
            user
        )
        
    finally:
        await cluster_conn.close()
        await user_conn.close()


async def main():
    """Main test function"""
    try:
        await test_entity_notifications()
        logger.info("Entity notification test completed successfully")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())