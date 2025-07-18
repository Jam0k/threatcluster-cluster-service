"""
Entity Notification Service for ThreatCluster
Sends notifications when new articles mention entities in custom feeds
"""

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set
import json
from jinja2 import Template

from src.config.settings import settings
from src.email_service.email_service import EmailService, clean_dict
import asyncpg
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class EntityNotificationService(EmailService):
    """Service for sending entity-based notifications for custom feeds"""
    
    def __init__(self):
        super().__init__()
        # Initialize AWS SES client (required for entity notifications)
        if not hasattr(settings, 'aws_ses_region'):
            raise ValueError("AWS SES configuration required for entity notifications. Please set AWS_SES_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY in your .env file.")
        
        self.ses_client = boto3.client(
            'ses',
            region_name=settings.aws_ses_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key
        )
        
    async def get_users_with_entity_notifications(self, user_conn: asyncpg.Connection) -> List[Dict[str, Any]]:
        """Get all users who have entities with notifications enabled"""
        query = """
            SELECT DISTINCT 
                u.users_id,
                u.users_email,
                u.users_name,
                u.users_metadata
            FROM cluster_user.user_followed_entities ufe
            JOIN cluster_user.users u ON ufe.user_followed_entities_user_id = u.users_id
            WHERE ufe.user_followed_entities_notification_enabled = true
                AND u.users_is_active = true
                AND u.users_email IS NOT NULL
        """
        
        rows = await user_conn.fetch(query)
        return [dict(row) for row in rows]
    
    async def get_user_followed_entities_by_feed(
        self, 
        user_conn: asyncpg.Connection, 
        user_id: str
    ) -> Dict[int, Dict[str, Any]]:
        """Get all entities a user follows grouped by feed"""
        query = """
            SELECT 
                ufe.user_followed_entities_entity_id as entity_id,
                ufe.user_followed_entities_feed_id as feed_id,
                ufe.user_followed_entities_last_activity as last_activity,
                uef.user_entity_feeds_name as feed_name,
                uef.user_entity_feeds_is_default as is_default_feed,
                COALESCE(uef.user_entity_feeds_notifications_enabled, true) as feed_notifications_enabled
            FROM cluster_user.user_followed_entities ufe
            JOIN cluster_user.user_entity_feeds uef ON ufe.user_followed_entities_feed_id = uef.user_entity_feeds_id
            WHERE ufe.user_followed_entities_user_id = $1
                AND ufe.user_followed_entities_notification_enabled = true
                AND uef.user_entity_feeds_is_active = true
                AND COALESCE(uef.user_entity_feeds_notifications_enabled, true) = true
        """
        
        rows = await user_conn.fetch(query, user_id)
        
        # Group by feed
        feeds = {}
        for row in rows:
            feed_id = row['feed_id']
            if feed_id not in feeds:
                feeds[feed_id] = {
                    'feed_id': feed_id,
                    'feed_name': row['feed_name'],
                    'is_default': row['is_default_feed'],
                    'entities': []
                }
            feeds[feed_id]['entities'].append({
                'entity_id': row['entity_id'],
                'last_activity': row['last_activity']
            })
            
        return feeds
    
    async def get_entity_details(
        self,
        cluster_conn: asyncpg.Connection,
        entity_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """Get entity details for a list of entity IDs"""
        if not entity_ids:
            return {}
            
        query = """
            SELECT 
                entities_id,
                entities_name,
                entities_category,
                entities_importance_weight
            FROM cluster_data.entities
            WHERE entities_id = ANY($1::int[])
        """
        
        rows = await cluster_conn.fetch(query, entity_ids)
        return {row['entities_id']: dict(row) for row in rows}
    
    async def get_new_articles_with_entities(
        self,
        cluster_conn: asyncpg.Connection,
        entity_ids: List[int],
        since: datetime,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get new articles that mention any of the specified entities"""
        if not entity_ids:
            return []
            
        query = """
            WITH entity_articles AS (
                SELECT DISTINCT
                    rfc.rss_feeds_clean_id,
                    rfr.rss_feeds_raw_id,
                    rfr.rss_feeds_raw_xml->>'title' as title,
                    rfr.rss_feeds_raw_xml->>'link' as link,
                    rfr.rss_feeds_raw_published_date as published_date,
                    rfc.rss_feeds_clean_content,
                    rf.rss_feeds_name as source_name,
                    -- Extract all entity IDs from this article
                    ARRAY_AGG(DISTINCT (ent->>'entities_id')::int) FILTER (
                        WHERE (ent->>'entities_id')::int = ANY($1::int[])
                    ) as matching_entity_ids
                FROM cluster_data.rss_feeds_clean rfc
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                CROSS JOIN LATERAL jsonb_array_elements(
                    rfc.rss_feeds_clean_extracted_entities->'entities'
                ) AS ent
                WHERE rfr.rss_feeds_raw_published_date > $2
                    AND (ent->>'entities_id')::int = ANY($1::int[])
                GROUP BY 
                    rfc.rss_feeds_clean_id,
                    rfr.rss_feeds_raw_id,
                    rfr.rss_feeds_raw_xml,
                    rfr.rss_feeds_raw_published_date,
                    rfc.rss_feeds_clean_content,
                    rf.rss_feeds_name
            )
            SELECT *
            FROM entity_articles
            ORDER BY published_date DESC
            LIMIT $3
        """
        
        rows = await cluster_conn.fetch(query, entity_ids, since, limit)
        return [dict(row) for row in rows]
    
    async def update_entity_last_activity(
        self,
        user_conn: asyncpg.Connection,
        user_id: str,
        entity_ids: List[int]
    ):
        """Update last activity timestamp for multiple entities"""
        if not entity_ids:
            return
            
        query = """
            UPDATE cluster_user.user_followed_entities
            SET user_followed_entities_last_activity = NOW()
            WHERE user_followed_entities_user_id = $1
                AND user_followed_entities_entity_id = ANY($2::int[])
        """
        await user_conn.execute(query, user_id, entity_ids)
    
    async def record_notification_history(
        self,
        user_conn: asyncpg.Connection,
        user_id: str,
        feed_id: int,
        article_count: int,
        entity_ids: List[int],
        status: str,
        error_message: Optional[str] = None
    ):
        """Record entity notification history"""
        query = """
            INSERT INTO cluster_user.entity_notification_history 
            (user_id, feed_id, entity_ids, article_count, email_status, error_message)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        await user_conn.execute(
            query, 
            user_id, 
            feed_id, 
            entity_ids,
            article_count, 
            status, 
            error_message
        )
    
    def render_entity_notification_email(
        self,
        user: Dict[str, Any],
        feed_name: str,
        articles: List[Dict[str, Any]],
        entity_details: Dict[int, Dict[str, Any]]
    ) -> tuple[str, str]:
        """Render email content for entity notifications"""
        
        # Group articles by entity for better organization
        articles_by_entity = {}
        for article in articles:
            for entity_id in article['matching_entity_ids']:
                if entity_id not in articles_by_entity:
                    articles_by_entity[entity_id] = []
                articles_by_entity[entity_id].append(article)
        
        # HTML template with ThreatCluster branding
        html_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
                .container { max-width: 600px; margin: 0 auto; padding: 0; }
                .header { background: #dc2626; color: white; padding: 30px 20px; text-align: center; }
                .header h1 { margin: 0; font-size: 24px; font-weight: 600; }
                .content { padding: 30px 20px; background: #ffffff; }
                .content p { color: #374151; margin: 16px 0; }
                .entity-section { margin: 30px 0; }
                .entity-header { 
                    background: #fef2f2; 
                    padding: 15px 20px; 
                    border-left: 4px solid #dc2626; 
                    margin-bottom: 20px;
                }
                .entity-header h2 { 
                    margin: 0; 
                    color: #dc2626; 
                    font-size: 20px; 
                    font-weight: 600; 
                }
                .entity-category { 
                    color: #6b7280; 
                    font-size: 14px; 
                    margin-top: 5px; 
                }
                .article { 
                    background: #f9fafb; 
                    padding: 20px; 
                    margin: 15px 0; 
                    border-radius: 8px; 
                    border: 1px solid #e5e7eb;
                }
                .article h3 { 
                    margin-top: 0; 
                    margin-bottom: 12px; 
                    color: #111827; 
                    font-size: 18px; 
                    font-weight: 600; 
                    line-height: 1.3; 
                }
                .meta { color: #6b7280; font-size: 14px; margin: 12px 0; }
                .meta strong { color: #374151; }
                .article-content { color: #4b5563; margin: 12px 0; line-height: 1.5; }
                .button { 
                    display: inline-block; 
                    padding: 12px 30px; 
                    background: #dc2626; 
                    color: white; 
                    text-decoration: none; 
                    border-radius: 6px; 
                    margin: 20px 0; 
                    font-weight: 500; 
                }
                .button:hover { background: #b91c1c; }
                .footer { text-align: center; padding: 20px; color: #6b7280; font-size: 14px; background: #f9fafb; }
                .footer a { color: #dc2626; text-decoration: none; }
                .footer a:hover { text-decoration: underline; }
                a { color: #dc2626; text-decoration: none; }
                a:hover { text-decoration: underline; }
                .summary-box {
                    background: #f3f4f6;
                    border-radius: 8px;
                    padding: 15px;
                    margin: 20px 0;
                    font-size: 14px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔔 New Entity Alerts: {{ feed_name }}</h1>
                </div>
                
                <div class="content">
                    <p>Hi {{ user.users_name or 'there' }},</p>
                    
                    <div class="summary-box">
                        <strong>{{ articles|length }} new article{% if articles|length != 1 %}s{% endif %}</strong> 
                        mentioning <strong>{{ articles_by_entity|length }} entit{% if articles_by_entity|length != 1 %}ies{% else %}y{% endif %}</strong> 
                        from your "{{ feed_name }}" feed.
                    </div>
                    
                    {% for entity_id, entity_articles in articles_by_entity.items() %}
                    {% set entity = entity_details[entity_id] %}
                    <div class="entity-section">
                        <div class="entity-header">
                            <h2>{{ entity.entities_name }}</h2>
                            <div class="entity-category">{{ entity.entities_category|replace('_', ' ')|title }}</div>
                        </div>
                        
                        {% for article in entity_articles[:3] %}
                        <div class="article">
                            <h3>{{ article.title }}</h3>
                            <div class="meta">
                                <strong>Source:</strong> {{ article.source_name }}<br>
                                <strong>Published:</strong> {{ article.published_date.strftime('%B %d, %Y at %I:%M %p UTC') }}
                            </div>
                            <div class="article-content">
                                {% set clean_content = "" %}
                                {% if article.rss_feeds_clean_content %}
                                    {% if article.rss_feeds_clean_content.content %}
                                        {% set clean_content = article.rss_feeds_clean_content.content %}
                                    {% endif %}
                                {% endif %}
                                {{ clean_content[:300] }}{% if clean_content|length > 300 %}...{% endif %}
                            </div>
                            <a href="{{ article.link }}" target="_blank">Read full article →</a>
                        </div>
                        {% endfor %}
                        
                        {% if entity_articles|length > 3 %}
                        <p style="text-align: center; color: #6b7280; font-size: 14px;">
                            ... and {{ entity_articles|length - 3 }} more article{% if entity_articles|length - 3 != 1 %}s{% endif %}
                        </p>
                        {% endif %}
                    </div>
                    {% endfor %}
                    
                    <div style="text-align: center;">
                        <a href="https://threatcluster.io/feeds" class="button">
                            View Your Custom Feeds
                        </a>
                    </div>
                </div>
                
                <div class="footer">
                    <p>
                        You're receiving this because you have notifications enabled for entities in your "{{ feed_name }}" feed.<br>
                        <a href="https://threatcluster.io/settings/notifications">Manage your notifications</a> | 
                        <a href="https://threatcluster.io/feeds">Manage your feeds</a>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """)
        
        # Plain text template
        text_template = Template("""
New Entity Alerts: {{ feed_name }}

Hi {{ user.users_name or 'there' }},

{{ articles|length }} new article{% if articles|length != 1 %}s{% endif %} mentioning {{ articles_by_entity|length }} entit{% if articles_by_entity|length != 1 %}ies{% else %}y{% endif %} from your "{{ feed_name }}" feed:

{% for entity_id, entity_articles in articles_by_entity.items() %}
{% set entity = entity_details[entity_id] %}
=====================================
{{ entity.entities_name }} ({{ entity.entities_category|replace('_', ' ')|title }})
=====================================

{% for article in entity_articles[:3] %}
---
{{ article.title }}
Source: {{ article.source_name }}
Published: {{ article.published_date.strftime('%B %d, %Y at %I:%M %p UTC') }}

{% set clean_content = "" %}
{% if article.rss_feeds_clean_content %}
    {% if article.rss_feeds_clean_content.content %}
        {% set clean_content = article.rss_feeds_clean_content.content %}
    {% endif %}
{% endif %}
{{ clean_content[:200] }}{% if clean_content|length > 200 %}...{% endif %}

Read more: {{ article.link }}

{% endfor %}
{% if entity_articles|length > 3 %}
... and {{ entity_articles|length - 3 }} more article{% if entity_articles|length - 3 != 1 %}s{% endif %}
{% endif %}

{% endfor %}

View your custom feeds: https://threatcluster.io/feeds

---
You're receiving this because you have notifications enabled for entities in your "{{ feed_name }}" feed.
Manage notifications: https://threatcluster.io/settings/notifications
Manage your feeds: https://threatcluster.io/feeds
        """)
        
        html_content = html_template.render(
            user=user,
            feed_name=feed_name,
            articles=articles,
            entity_details=entity_details,
            articles_by_entity=articles_by_entity
        )
        
        text_content = text_template.render(
            user=user,
            feed_name=feed_name,
            articles=articles,
            entity_details=entity_details,
            articles_by_entity=articles_by_entity
        )
        
        return html_content, text_content
    
    async def send_notification_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
        text_content: str
    ) -> bool:
        """Send email using AWS SES"""
        
        try:
            # Use entity-specific from email if configured, otherwise fall back to default
            from_email = getattr(settings, 'entity_notification_from_email', self.from_email)
            
            response = self.ses_client.send_email(
                Source=from_email,
                Destination={'ToAddresses': [to_email]},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {
                        'Text': {'Data': text_content},
                        'Html': {'Data': html_content}
                    }
                }
            )
            logger.info(f"Sent entity notification via AWS SES to {to_email}, MessageId: {response['MessageId']}")
            return True
        except ClientError as e:
            logger.error(f"AWS SES error sending to {to_email}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email to {to_email}: {e}")
            return False
    
    async def check_and_notify_user_entities(
        self,
        cluster_conn: asyncpg.Connection,
        user_conn: asyncpg.Connection,
        user: Dict[str, Any]
    ):
        """Check for new articles mentioning user's followed entities and send notifications"""
        
        user_id = user['users_id']
        user_email = user['users_email']
        
        # Get user's followed entities grouped by feed
        feeds = await self.get_user_followed_entities_by_feed(user_conn, user_id)
        if not feeds:
            logger.debug(f"No followed entities with notifications for user {user_id}")
            return
        
        # Process each feed
        notifications_sent = 0
        for feed_id, feed_data in feeds.items():
            try:
                # Get all entity IDs in this feed
                entity_ids = [e['entity_id'] for e in feed_data['entities']]
                
                # Determine the lookback period (last 24 hours by default)
                # Use the most recent last_activity from any entity in the feed
                last_activities = [e['last_activity'] for e in feed_data['entities'] if e['last_activity']]
                if last_activities:
                    since = max(last_activities)
                else:
                    since = datetime.now() - timedelta(hours=24)
                
                # Ensure timezone-naive for DB compatibility
                if hasattr(since, 'tzinfo') and since.tzinfo is not None:
                    since = since.replace(tzinfo=None)
                
                # Get new articles mentioning these entities
                articles = await self.get_new_articles_with_entities(
                    cluster_conn,
                    entity_ids,
                    since
                )
                
                if not articles:
                    continue
                
                # Get entity details for rendering
                all_mentioned_entity_ids = set()
                for article in articles:
                    all_mentioned_entity_ids.update(article['matching_entity_ids'])
                
                entity_details = await self.get_entity_details(
                    cluster_conn,
                    list(all_mentioned_entity_ids)
                )
                
                # Render email
                html_content, text_content = self.render_entity_notification_email(
                    user,
                    feed_data['feed_name'],
                    articles,
                    entity_details
                )
                
                # Send email
                subject = f"New alerts for {feed_data['feed_name']}"
                success = await self.send_notification_email(
                    user_email,
                    subject,
                    html_content,
                    text_content
                )
                
                if success:
                    # Update last activity for all entities that had matches
                    mentioned_entity_ids = list(all_mentioned_entity_ids)
                    await self.update_entity_last_activity(
                        user_conn,
                        user_id,
                        mentioned_entity_ids
                    )
                    
                    # Record notification history
                    await self.record_notification_history(
                        user_conn,
                        user_id,
                        feed_id,
                        len(articles),
                        list(all_mentioned_entity_ids),
                        'sent'
                    )
                    
                    notifications_sent += 1
                    logger.info(f"Sent entity notification to {user_email} for feed '{feed_data['feed_name']}' with {len(articles)} articles")
                else:
                    # Record failure
                    await self.record_notification_history(
                        user_conn,
                        user_id,
                        feed_id,
                        len(articles),
                        list(all_mentioned_entity_ids),
                        'failed',
                        'Email send failed'
                    )
                
                # asyncpg doesn't need explicit commit for connections
                
            except Exception as e:
                logger.error(f"Error processing feed {feed_id} for user {user_id}: {e}")
                # asyncpg connections don't have rollback method
        
        if notifications_sent > 0:
            logger.info(f"Sent {notifications_sent} feed notifications to user {user_id}")
    
    async def check_all_users_for_entity_updates(self):
        """Check all users with entity notifications enabled"""
        
        # Connect to databases
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        
        try:
            # Get users with entity notifications enabled
            users = await self.get_users_with_entity_notifications(user_conn)
            
            logger.info(f"Checking entity notifications for {len(users)} users")
            
            for user in users:
                await self.check_and_notify_user_entities(
                    cluster_conn,
                    user_conn,
                    user
                )
                
        finally:
            await cluster_conn.close()
            await user_conn.close()


async def main():
    """Test the entity notification service"""
    service = EntityNotificationService()
    await service.check_all_users_for_entity_updates()


if __name__ == "__main__":
    asyncio.run(main())