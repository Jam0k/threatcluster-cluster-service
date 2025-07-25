"""
Cluster Notification Service for ThreatCluster
Sends notifications when new articles are added to followed clusters
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


class ClusterNotificationService(EmailService):
    """Service for sending cluster update notifications"""
    
    def __init__(self):
        super().__init__()
        # Initialize AWS SES client (required for cluster notifications)
        if not hasattr(settings, 'aws_ses_region'):
            raise ValueError("AWS SES configuration required for cluster notifications. Please set AWS_SES_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY in your .env file.")
        
        self.ses_client = boto3.client(
            'ses',
            region_name=settings.aws_ses_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key
        )
            
    async def get_cluster_followers(self, conn: asyncpg.Connection, cluster_id: int) -> List[Dict[str, Any]]:
        """Get all users following a specific cluster with notifications enabled"""
        query = """
            SELECT 
                u.users_id,
                u.users_email,
                u.users_name,
                ucf.last_notified_at
            FROM cluster_user.user_cluster_follows ucf
            JOIN cluster_user.users u ON ucf.user_id = u.users_id
            WHERE ucf.cluster_id = $1
                AND ucf.notification_enabled = true
                AND u.users_is_active = true
                AND u.users_email IS NOT NULL
        """
        
        rows = await conn.fetch(query, cluster_id)
        return [dict(row) for row in rows]
    
    async def get_new_articles_for_cluster(
        self, 
        cluster_conn: asyncpg.Connection,
        user_conn: asyncpg.Connection, 
        cluster_id: int, 
        user_id: str,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get new articles added to a cluster since the user started following (or since last notification)"""
        if not since:
            since = datetime.now() - timedelta(hours=24)  # Use timezone-naive to match DB
        
        # Get when the user started following this cluster
        follow_query = """
            SELECT created_at 
            FROM cluster_user.user_cluster_follows 
            WHERE user_id = $1 AND cluster_id = $2
        """
        follow_row = await user_conn.fetchrow(follow_query, user_id, cluster_id)
        
        # Use the later of: when user started following OR last notification time
        if follow_row and follow_row['created_at']:
            follow_start = follow_row['created_at']
            # Convert to timezone-naive if needed to match database format
            if hasattr(follow_start, 'tzinfo') and follow_start.tzinfo is not None:
                follow_start = follow_start.replace(tzinfo=None)
            if hasattr(since, 'tzinfo') and since.tzinfo is not None:
                since = since.replace(tzinfo=None)
            # Use the later date to ensure we only show articles added after following
            since = max(since, follow_start)
            
        query = """
            SELECT 
                rfr.rss_feeds_raw_id,
                rfr.rss_feeds_raw_xml->>'title' as rss_feeds_raw_title,
                rfr.rss_feeds_raw_xml->>'link' as rss_feeds_raw_link,
                rfr.rss_feeds_raw_published_date,
                rfc.rss_feeds_clean_content,
                rf.rss_feeds_name,
                ca.cluster_articles_added_at as added_to_cluster_at
            FROM cluster_data.cluster_articles ca
            JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            WHERE ca.cluster_articles_cluster_id = $1
                AND ca.cluster_articles_added_at > $2
            ORDER BY ca.cluster_articles_added_at DESC
            LIMIT 10
        """
        
        rows = await cluster_conn.fetch(query, cluster_id, since)
        return [dict(row) for row in rows]
    
    async def get_cluster_details(self, conn: asyncpg.Connection, cluster_id: int) -> Optional[Dict[str, Any]]:
        """Get cluster details including name and metadata"""
        query = """
            SELECT 
                c.clusters_id as cluster_id,
                c.clusters_name as cluster_name,
                c.clusters_created_at as cluster_created_at,
                COUNT(DISTINCT ca.cluster_articles_clean_id) as total_articles
            FROM cluster_data.clusters c
            LEFT JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            WHERE c.clusters_id = $1
            GROUP BY c.clusters_id, c.clusters_name, c.clusters_created_at
        """
        
        row = await conn.fetchrow(query, cluster_id)
        return dict(row) if row else None
    
    async def update_last_notified(
        self, 
        user_conn: asyncpg.Connection, 
        user_id: str, 
        cluster_id: int
    ):
        """Update the last notification timestamp for a user-cluster follow"""
        query = """
            UPDATE cluster_user.user_cluster_follows
            SET last_notified_at = NOW()
            WHERE user_id = $1 AND cluster_id = $2
        """
        await user_conn.execute(query, user_id, cluster_id)
    
    async def record_notification(
        self,
        user_conn: asyncpg.Connection,
        user_id: str,
        cluster_id: int,
        article_id: int,
        status: str,
        error_message: Optional[str] = None
    ):
        """Record notification history"""
        query = """
            INSERT INTO cluster_user.cluster_notification_history 
            (user_id, cluster_id, article_id, email_status, error_message)
            VALUES ($1, $2, $3, $4, $5)
        """
        await user_conn.execute(query, user_id, cluster_id, article_id, status, error_message)
    
    def render_notification_email(
        self, 
        user: Dict[str, Any], 
        cluster: Dict[str, Any], 
        new_articles: List[Dict[str, Any]]
    ) -> tuple[str, str]:
        """Render email content for cluster notifications"""
        
        # HTML template with red brand colors
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
                .article { background: #f9fafb; padding: 20px; margin: 20px 0; border-radius: 8px; border-left: 4px solid #dc2626; }
                .article h3 { margin-top: 0; margin-bottom: 12px; color: #dc2626; font-size: 18px; font-weight: 600; line-height: 1.3; }
                .meta { color: #6b7280; font-size: 14px; margin: 12px 0; }
                .meta strong { color: #374151; }
                .article-content { color: #4b5563; margin: 12px 0; line-height: 1.5; }
                .button { display: inline-block; padding: 12px 30px; background: #dc2626; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; font-weight: 500; }
                .button:hover { background: #b91c1c; }
                .footer { text-align: center; padding: 20px; color: #6b7280; font-size: 14px; background: #f9fafb; }
                .footer a { color: #dc2626; text-decoration: none; }
                .footer a:hover { text-decoration: underline; }
                a { color: #dc2626; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔔 New Updates: {{ cluster.cluster_name }}</h1>
                </div>
                
                <div class="content">
                    <p>Hi {{ user.users_name or 'there' }},</p>
                    
                    <p><strong>{{ new_articles|length }} new article{% if new_articles|length > 1 %}s have{% else %} has{% endif %} been added to the cluster you're following:</strong></p>
                    
                    {% for article in new_articles %}
                    <div class="article">
                        <h3>{{ article.rss_feeds_raw_title }}</h3>
                        <div class="meta">
                            <strong>Source:</strong> {{ article.rss_feeds_name }}<br>
                            <strong>Published:</strong> {{ article.rss_feeds_raw_published_date.strftime('%B %d, %Y at %I:%M %p UTC') }}
                        </div>
                        <div class="article-content">{{ article.clean_content[:300] }}{% if article.clean_content|length > 300 %}...{% endif %}</div>
                        <a href="{{ article.rss_feeds_raw_link }}" target="_blank">Read full article →</a>
                    </div>
                    {% endfor %}
                    
                    <div style="text-align: center;">
                        <a href="https://threatcluster.io/clusters/{{ cluster.cluster_id }}" class="button">
                            View Full Cluster Analysis
                        </a>
                    </div>
                    
                    <p style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #e5e7eb; color: #6b7280;">
                        <small>This cluster now contains <strong>{{ cluster.total_articles }}</strong> total articles.</small>
                    </p>
                </div>
                
                <div class="footer">
                    <p>
                        You're receiving this because you're following this story on ThreatCluster.<br>
                        <a href="https://threatcluster.io/settings/notifications">Manage your notifications</a> | 
                        <a href="https://threatcluster.io/clusters/{{ cluster.cluster_id }}/unfollow">Unfollow this story</a>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """)
        
        # Plain text template
        text_template = Template("""
New Updates: {{ cluster.cluster_name }}

Hi {{ user.users_name or 'there' }},

{{ new_articles|length }} new article{% if new_articles|length > 1 %}s have{% else %} has{% endif %} been added to the cluster you're following:

{% for article in new_articles %}
---
{{ article.rss_feeds_raw_title }}
Source: {{ article.rss_feeds_name }}
Published: {{ article.rss_feeds_raw_published_date.strftime('%B %d, %Y at %I:%M %p UTC') }}

{{ article.clean_content[:200] }}{% if article.clean_content|length > 200 %}...{% endif %}

Read more: {{ article.rss_feeds_raw_link }}

{% endfor %}

View full cluster analysis: https://threatcluster.io/clusters/{{ cluster.cluster_id }}

This cluster now contains {{ cluster.total_articles }} total articles.

---
You're receiving this because you're following this story on ThreatCluster.
Manage notifications: https://threatcluster.io/settings/notifications
Unfollow this story: https://threatcluster.io/clusters/{{ cluster.cluster_id }}/unfollow
        """)
        
        # Process articles to extract clean content from JSON
        processed_articles = []
        for article in new_articles:
            processed_article = dict(article)
            
            # Extract clean content from JSON
            clean_content = ""
            if article.get('rss_feeds_clean_content'):
                content_json = article['rss_feeds_clean_content']
                if isinstance(content_json, dict):
                    clean_content = content_json.get('content', '')
                elif isinstance(content_json, str):
                    try:
                        import json
                        content_data = json.loads(content_json)
                        clean_content = content_data.get('content', '')
                    except:
                        clean_content = content_json
                else:
                    clean_content = str(content_json)
            
            processed_article['clean_content'] = clean_content or "No content preview available"
            processed_articles.append(processed_article)
        
        html_content = html_template.render(
            user=user,
            cluster=cluster,
            new_articles=processed_articles
        )
        
        text_content = text_template.render(
            user=user,
            cluster=cluster,
            new_articles=processed_articles
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
            # Use cluster-specific from email if configured, otherwise fall back to default
            from_email = getattr(settings, 'cluster_notification_from_email', self.from_email)
            
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
            logger.info(f"Sent notification via AWS SES to {to_email}, MessageId: {response['MessageId']}")
            return True
        except ClientError as e:
            logger.error(f"AWS SES error sending to {to_email}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email to {to_email}: {e}")
            return False
    
    
    async def check_and_notify_cluster_updates(
        self,
        cluster_conn: asyncpg.Connection,
        user_conn: asyncpg.Connection,
        cluster_id: int
    ):
        """Check for new articles in a cluster and notify followers"""
        
        # Get cluster details
        cluster = await self.get_cluster_details(cluster_conn, cluster_id)
        if not cluster:
            logger.warning(f"Cluster {cluster_id} not found")
            return
        
        # Get followers
        followers = await self.get_cluster_followers(user_conn, cluster_id)
        if not followers:
            logger.info(f"No followers for cluster {cluster_id}")
            return
        
        logger.info(f"Found {len(followers)} followers for cluster {cluster_id}: {cluster['cluster_name']}")
        
        # Process each follower
        notifications_sent = 0
        for follower in followers:
            # Use transaction for each follower
            async with user_conn.transaction():
                try:
                    # Get new articles since last notification
                    last_notified = follower.get('last_notified_at') or datetime.now() - timedelta(days=7)
                    new_articles = await self.get_new_articles_for_cluster(
                        cluster_conn,
                        user_conn,
                        cluster_id,
                        follower['users_id'],
                        last_notified
                    )
                    
                    if not new_articles:
                        continue
                    
                    # Render email
                    html_content, text_content = self.render_notification_email(
                        follower,
                        cluster,
                        new_articles
                    )
                    
                    # Send email
                    subject = f"New updates: {cluster['cluster_name']}"
                    success = await self.send_notification_email(
                        follower['users_email'],
                        subject,
                        html_content,
                        text_content
                    )
                    
                    if success:
                        # Update last notified timestamp
                        await self.update_last_notified(
                            user_conn,
                            follower['users_id'],
                            cluster_id
                        )
                        
                        # Record notification history for first article
                        await self.record_notification(
                            user_conn,
                            follower['users_id'],
                            cluster_id,
                            new_articles[0]['rss_feeds_raw_id'],
                            'sent'
                        )
                        
                        notifications_sent += 1
                    else:
                        # Record failure
                        await self.record_notification(
                            user_conn,
                            follower['users_id'],
                            cluster_id,
                            new_articles[0]['rss_feeds_raw_id'],
                            'failed',
                            'Email send failed'
                        )
                        
                except Exception as e:
                    logger.error(f"Error notifying user {follower['users_id']} for cluster {cluster_id}: {e}")
                    # Transaction will automatically rollback on exception
        
        logger.info(f"Sent {notifications_sent} notifications for cluster {cluster_id}")
    
    async def check_all_clusters_for_updates(self):
        """Check all clusters with followers for new articles"""
        
        # Connect to databases
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        
        try:
            # Get clusters that have followers
            query = """
                SELECT DISTINCT cluster_id 
                FROM cluster_user.user_cluster_follows 
                WHERE notification_enabled = true
            """
            
            rows = await user_conn.fetch(query)
            cluster_ids = [row['cluster_id'] for row in rows]
            
            logger.info(f"Checking {len(cluster_ids)} clusters for updates")
            
            for cluster_id in cluster_ids:
                await self.check_and_notify_cluster_updates(
                    cluster_conn,
                    user_conn,
                    cluster_id
                )
                
        finally:
            await cluster_conn.close()
            await user_conn.close()


async def main():
    """Test the notification service"""
    service = ClusterNotificationService()
    await service.check_all_clusters_for_updates()


if __name__ == "__main__":
    asyncio.run(main())