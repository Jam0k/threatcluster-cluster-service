"""
Daily Email Service for ThreatCluster
Sends daily threat intelligence bulletins to subscribed users
"""

import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
from jinja2 import Template

from src.config.settings import settings
import asyncpg
import openai

logger = logging.getLogger(__name__)


class EmailService:
    """Service for sending daily threat bulletins"""
    
    def __init__(self):
        self.postmark_token = settings.postmark_api_token
        self.postmark_url = "https://api.postmarkapp.com/email"
        self.from_email = settings.email_from_address
        self.openai_client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        
    async def get_subscribed_users(self, conn: asyncpg.Connection) -> List[Dict[str, Any]]:
        """Get all users who have enabled daily email bulletins"""
        query = """
            SELECT 
                users_id,
                users_email,
                users_name,
                users_metadata
            FROM cluster_user.users
            WHERE users_is_active = true
                AND users_email IS NOT NULL
                AND users_metadata->>'daily_email_enabled' = 'true'
        """
        
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]
    
    async def get_top_clusters(self, conn: asyncpg.Connection, limit: int = 5) -> List[Dict[str, Any]]:
        """Get top clusters from the last 24 hours"""
        query = """
            SELECT 
                cr.clusters_id,
                cr.clusters_name,
                cr.clusters_summary,
                cr.cluster_score as ranking_score,
                cr.article_count,
                cr.clusters_created_at,
                cr.avg_article_score,
                cr.clusters_coherence_score
            FROM cluster_data.cluster_rankings cr
            WHERE cr.clusters_created_at >= NOW() - INTERVAL '24 hours'
                AND cr.article_count >= 2
                AND cr.cluster_score IS NOT NULL
            ORDER BY cr.cluster_score DESC
            LIMIT $1
        """
        
        rows = await conn.fetch(query, limit)
        clusters = []
        
        for row in rows:
            cluster = dict(row)
            
            # Get articles for this cluster
            articles_query = """
                SELECT 
                    rfc.rss_feeds_clean_id,
                    rfc.rss_feeds_clean_title->>'title' as title,
                    rfc.rss_feeds_clean_content->>'content' as content,
                    rfr.rss_feeds_raw_xml->>'link' as url,
                    ca.cluster_articles_is_primary
                FROM cluster_data.cluster_articles ca
                JOIN cluster_data.rss_feeds_clean rfc 
                    ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                JOIN cluster_data.rss_feeds_raw rfr
                    ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                WHERE ca.cluster_articles_cluster_id = $1
                ORDER BY ca.cluster_articles_is_primary DESC, 
                         ca.cluster_articles_similarity_score DESC
                LIMIT 5
            """
            
            article_rows = await conn.fetch(articles_query, cluster['clusters_id'])
            cluster['articles'] = [dict(article) for article in article_rows]
            
            clusters.append(cluster)
            
        return clusters
    
    async def generate_cluster_summary(self, cluster: Dict[str, Any]) -> str:
        """Generate a brief AI summary for a cluster"""
        try:
            # Get the first few article titles and content
            articles = cluster.get('articles', [])[:3]
            article_info = []
            
            for article in articles:
                title = article.get('title', 'No title')
                content = article.get('content', '')[:200]
                article_info.append(f"- {title}: {content}")
            
            prompt = f"""
            Create a very brief (2-3 sentences) security threat summary for this cluster:
            
            Cluster Name: {cluster.get('clusters_name', 'Unknown')}
            
            Articles:
            {chr(10).join(article_info)}
            
            Focus on: What is the threat? Who is affected? Why is it important?
            Keep it concise and actionable for security professionals.
            """
            
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a cybersecurity analyst creating brief, actionable threat summaries. Do not include any titles or headers like 'Threat Summary' - just provide the summary text directly."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )
            
            # Remove any "Threat Summary:" prefix if present
            summary = response.choices[0].message.content.strip()
            if summary.startswith("**Threat Summary**:"):
                summary = summary.replace("**Threat Summary**:", "").strip()
            if summary.startswith("Threat Summary:"):
                summary = summary.replace("Threat Summary:", "").strip()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary for cluster {cluster.get('clusters_id')}: {e}")
            # Fallback to basic summary
            return f"Security cluster containing {cluster.get('article_count', 0)} related articles about {cluster.get('clusters_name', 'emerging threats')}."
    
    def create_email_html(self, clusters: List[Dict[str, Any]], user_name: Optional[str] = None) -> str:
        """Create HTML email content"""
        # Get the primary article title for each cluster
        for cluster in clusters:
            if cluster.get('articles'):
                for article in cluster['articles']:
                    if article.get('cluster_articles_is_primary'):
                        cluster['display_name'] = article.get('title', cluster.get('clusters_name', 'Security Cluster'))
                        break
                else:
                    # If no primary article, use first article title
                    cluster['display_name'] = cluster['articles'][0].get('title', cluster.get('clusters_name', 'Security Cluster'))
            else:
                cluster['display_name'] = cluster.get('clusters_name', 'Security Cluster')
        
        template = Template("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily Threat Intelligence Brief</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #E5E5E5;
            background-color: #0A0A0A;
            margin: 0;
            padding: 0;
        }
        .wrapper {
            background-color: #0A0A0A;
            padding: 20px 0;
        }
        .container {
            max-width: 600px;
            margin: 0 auto;
            background-color: #1C1917;
            border-radius: 8px;
            overflow: hidden;
        }
        .header {
            background-color: #DC2626;
            color: #ffffff;
            padding: 30px 20px;
            text-align: center;
            border-radius: 8px 8px 0 0;
        }
        .header h1 {
            margin: 0;
            font-size: 24px;
            font-weight: 600;
        }
        .date {
            font-size: 14px;
            opacity: 0.9;
            margin-top: 5px;
        }
        .content {
            padding: 30px 20px;
        }
        .greeting {
            font-size: 18px;
            margin-bottom: 20px;
            color: #E5E5E5;
        }
        .summary-intro {
            background-color: #292524;
            border-left: 4px solid #DC2626;
            padding: 15px;
            margin-bottom: 30px;
            border-radius: 4px;
            color: #E5E5E5;
        }
        .cluster {
            background-color: #292524;
            border: 1px solid #3F3F46;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }
        .cluster-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 15px;
        }
        .cluster-title {
            font-size: 18px;
            font-weight: 600;
            color: #E5E5E5;
            margin: 0;
            flex: 1;
            padding-right: 10px;
        }
        .cluster-score {
            background-color: #DC2626;
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 500;
            white-space: nowrap;
        }
        .cluster-summary {
            color: #A8A29E;
            margin-bottom: 15px;
            font-size: 15px;
            line-height: 1.5;
        }
        .cluster-meta {
            display: flex;
            gap: 20px;
            font-size: 14px;
            color: #A8A29E;
            margin-bottom: 15px;
        }
        .cluster-meta-item {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .view-button {
            display: inline-block;
            background-color: #DC2626;
            color: white !important;
            text-decoration: none;
            padding: 10px 20px;
            border-radius: 5px;
            font-weight: 500;
            font-size: 14px;
            transition: background-color 0.2s;
        }
        .view-button:hover {
            background-color: #B91C1C;
            color: white !important;
        }
        a {
            color: #DC2626;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .footer {
            background-color: #292524;
            padding: 30px 20px;
            text-align: center;
            font-size: 14px;
            color: #A8A29E;
            border-top: 1px solid #3F3F46;
            margin-top: 30px;
        }
        .unsubscribe {
            color: #DC2626;
            text-decoration: none;
        }
        .unsubscribe:hover {
            text-decoration: underline;
        }
        .powered-by {
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #3F3F46;
            color: #E5E5E5;
            font-size: 14px;
        }
        .ai-badge {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background-color: #3F3F46;
            padding: 8px 16px;
            border-radius: 20px;
            margin-top: 10px;
            font-weight: 500;
        }
        .robot-icon {
            color: #DC2626;
            font-size: 18px;
        }
        .slogan {
            margin-top: 15px;
            font-size: 16px;
            color: #E5E5E5;
            letter-spacing: 1px;
            font-weight: 500;
        }
    </style>
</head>
<body>
    <div class="wrapper">
        <div class="container">
        <div class="header">
            <h1>ThreatCluster Daily Brief</h1>
            <div class="date">{{ date }}</div>
        </div>
        
        <div class="content">
            <div class="greeting">
                {% if user_name %}Hello {{ user_name }},{% else %}Hello,{% endif %}
            </div>
            
            <div class="summary-intro">
                <strong>Today's Top Security Threats</strong><br>
                We've identified {{ cluster_count }} critical threat clusters in the last 24 hours. 
                Here are the top {{ top_count }} that require your attention.
            </div>
            
            {% for cluster in clusters %}
            <div class="cluster">
                <div class="cluster-header">
                    <h2 class="cluster-title">{{ cluster.display_name }}</h2>
                    <span class="cluster-score">Score: {{ "%.0f"|format(cluster.ranking_score) }}</span>
                </div>
                
                <p class="cluster-summary">{{ cluster.summary }}</p>
                
                <div class="cluster-meta">
                    <div class="cluster-meta-item">
                        📄 {{ cluster.article_count }} articles
                    </div>
                    <div class="cluster-meta-item">
                        🕐 {{ cluster.time_ago }}
                    </div>
                </div>
                
                <a href="https://threatcluster.io/clusters/{{ cluster.clusters_id }}" class="view-button">
                    View Full Analysis →
                </a>
            </div>
            {% endfor %}
        </div>
        
        <div class="footer">
            <p>You're receiving this because you've subscribed to ThreatCluster daily briefs.</p>
            <p><a href="https://threatcluster.io/profile" class="unsubscribe">Manage email preferences</a></p>
            
            <div class="powered-by">
                <p>Powered by</p>
                <div class="ai-badge">
                    <span class="robot-icon">🤖</span>
                    <span>Cluster AI</span>
                </div>
                <p class="slogan">Connected Intelligence</p>
            </div>
        </div>
        </div>
    </div>
</body>
</html>
        """)
        
        # Calculate time ago for each cluster
        now = datetime.utcnow()
        for cluster in clusters:
            created_at = cluster['clusters_created_at']
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            
            time_diff = now - created_at
            hours_ago = int(time_diff.total_seconds() / 3600)
            if hours_ago < 1:
                cluster['time_ago'] = "Just now"
            elif hours_ago == 1:
                cluster['time_ago'] = "1 hour ago"
            else:
                cluster['time_ago'] = f"{hours_ago} hours ago"
        
        return template.render(
            date=datetime.utcnow().strftime("%B %d, %Y"),
            user_name=user_name,
            cluster_count=len(clusters),
            top_count=min(5, len(clusters)),
            clusters=clusters
        )
    
    async def send_email(self, to_email: str, subject: str, html_body: str) -> bool:
        """Send email via Postmark API"""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Postmark-Server-Token": self.postmark_token
        }
        
        data = {
            "From": self.from_email,
            "To": to_email,
            "Subject": subject,
            "HtmlBody": html_body,
            "MessageStream": "broadcast"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.postmark_url, headers=headers, json=data) as response:
                    if response.status == 200:
                        logger.info(f"Email sent successfully to {to_email}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to send email to {to_email}: {response.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {e}")
            return False
    
    async def send_daily_bulletins(self):
        """Main method to send daily bulletins to all subscribed users"""
        user_conn = None
        cluster_conn = None
        try:
            # Connect to both databases
            user_conn = await asyncpg.connect(settings.user_database_url)
            cluster_conn = await asyncpg.connect(settings.database_url)
            
            # Get subscribed users
            users = await self.get_subscribed_users(user_conn)
            logger.info(f"Found {len(users)} users subscribed to daily bulletins")
            
            if not users:
                logger.info("No users subscribed to daily bulletins")
                return
            
            # Get top clusters
            clusters = await self.get_top_clusters(cluster_conn)
            logger.info(f"Found {len(clusters)} top clusters from last 24 hours")
            
            if not clusters:
                logger.info("No clusters found for daily bulletin")
                return
            
            # Generate summaries for each cluster
            for cluster in clusters:
                cluster['summary'] = await self.generate_cluster_summary(cluster)
            
            # Send emails to each user
            subject = f"ThreatCluster Daily Brief - {datetime.utcnow().strftime('%B %d, %Y')}"
            success_count = 0
            
            for user in users:
                user_name = user.get('users_name')
                email = user['users_email']
                
                # Create personalized email
                html_body = self.create_email_html(clusters, user_name)
                
                # Send email
                if await self.send_email(email, subject, html_body):
                    success_count += 1
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
            
            logger.info(f"Successfully sent {success_count}/{len(users)} daily bulletins")
            
        except Exception as e:
            logger.error(f"Error in send_daily_bulletins: {e}")
            raise
        finally:
            if user_conn:
                await user_conn.close()
            if cluster_conn:
                await cluster_conn.close()


async def main():
    """Test the email service"""
    logging.basicConfig(level=logging.INFO)
    
    service = EmailService()
    await service.send_daily_bulletins()


if __name__ == "__main__":
    asyncio.run(main())