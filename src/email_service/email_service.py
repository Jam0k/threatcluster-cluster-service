"""
Daily Email Service for ThreatCluster
Sends daily threat intelligence bulletins to subscribed users
"""

import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
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
        return [{str(k) if k is not None else 'unknown': v for k, v in dict(row).items()} for row in rows]
    
    async def get_top_clusters(self, conn: asyncpg.Connection, limit: int = 5) -> List[Dict[str, Any]]:
        """Get top clusters from the last 24 hours based on article publish time"""
        query = """
            WITH cluster_article_times AS (
                -- Get the most recent article time for each cluster
                SELECT 
                    c.clusters_id,
                    MAX(rfr.rss_feeds_raw_published_date) as latest_article_time,
                    MIN(rfr.rss_feeds_raw_published_date) as earliest_article_time
                FROM cluster_data.clusters c
                JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                WHERE c.clusters_is_active = true
                GROUP BY c.clusters_id
                HAVING MAX(rfr.rss_feeds_raw_published_date) >= NOW() - INTERVAL '24 hours'
            ),
            cluster_threat_indicators AS (
                -- Check for actual threat indicators in clusters
                SELECT 
                    c.clusters_id,
                    -- Count threat-related entities
                    COUNT(DISTINCT CASE 
                        WHEN e.entities_category IN ('cve', 'malware_family', 'apt_group', 'ransomware_group', 
                                                    'vulnerability_type', 'attack_type') 
                        THEN e.entities_id 
                    END) as threat_entity_count,
                    -- Check for security keywords in titles
                    BOOL_OR(
                        rfc.rss_feeds_clean_title->>'title' ~* 
                        '(breach|attack|vulnerability|exploit|ransomware|malware|compromise|incident|threat|cve-|defac|phish|trojan|backdoor)'
                    ) as has_threat_keywords,
                    -- Exclude product announcements
                    BOOL_OR(
                        rfc.rss_feeds_clean_title->>'title' ~* 
                        '(now available|marketplace|launches|announces|partnership|integration|release|update.*version)'
                    ) as is_product_announcement
                FROM cluster_data.clusters c
                JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                LEFT JOIN LATERAL jsonb_array_elements(rfc.rss_feeds_clean_extracted_entities->'entities') AS ent ON true
                LEFT JOIN cluster_data.entities e ON (ent->>'entities_id')::int = e.entities_id
                GROUP BY c.clusters_id
            )
            SELECT 
                cr.clusters_id,
                cr.clusters_name,
                cr.clusters_summary,
                cr.cluster_score as ranking_score,
                cr.article_count,
                cr.clusters_created_at,
                cr.avg_article_score,
                cr.clusters_coherence_score,
                cat.latest_article_time,
                cat.earliest_article_time,
                cti.threat_entity_count,
                cti.has_threat_keywords,
                -- Get key entities for display
                ARRAY(
                    SELECT DISTINCT e.entities_name FROM (
                        SELECT e.entities_name, e.entities_category, MAX(e.entities_importance_weight) as weight
                        FROM cluster_data.cluster_articles ca2
                        JOIN cluster_data.rss_feeds_clean rfc2 ON ca2.cluster_articles_clean_id = rfc2.rss_feeds_clean_id
                        JOIN LATERAL jsonb_array_elements(rfc2.rss_feeds_clean_extracted_entities->'entities') AS ent ON true
                        JOIN cluster_data.entities e ON (ent->>'entities_id')::int = e.entities_id
                        WHERE ca2.cluster_articles_cluster_id = cr.clusters_id
                            AND e.entities_category IN ('cve', 'malware_family', 'apt_group', 'ransomware_group', 'vulnerability_type')
                            AND e.entities_importance_weight >= 70
                        GROUP BY e.entities_name, e.entities_category
                        ORDER BY MAX(e.entities_importance_weight) DESC, e.entities_name
                        LIMIT 5
                    ) e
                ) as key_entities
            FROM cluster_data.cluster_rankings cr
            JOIN cluster_article_times cat ON cr.clusters_id = cat.clusters_id
            JOIN cluster_threat_indicators cti ON cr.clusters_id = cti.clusters_id
            WHERE cr.article_count >= 2
                AND cr.cluster_score IS NOT NULL
                -- Filter for actual threats
                AND (cti.threat_entity_count > 0 OR cti.has_threat_keywords = true)
                -- Exclude product announcements
                AND cti.is_product_announcement = false
                -- Minimum score threshold for email inclusion
                AND cr.cluster_score >= 40
            ORDER BY cr.cluster_score DESC
            LIMIT $1
        """
        
        rows = await conn.fetch(query, limit)
        clusters = []
        
        for row in rows:
            # Convert row to dict and ensure no None keys
            cluster = {str(k) if k is not None else 'unknown': v for k, v in dict(row).items()}
            
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
            # Ensure no None keys in article dicts
            cluster['articles'] = [{str(k) if k is not None else 'unknown': v for k, v in dict(article).items()} for article in article_rows]
            
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
            Create a compelling 2-3 sentence security threat summary for this cluster that answers:
            1. What is the specific threat, vulnerability, or security incident?
            2. Who is targeted, compromised, or affected?
            3. What immediate action should security teams take?
            
            Cluster: {cluster.get('display_name', 'Unknown')}
            Score: {cluster.get('ranking_score', 0)}/100
            Threat Entities: {cluster.get('threat_entity_count', 0)}
            
            Articles ({len(articles)} total):
            {chr(10).join(article_info)}
            
            Focus ONLY on actual security threats, breaches, vulnerabilities, or attacks.
            Do NOT summarize product announcements, integrations, or partnerships.
            Be specific about threat actors, malware, CVEs, or companies breached.
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
    
    def create_email_html(self, clusters: List[Dict[str, Any]], trending_entities: List[Dict[str, Any]], 
                          trending_cves: List[Dict[str, Any]], user_name: Optional[str] = None) -> str:
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
            color: #E5E5E5 !important;
        }
        .summary-intro strong {
            color: #E5E5E5 !important;
        }
        .cluster {
            background-color: #292524;
            border: 1px solid #3F3F46;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }
        .cluster-header {
            margin-bottom: 15px;
        }
        .cluster-title {
            font-size: 18px;
            font-weight: 600;
            color: #E5E5E5;
            margin: 0;
        }
        .cluster-summary {
            color: #A8A29E;
            margin-bottom: 15px;
            font-size: 15px;
            line-height: 1.5;
        }
        .cluster-entities {
            background-color: #1F1F23;
            border-left: 3px solid #DC2626;
            padding: 10px 15px;
            margin-bottom: 15px;
            font-size: 14px;
            color: #E5E5E5;
            border-radius: 4px;
        }
        .cluster-entities strong {
            color: #DC2626;
            margin-right: 5px;
        }
        .trending-section {
            background-color: #1C1917;
            border: 1px solid #3F3F46;
            border-radius: 8px;
            padding: 25px 20px;
            margin: 30px 0;
        }
        .trending-title {
            font-size: 20px;
            font-weight: 600;
            color: #DC2626;
            margin: 0 0 20px 0;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .trending-grid {
            width: 100%;
        }
        .trending-row {
            width: 100%;
            margin-bottom: 15px;
        }
        .trending-item {
            background-color: #292524;
            border-left: 3px solid #DC2626;
            padding: 10px 12px;
            border-radius: 4px;
            min-height: 60px;
        }
        .trending-name {
            font-weight: 600;
            color: #E5E5E5;
            font-size: 14px;
            margin-bottom: 4px;
            word-break: break-word;
            overflow-wrap: break-word;
        }
        .trending-meta {
            font-size: 12px;
            color: #A8A29E;
        }
        .trending-category {
            text-transform: uppercase;
            font-size: 11px;
            color: #DC2626;
            font-weight: 500;
        }
        .cve-section {
            background-color: #1C1917;
            border: 1px solid #3F3F46;
            border-radius: 8px;
            padding: 25px 20px;
            margin: 30px 0;
        }
        .cve-title {
            font-size: 20px;
            font-weight: 600;
            color: #DC2626;
            margin: 0 0 20px 0;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .cve-grid {
            width: 100%;
        }
        .cve-row {
            width: 100%;
            margin-bottom: 15px;
        }
        .cve-item {
            background-color: #292524;
            border-left: 3px solid #DC2626;
            padding: 10px 12px;
            border-radius: 4px;
        }
        .cve-item-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
            width: 100%;
        }
        .cve-name {
            font-weight: 600;
            color: #E5E5E5;
            font-size: 14px;
            font-family: monospace;
            word-break: break-all;
            overflow-wrap: break-word;
        }
        .cve-count {
            font-size: 12px;
            color: #A8A29E;
            background-color: #1F1F23;
            padding: 2px 8px;
            border-radius: 12px;
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
                <strong>🔴 Today's Critical Security Threats</strong><br>
                {% if cluster_count > 0 %}
                We've analyzed thousands of security articles from the last 24 hours and identified {{ cluster_count }} significant threat clusters. 
                Here are the top {{ top_count }} highest-priority threats that require immediate attention.
                {% else %}
                No significant security threats were detected in the last 24 hours that met our threat criteria. 
                This could indicate a quiet period in the threat landscape.
                {% endif %}
            </div>
            
            {% for cluster in clusters %}
            <div class="cluster">
                <div class="cluster-header">
                    <h2 class="cluster-title">{{ cluster.display_name }}</h2>
                </div>
                
                <p class="cluster-summary">{{ cluster.summary }}</p>
                
                {% if cluster.key_entities %}
                <div class="cluster-entities">
                    <strong>Key Entities:</strong> {{ cluster.key_entities | join(', ') }}
                </div>
                {% endif %}
                
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
        
        # Calculate time ago for each cluster based on latest article
        now = datetime.now(timezone.utc)
        for cluster in clusters:
            # Use latest_article_time if available, otherwise fall back to cluster creation time
            time_field = cluster.get('latest_article_time') or cluster.get('clusters_created_at')
            if time_field:
                if isinstance(time_field, str):
                    time_field = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
                
                # Ensure time_field is timezone-aware
                if time_field.tzinfo is None:
                    time_field = time_field.replace(tzinfo=timezone.utc)
                
                time_diff = now - time_field
                hours_ago = int(time_diff.total_seconds() / 3600)
                if hours_ago < 1:
                    cluster['time_ago'] = "Just now"
                elif hours_ago == 1:
                    cluster['time_ago'] = "1 hour ago"
                else:
                    cluster['time_ago'] = f"{hours_ago} hours ago"
            else:
                cluster['time_ago'] = "Recent"
            
            # Convert datetime objects to strings after time_ago calculation
            if 'clusters_created_at' in cluster and cluster['clusters_created_at'] and hasattr(cluster['clusters_created_at'], 'isoformat'):
                cluster['clusters_created_at'] = cluster['clusters_created_at'].isoformat()
            if 'latest_article_time' in cluster and cluster['latest_article_time'] and hasattr(cluster['latest_article_time'], 'isoformat'):
                cluster['latest_article_time'] = cluster['latest_article_time'].isoformat()
            if 'earliest_article_time' in cluster and cluster['earliest_article_time'] and hasattr(cluster['earliest_article_time'], 'isoformat'):
                cluster['earliest_article_time'] = cluster['earliest_article_time'].isoformat()
        
        return template.render(
            date=datetime.now(timezone.utc).strftime("%B %d, %Y"),
            user_name=user_name,
            cluster_count=len(clusters),
            top_count=min(5, len(clusters)),
            clusters=clusters,
            trending_entities=trending_entities,
            trending_cves=trending_cves
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
            "MessageStream": "outbound"  # Changed from "broadcast" to "outbound"
        }
        
        try:
            # Debug logging
            logger.debug(f"Email data - From: {self.from_email}, To: {to_email}, Subject: {subject[:50]}...")
            
            # Ensure all values are strings and not None
            if not all([self.from_email, to_email, subject, html_body]):
                logger.error(f"Missing required email fields - From: {self.from_email}, To: {to_email}, Subject: {bool(subject)}, Body: {bool(html_body)}")
                return False
            
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
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def get_trending_cves(self, conn: asyncpg.Connection, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending CVEs from the last 24 hours"""
        query = """
            WITH recent_cves AS (
                SELECT 
                    e.entities_name,
                    COUNT(DISTINCT recent.rss_feeds_clean_id) as article_count,
                    COUNT(DISTINCT c.clusters_id) as cluster_count,
                    MAX(e.entities_importance_weight) as importance
                FROM cluster_data.entities e
                JOIN LATERAL (
                    SELECT rfc.rss_feeds_clean_id, ent->>'entities_id' as entity_id
                    FROM cluster_data.rss_feeds_clean rfc
                    JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                    CROSS JOIN LATERAL jsonb_array_elements(rfc.rss_feeds_clean_extracted_entities->'entities') AS ent
                    WHERE rfr.rss_feeds_raw_published_date >= NOW() - INTERVAL '24 hours'
                        AND (ent->>'entities_id')::int = e.entities_id
                ) recent ON true
                LEFT JOIN cluster_data.cluster_articles ca ON ca.cluster_articles_clean_id = recent.rss_feeds_clean_id
                LEFT JOIN cluster_data.clusters c ON ca.cluster_articles_cluster_id = c.clusters_id AND c.clusters_is_active = true
                WHERE e.entities_category = 'cve'
                GROUP BY e.entities_name
                HAVING COUNT(DISTINCT recent.rss_feeds_clean_id) >= 1  -- Show CVEs mentioned even once
            )
            SELECT 
                entities_name,
                article_count,
                cluster_count
            FROM recent_cves
            ORDER BY article_count DESC, entities_name
            LIMIT $1
        """
        
        rows = await conn.fetch(query, limit)
        return [{str(k) if k is not None else 'unknown': v for k, v in dict(row).items()} for row in rows]
    
    async def get_trending_entities(self, conn: asyncpg.Connection, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending security entities from the last 24 hours"""
        query = """
            WITH recent_entities AS (
                SELECT 
                    e.entities_name,
                    e.entities_category,
                    e.entities_importance_weight,
                    COUNT(DISTINCT recent.rss_feeds_clean_id) as article_count,
                    COUNT(DISTINCT c.clusters_id) as cluster_count
                FROM cluster_data.entities e
                JOIN LATERAL (
                    SELECT rfc.rss_feeds_clean_id, ent->>'entities_id' as entity_id
                    FROM cluster_data.rss_feeds_clean rfc
                    JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                    CROSS JOIN LATERAL jsonb_array_elements(rfc.rss_feeds_clean_extracted_entities->'entities') AS ent
                    WHERE rfr.rss_feeds_raw_published_date >= NOW() - INTERVAL '24 hours'
                        AND (ent->>'entities_id')::int = e.entities_id
                ) recent ON true
                LEFT JOIN cluster_data.cluster_articles ca ON ca.cluster_articles_clean_id = recent.rss_feeds_clean_id
                LEFT JOIN cluster_data.clusters c ON ca.cluster_articles_cluster_id = c.clusters_id AND c.clusters_is_active = true
                WHERE e.entities_category IN ('malware_family', 'apt_group', 'ransomware_group', 
                                            'vulnerability_type', 'attack_type', 'company')
                    AND e.entities_importance_weight >= 60
                GROUP BY e.entities_name, e.entities_category, e.entities_importance_weight
                HAVING COUNT(DISTINCT recent.rss_feeds_clean_id) >= 2  -- Mentioned in at least 2 articles
            )
            SELECT 
                entities_name,
                entities_category,
                article_count,
                cluster_count,
                CASE 
                    WHEN entities_category = 'cve' THEN 1
                    WHEN entities_category = 'ransomware_group' THEN 2
                    WHEN entities_category = 'apt_group' THEN 3
                    WHEN entities_category = 'malware_family' THEN 4
                    WHEN entities_category = 'vulnerability_type' THEN 5
                    WHEN entities_category = 'attack_type' THEN 6
                    WHEN entities_category = 'company' THEN 7
                    ELSE 8
                END as category_priority
            FROM recent_entities
            ORDER BY 
                article_count DESC,
                category_priority,
                entities_name
            LIMIT $1
        """
        
        rows = await conn.fetch(query, limit)
        return [{str(k) if k is not None else 'unknown': v for k, v in dict(row).items()} for row in rows]
    
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
                logger.info("No threat clusters found for daily bulletin - only product announcements or non-threat content")
                # Optionally send a "no threats" email or skip
                return  # Skip sending email when no real threats
            
            # Get trending entities and CVEs
            trending_entities = await self.get_trending_entities(cluster_conn, limit=10)
            logger.info(f"Found {len(trending_entities)} trending entities")
            
            trending_cves = await self.get_trending_cves(cluster_conn, limit=10)
            logger.info(f"Found {len(trending_cves)} trending CVEs")
            
            # Convert datetime objects to strings before generating summaries
            for cluster in clusters:
                # Convert datetime fields to strings to avoid serialization issues
                if 'clusters_created_at' in cluster and cluster['clusters_created_at'] and hasattr(cluster['clusters_created_at'], 'isoformat'):
                    cluster['clusters_created_at'] = cluster['clusters_created_at'].isoformat()
                if 'latest_article_time' in cluster and cluster['latest_article_time'] and hasattr(cluster['latest_article_time'], 'isoformat'):
                    cluster['latest_article_time'] = cluster['latest_article_time'].isoformat()
                if 'earliest_article_time' in cluster and cluster['earliest_article_time'] and hasattr(cluster['earliest_article_time'], 'isoformat'):
                    cluster['earliest_article_time'] = cluster['earliest_article_time'].isoformat()
                
                # Also convert article datetimes
                for article in cluster.get('articles', []):
                    if 'published_date' in article and article['published_date'] and hasattr(article['published_date'], 'isoformat'):
                        article['published_date'] = article['published_date'].isoformat()
                
                cluster['summary'] = await self.generate_cluster_summary(cluster)
            
            # Send emails to each user
            subject = f"ThreatCluster Daily Brief - {datetime.now(timezone.utc).strftime('%B %d, %Y')}"
            success_count = 0
            
            for user in users:
                user_name = user.get('users_name')
                email = user.get('users_email')
                
                if not email:
                    logger.warning(f"User {user.get('users_id')} has no email address")
                    continue
                
                # Create personalized email
                html_body = self.create_email_html(clusters, trending_entities, trending_cves, user_name)
                
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