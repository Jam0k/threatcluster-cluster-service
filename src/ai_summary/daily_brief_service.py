"""
Daily Threat Brief Service - Generates daily AI-powered threat intelligence summaries as articles
"""
import os
import json
import logging
from datetime import datetime, timedelta, date, timezone
from typing import List, Dict, Optional, Any
import asyncio
from openai import AsyncOpenAI
import psycopg2
from psycopg2.extras import RealDictCursor, Json

from src.config.settings import settings

logger = logging.getLogger(__name__)


class DailyBriefService:
    """Service for generating daily AI threat intelligence briefs as articles"""
    
    def __init__(self):
        """Initialize the Daily Brief Service"""
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
        self.model = "gpt-4o-mini"
        self.max_retries = 3
        self.retry_delay = 2
        
    def check_existing_brief(self, brief_date: date) -> bool:
        """Check if a brief already exists for the given date"""
        # Check for existing brief by looking for the specific title pattern
        title = f"{brief_date} - Cluster AI Daily Threat Brief"
        query = """
        SELECT rss_feeds_clean_id 
        FROM cluster_data.rss_feeds_clean rfc
        JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
        WHERE rfc.rss_feeds_clean_title->>'title' = %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor() as cur:
                cur.execute(query, (title,))
                return cur.fetchone() is not None
        finally:
            conn.close()
    
    def get_or_create_ai_feed(self) -> int:
        """Get or create the Cluster AI feed"""
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor() as cur:
                # Check if Cluster AI feed exists
                cur.execute("""
                    SELECT rss_feeds_id FROM cluster_data.rss_feeds 
                    WHERE rss_feeds_name = 'Cluster AI'
                """)
                result = cur.fetchone()
                
                if result:
                    return result[0]
                
                # Create the feed if it doesn't exist - use ON CONFLICT to handle race conditions
                cur.execute("""
                    INSERT INTO cluster_data.rss_feeds (
                        rss_feeds_name,
                        rss_feeds_url,
                        rss_feeds_category,
                        rss_feeds_is_active,
                        rss_feeds_credibility,
                        rss_feeds_created_at
                    ) VALUES (
                        'Cluster AI',
                        'internal://cluster-ai/daily-briefs',
                        'cybersecurity',
                        true,
                        100,  -- Maximum credibility for AI-generated content
                        CURRENT_TIMESTAMP
                    ) 
                    ON CONFLICT (rss_feeds_url) DO UPDATE SET
                        rss_feeds_is_active = true
                    RETURNING rss_feeds_id
                """)
                feed_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"Created or updated Cluster AI feed with ID: {feed_id}")
                return feed_id
        finally:
            conn.close()
    
    def get_top_threats(self, hours: int = 24, limit: int = 20) -> Dict[str, Any]:
        """
        Fetch top threats (clusters and articles) from the last N hours
        
        Returns dict with 'clusters' and 'articles' lists
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        # Query for top clusters with their articles
        clusters_query = """
        WITH cluster_data AS (
            SELECT 
                c.clusters_id,
                c.clusters_name,
                c.ai_summary->>'executive_brief' as executive_brief,
                c.ai_summary->>'technical_brief' as technical_brief,
                COUNT(DISTINCT ca.cluster_articles_clean_id) as article_count,
                AVG(ar.article_rankings_score)::NUMERIC(5,2) as avg_score,
                ROUND((AVG(ar.article_rankings_score) * 0.7 + c.clusters_coherence_score * 100 * 0.3))::INTEGER as cluster_score,
                -- Get primary article details
                (SELECT rfc_primary.rss_feeds_clean_title->>'title'
                 FROM cluster_data.cluster_articles ca_primary
                 JOIN cluster_data.rss_feeds_clean rfc_primary ON ca_primary.cluster_articles_clean_id = rfc_primary.rss_feeds_clean_id
                 WHERE ca_primary.cluster_articles_cluster_id = c.clusters_id
                 ORDER BY ca_primary.cluster_articles_is_primary DESC
                 LIMIT 1
                ) as primary_article_title,
                -- Get top entities
                ARRAY(
                    SELECT entities_name FROM (
                        SELECT DISTINCT e.entities_name, MAX(e.entities_importance_weight) as max_weight
                        FROM cluster_data.cluster_articles ca2
                        JOIN cluster_data.rss_feeds_clean rfc2 ON ca2.cluster_articles_clean_id = rfc2.rss_feeds_clean_id
                        JOIN LATERAL jsonb_array_elements(rfc2.rss_feeds_clean_extracted_entities->'entities') AS ent ON true
                        JOIN cluster_data.entities e ON (ent->>'entities_id')::int = e.entities_id
                        WHERE ca2.cluster_articles_cluster_id = c.clusters_id
                            AND e.entities_importance_weight >= 70
                        GROUP BY e.entities_name
                        ORDER BY MAX(e.entities_importance_weight) DESC
                        LIMIT 10
                    ) sub
                ) as key_entities
            FROM cluster_data.clusters c
            JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            JOIN cluster_data.article_rankings ar ON ca.cluster_articles_cluster_id = ar.article_rankings_cluster_id 
                AND ca.cluster_articles_clean_id = ar.article_rankings_clean_id
            WHERE c.clusters_created_at >= %s
                AND c.clusters_is_active = TRUE
            GROUP BY c.clusters_id
            ORDER BY cluster_score DESC
            LIMIT %s
        )
        SELECT * FROM cluster_data
        """
        
        # Query for top individual articles not in clusters
        articles_query = """
        SELECT 
            rfc.rss_feeds_clean_id as article_id,
            rfc.rss_feeds_clean_title->>'title' as title,
            LEFT(rfc.rss_feeds_clean_content->>'content', 500) as content_preview,
            rf.rss_feeds_name as source,
            ar.article_rankings_score as score,
            ar.article_rankings_factors as ranking_factors,
            -- Get entities
            ARRAY(
                SELECT entities_name FROM (
                    SELECT DISTINCT e.entities_name, MAX(e.entities_importance_weight) as max_weight
                    FROM jsonb_array_elements(rfc.rss_feeds_clean_extracted_entities->'entities') AS ent
                    JOIN cluster_data.entities e ON (ent->>'entities_id')::int = e.entities_id
                    WHERE e.entities_importance_weight >= 50
                    GROUP BY e.entities_name
                    ORDER BY MAX(e.entities_importance_weight) DESC
                    LIMIT 5
                ) sub
            ) as key_entities
        FROM cluster_data.rss_feeds_clean rfc
        JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
        JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
        LEFT JOIN cluster_data.article_rankings ar ON rfc.rss_feeds_clean_id = ar.article_rankings_clean_id
        LEFT JOIN cluster_data.cluster_articles ca ON rfc.rss_feeds_clean_id = ca.cluster_articles_clean_id
        WHERE rfr.rss_feeds_raw_published_date >= %s
            AND ca.cluster_articles_id IS NULL  -- Not in a cluster
            AND ar.article_rankings_score >= 60  -- High scoring articles only
            AND rf.rss_feeds_name != 'Cluster AI'  -- Don't include previous AI briefs
        ORDER BY ar.article_rankings_score DESC
        LIMIT %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get clusters
                cur.execute(clusters_query, (cutoff_time, limit))
                clusters = cur.fetchall()
                
                # Get individual articles
                cur.execute(articles_query, (cutoff_time, limit // 2))
                articles = cur.fetchall()
                
                return {
                    'clusters': clusters,
                    'articles': articles,
                    'total_count': len(clusters) + len(articles),
                    'time_range_hours': hours
                }
        finally:
            conn.close()
    
    def build_threat_context(self, threats_data: Dict[str, Any]) -> str:
        """Build context string for GPT from threats data"""
        context_parts = []
        
        # Add clusters
        context_parts.append("=== TOP THREAT CLUSTERS ===\n")
        for i, cluster in enumerate(threats_data['clusters'][:10], 1):
            context_parts.append(f"\n{i}. {cluster['clusters_name']} (Score: {cluster['cluster_score']}, Articles: {cluster['article_count']})")
            if cluster.get('primary_article_title'):
                context_parts.append(f"   Primary Article: {cluster['primary_article_title']}")
            if cluster.get('executive_brief'):
                context_parts.append(f"   Summary: {cluster['executive_brief'][:200]}...")
            if cluster.get('key_entities'):
                context_parts.append(f"   Key Entities: {', '.join(cluster['key_entities'][:5])}")
            context_parts.append("")
        
        # Add individual articles
        if threats_data['articles']:
            context_parts.append("\n=== HIGH-PRIORITY INDIVIDUAL THREATS ===\n")
            for i, article in enumerate(threats_data['articles'][:5], 1):
                context_parts.append(f"\n{i}. {article['title']} (Score: {article['score']})")
                context_parts.append(f"   Source: {article['source']}")
                if article.get('content_preview'):
                    context_parts.append(f"   Preview: {article['content_preview'][:150]}...")
                if article.get('key_entities'):
                    context_parts.append(f"   Entities: {', '.join(article['key_entities'])}")
                context_parts.append("")
        
        return "\n".join(context_parts)
    
    async def generate_brief_content(self, threats_data: Dict[str, Any], brief_date: date) -> Dict[str, str]:
        """Generate the brief content using OpenAI"""
        context = self.build_threat_context(threats_data)
        
        system_prompt = """You are a senior cybersecurity analyst creating a daily threat intelligence brief article.
Your audience includes executives, security teams, and technical staff.
Be concise, accurate, and actionable. Focus on the most critical threats and their implications.
Write in a clear, professional tone suitable for a security news article."""
        
        user_prompt = f"""Based on the following top security threats from {brief_date}, create a comprehensive daily threat intelligence article.

{context}

Please provide a well-structured article with the following sections:

1. **Executive Summary** (200-300 words)
- High-level overview of today's threat landscape
- Most critical threats requiring immediate attention
- Business impact and risk assessment
- Key statistics (number of threats, severity distribution)

2. **Technical Analysis** (400-500 words)
- Detailed analysis of attack methods and vulnerabilities
- Specific CVEs, malware families, and threat actors
- Technical indicators and patterns observed
- Correlation between different threats
- Attack chains and TTPs (Tactics, Techniques, and Procedures)

3. **Remediation Recommendations** (300-400 words)
- Prioritized action items for security teams
- Specific patches, configurations, or mitigations needed
- Detection and monitoring recommendations
- Long-term security posture improvements
- Proactive defense strategies

4. **Key Takeaways**
- 3-5 bullet points summarizing the most important actions

The article should flow naturally with smooth transitions between sections. Use markdown formatting for headers and emphasis.
Make it engaging and informative, suitable for both technical and executive audiences."""
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=2500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating brief content: {e}")
            raise
    
    def calculate_severity_score(self, threats_data: Dict[str, Any]) -> int:
        """Calculate severity score (0-100) based on threat scores"""
        if not threats_data['clusters'] and not threats_data['articles']:
            return 50
        
        # Get average score of top threats
        scores = []
        for cluster in threats_data['clusters'][:5]:
            scores.append(cluster.get('cluster_score', 0))
        for article in threats_data['articles'][:3]:
            scores.append(article.get('score', 0))
        
        if not scores:
            return 60
        
        avg_score = sum(scores) / len(scores)
        
        # Boost score for AI briefs to ensure visibility
        return min(100, int(avg_score * 1.2))
    
    def extract_key_entities_for_brief(self, threats_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract and format entities for the brief article"""
        entity_data = {}
        
        # Collect entities from clusters with higher weight
        for cluster in threats_data['clusters']:
            for entity in cluster.get('key_entities', []):
                if entity not in entity_data:
                    entity_data[entity] = {'count': 0, 'weight': 0}
                entity_data[entity]['count'] += 2
                entity_data[entity]['weight'] = 90  # High importance for cluster entities
        
        # Collect entities from articles
        for article in threats_data['articles']:
            for entity in article.get('key_entities', []):
                if entity not in entity_data:
                    entity_data[entity] = {'count': 0, 'weight': 0}
                entity_data[entity]['count'] += 1
                entity_data[entity]['weight'] = max(entity_data[entity]['weight'], 70)
        
        # Get entity IDs and categories from database
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                entity_names = list(entity_data.keys())
                if not entity_names:
                    return []
                
                cur.execute("""
                    SELECT entities_id, entities_name, entities_category, entities_importance_weight
                    FROM cluster_data.entities
                    WHERE entities_name = ANY(%s)
                """, (entity_names,))
                
                entity_results = []
                for row in cur.fetchall():
                    entity_results.append({
                        "entity_name": row['entities_name'],
                        "entity_category": row['entities_category'],
                        "entities_id": row['entities_id'],
                        "confidence": 0.95,
                        "position": "content",
                        "extraction_method": "ai_analysis"
                    })
                
                return entity_results[:20]  # Top 20 entities
        finally:
            conn.close()
    
    async def generate_daily_brief(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Generate a daily threat brief as an article for the specified date
        
        Returns the generated brief data
        """
        if target_date is None:
            target_date = date.today()
        
        # Check if brief already exists
        if self.check_existing_brief(target_date):
            logger.info(f"Daily brief already exists for {target_date}")
            return {"status": "exists", "date": str(target_date)}
        
        logger.info(f"Generating daily threat brief for {target_date}")
        
        # Get or create the Cluster AI feed
        feed_id = self.get_or_create_ai_feed()
        
        # Get top threats
        threats_data = self.get_top_threats(hours=24)
        
        if threats_data['total_count'] == 0:
            logger.warning(f"No threats found for {target_date}")
            return {"status": "no_data", "date": str(target_date)}
        
        # Generate AI content
        article_content = await self.generate_brief_content(threats_data, target_date)
        
        # Calculate metadata
        severity_score = self.calculate_severity_score(threats_data)
        entities = self.extract_key_entities_for_brief(threats_data)
        
        # Prepare article data
        title = f"{target_date} - Cluster AI Daily Threat Brief"
        
        # Create a summary from the executive summary section
        summary_start = article_content.find("**Executive Summary**")
        summary_end = article_content.find("**Technical Analysis**")
        if summary_start != -1 and summary_end != -1:
            summary = article_content[summary_start:summary_end].replace("**Executive Summary**", "").strip()[:500]
        else:
            summary = f"Daily threat intelligence brief analyzing {len(threats_data['clusters'])} threat clusters and {len(threats_data['articles'])} high-priority articles from the last 24 hours."
        
        # Insert into database as a regular article
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # First, insert with a placeholder URL
                # Insert raw feed entry
                cur.execute("""
                    INSERT INTO cluster_data.rss_feeds_raw (
                        rss_feeds_raw_feed_id,
                        rss_feeds_raw_xml,
                        rss_feeds_raw_published_date,
                        rss_feeds_raw_created_at
                    ) VALUES (
                        %s, %s, %s, CURRENT_TIMESTAMP
                    ) RETURNING rss_feeds_raw_id
                """, (
                    feed_id,
                    Json({
                        "title": title,
                        "link": "placeholder",  # Will update after we get clean_id
                        "description": summary,
                        "published": target_date.isoformat(),
                        "author": "Cluster AI",
                        "category": "Threat Intelligence",
                        "is_ai_generated": True,
                        "brief_metadata": {
                            "threats_analyzed": threats_data['total_count'],
                            "clusters": len(threats_data['clusters']),
                            "articles": len(threats_data['articles'])
                        }
                    }),
                    datetime.combine(target_date, datetime.min.time())
                ))
                raw_id = cur.fetchone()['rss_feeds_raw_id']
                
                # Insert clean article
                cur.execute("""
                    INSERT INTO cluster_data.rss_feeds_clean (
                        rss_feeds_clean_raw_id,
                        rss_feeds_clean_title,
                        rss_feeds_clean_content,
                        rss_feeds_clean_extracted_entities,
                        rss_feeds_clean_processed,
                        rss_feeds_clean_created_at
                    ) VALUES (
                        %s, %s, %s, %s, true, CURRENT_TIMESTAMP
                    ) RETURNING rss_feeds_clean_id
                """, (
                    raw_id,
                    Json({"title": title}),
                    Json({"content": article_content}),
                    Json({
                        "entities": entities,
                        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                        "entity_count": len(entities),
                        "categories": list(set(e['entity_category'] for e in entities)),
                        "no_clustering": True,  # Flag to prevent clustering
                        "article_type": "ai_daily_brief"
                    })
                ))
                clean_id = cur.fetchone()['rss_feeds_clean_id']
                
                # Update the raw entry with the correct article URL
                article_url = f"https://threatcluster.io/articles/{clean_id}"
                cur.execute("""
                    UPDATE cluster_data.rss_feeds_raw 
                    SET rss_feeds_raw_xml = jsonb_set(
                        rss_feeds_raw_xml, 
                        '{link}', 
                        %s::jsonb
                    )
                    WHERE rss_feeds_raw_id = %s
                """, (json.dumps(article_url), raw_id))
                
                # Insert article ranking with high score
                cur.execute("""
                    INSERT INTO cluster_data.article_rankings (
                        article_rankings_clean_id,
                        article_rankings_score,
                        article_rankings_factors,
                        article_rankings_ranked_at
                    ) VALUES (
                        %s, %s, %s, CURRENT_TIMESTAMP
                    )
                """, (
                    clean_id,
                    severity_score,
                    Json({
                        "recency_score": 100,  # Always fresh
                        "source_credibility_score": 100,  # Maximum credibility
                        "entity_importance_score": 80,
                        "keyword_severity_score": severity_score,
                        "is_ai_generated": True,
                        "brief_type": "daily"
                    })
                ))
                
                conn.commit()
                
                logger.info(f"Successfully generated daily brief article {clean_id} for {target_date}")
                return {
                    "status": "success",
                    "article_id": clean_id,
                    "date": str(target_date),
                    "severity_score": severity_score,
                    "threats_analyzed": threats_data['total_count']
                }
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving daily brief: {e}")
            raise
        finally:
            conn.close()


async def main():
    """Test the daily brief generation"""
    service = DailyBriefService()
    result = await service.generate_daily_brief()
    print(f"Generated brief: {result}")


if __name__ == "__main__":
    asyncio.run(main())