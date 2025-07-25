"""
AI Summary Service - Generates AI-powered summaries for security clusters using OpenAI
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
import asyncio
from openai import AsyncOpenAI
import psycopg2
from psycopg2.extras import RealDictCursor

from src.config.settings import settings
from .prompts import build_cluster_prompt

logger = logging.getLogger(__name__)


class AISummaryService:
    """Service for generating AI summaries of security clusters using OpenAI GPT-4-mini"""
    
    def __init__(self):
        """Initialize the AI Summary Service"""
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
        self.model = "gpt-4o-mini"  # Using GPT-4-mini as specified
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    async def get_clusters_needing_regeneration(self, limit: int = 10) -> List[Dict]:
        """
        Fetch clusters that need AI summary regeneration (new or updated).
        
        Args:
            limit: Maximum number of clusters to fetch
            
        Returns:
            List of cluster dictionaries
        """
        query = """
        SELECT 
            cnr.clusters_id,
            c.clusters_name,
            c.clusters_created_at,
            cnr.regeneration_reason,
            cnr.current_article_count as article_count
        FROM cluster_data.clusters_needing_ai_regeneration cnr
        JOIN cluster_data.clusters c ON cnr.clusters_id = c.clusters_id
        ORDER BY 
            CASE cnr.regeneration_reason
                WHEN 'never_generated' THEN 1
                WHEN 'content_updated' THEN 2
                WHEN 'article_count_mismatch' THEN 3
                ELSE 4
            END,
            c.clusters_created_at ASC
        LIMIT %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (limit,))
                clusters = cur.fetchall()
                
            logger.info(f"Found {len(clusters)} clusters needing AI regeneration")
            for cluster in clusters:
                logger.info(f"  - Cluster {cluster['clusters_id']}: {cluster['regeneration_reason']}")
            return clusters
        finally:
            conn.close()
    
    async def get_clusters_without_summaries(self, limit: int = 10) -> List[Dict]:
        """
        Fetch clusters that don't have AI summaries yet.
        
        Args:
            limit: Maximum number of clusters to fetch
            
        Returns:
            List of cluster dictionaries
        """
        query = """
        SELECT 
            c.clusters_id,
            c.clusters_name,
            c.clusters_summary,
            c.clusters_created_at,
            COUNT(ca.cluster_articles_clean_id) as article_count
        FROM cluster_data.clusters c
        LEFT JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
        WHERE c.has_ai_summary = FALSE 
            AND c.clusters_is_active = TRUE
        GROUP BY c.clusters_id
        ORDER BY c.clusters_created_at ASC
        LIMIT %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (limit,))
                clusters = cur.fetchall()
                
            logger.info(f"Found {len(clusters)} clusters without AI summaries")
            return clusters
        finally:
            conn.close()
    
    def get_cluster_articles(self, cluster_id: int, max_articles: int = 10) -> List[Dict]:
        """
        Fetch articles for a specific cluster.
        
        Args:
            cluster_id: The cluster ID
            max_articles: Maximum number of articles to fetch
            
        Returns:
            List of article dictionaries
        """
        query = """
        SELECT 
            rfc.rss_feeds_clean_id,
            rfc.rss_feeds_clean_title->>'title' as title,
            rfc.rss_feeds_clean_content->>'content' as content,
            ca.cluster_articles_is_primary as is_primary_article,
            ca.cluster_articles_similarity_score as similarity_score,
            rf.rss_feeds_name as source,
            TO_CHAR(rfr.rss_feeds_raw_published_date, 'YYYY-MM-DD HH24:MI') as published_date
        FROM cluster_data.cluster_articles ca
        JOIN cluster_data.rss_feeds_clean rfc 
            ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
        JOIN cluster_data.rss_feeds_raw rfr
            ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
        JOIN cluster_data.rss_feeds rf
            ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
        WHERE ca.cluster_articles_cluster_id = %s
        ORDER BY ca.cluster_articles_is_primary DESC, ca.cluster_articles_similarity_score DESC
        LIMIT %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (cluster_id, max_articles))
                articles = cur.fetchall()
                
            return articles
        finally:
            conn.close()
    
    async def generate_ai_summary(self, articles: List[Dict]) -> Optional[Dict]:
        """
        Generate AI summary using OpenAI GPT-4-mini.
        
        Args:
            articles: List of article dictionaries
            
        Returns:
            Dictionary with executive_brief, technical_brief, and remediation_brief
        """
        if not articles:
            logger.warning("No articles provided for summarization")
            return None
        
        # Build the prompt
        prompt = build_cluster_prompt(articles)
        
        # Try to generate summary with retries
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Generating AI summary (attempt {attempt + 1}/{self.max_retries})")
                
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert cyber security threat intelligence analyst. Follow the exact format provided, focusing on actionable intelligence, technical accuracy, and balanced perspectives. Always synthesize information from ALL articles in the cluster."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,  # Lower temperature for more consistent output
                    max_tokens=1500,  # Reduced for concise summaries
                    response_format={"type": "json_object"}  # Ensure JSON response
                )
                
                # Parse the response
                content = response.choices[0].message.content
                summary_data = json.loads(content)
                
                # Validate the response structure
                required_keys = ['key_insights', 'summary', 'ttps', 'sources']
                if all(key in summary_data for key in required_keys):
                    # Validate key_insights is a list
                    if not isinstance(summary_data['key_insights'], list):
                        logger.error("key_insights is not a list")
                        continue
                    
                    # Validate ttps is a list
                    if not isinstance(summary_data['ttps'], list):
                        logger.error("ttps is not a list")
                        continue
                    
                    # Validate sources is a dict
                    if not isinstance(summary_data['sources'], dict):
                        logger.error("sources is not a dict")
                        continue
                    
                    # Ensure summary is within reasonable limits (150 words ~= 900 chars)
                    if len(summary_data['summary']) > 1000:
                        summary_data['summary'] = summary_data['summary'][:997] + "..."
                    
                    # Limit key insights to 5 items
                    if len(summary_data['key_insights']) > 5:
                        summary_data['key_insights'] = summary_data['key_insights'][:5]
                    
                    # Limit TTPs to 6 items
                    if len(summary_data['ttps']) > 6:
                        summary_data['ttps'] = summary_data['ttps'][:6]
                    
                    logger.info("Successfully generated AI summary")
                    return summary_data
                else:
                    logger.error(f"Invalid response structure: missing required keys. Got keys: {list(summary_data.keys())}")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse OpenAI response as JSON: {e}")
            except Exception as e:
                logger.error(f"Error generating AI summary: {e}")
                
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay)
        
        logger.error("Failed to generate AI summary after all retries")
        return None
    
    def save_ai_summary(self, cluster_id: int, ai_summary: Dict) -> bool:
        """
        Save the AI summary to the database.
        
        Args:
            cluster_id: The cluster ID
            ai_summary: Dictionary containing the briefs
            
        Returns:
            True if successful, False otherwise
        """
        query = """
        UPDATE cluster_data.clusters
        SET ai_summary = %s,
            has_ai_summary = TRUE,
            ai_summary_generated_at = CURRENT_TIMESTAMP
        WHERE clusters_id = %s
        """
        
        conn = psycopg2.connect(settings.database_url)
        try:
            with conn.cursor() as cur:
                cur.execute(query, (json.dumps(ai_summary), cluster_id))
                conn.commit()
                
            logger.info(f"Saved AI summary for cluster {cluster_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving AI summary for cluster {cluster_id}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            conn.close()
    
    async def process_cluster(self, cluster: Dict) -> bool:
        """
        Process a single cluster to generate and save AI summary.
        
        Args:
            cluster: Cluster dictionary
            
        Returns:
            True if successful, False otherwise
        """
        cluster_id = cluster['clusters_id']
        logger.info(f"Processing cluster {cluster_id}: {cluster['clusters_name']}")
        
        # Get articles for the cluster
        articles = self.get_cluster_articles(cluster_id)
        if not articles:
            logger.warning(f"No articles found for cluster {cluster_id}")
            return False
        
        logger.info(f"Found {len(articles)} articles for cluster {cluster_id}")
        
        # Generate AI summary
        ai_summary = await self.generate_ai_summary(articles)
        if not ai_summary:
            logger.error(f"Failed to generate AI summary for cluster {cluster_id}")
            return False
        
        # Save to database
        success = self.save_ai_summary(cluster_id, ai_summary)
        return success
    
    async def process_clusters_batch(self, limit: int = 10, include_updates: bool = True) -> Dict[str, Any]:
        """
        Process a batch of clusters that need AI summaries (new or updated).
        
        Args:
            limit: Maximum number of clusters to process
            include_updates: Whether to include clusters that need regeneration
            
        Returns:
            Dictionary with processing results
        """
        start_time = datetime.now()
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'regenerated': 0,
            'new': 0,
            'clusters': []
        }
        
        # Get clusters to process (either just new or including updates)
        if include_updates:
            clusters = await self.get_clusters_needing_regeneration(limit)
        else:
            clusters = await self.get_clusters_without_summaries(limit)
            
        if not clusters:
            logger.info("No clusters found that need AI summaries")
            # Calculate processing time even when no clusters found
            processing_time = (datetime.now() - start_time).total_seconds()
            results['processing_time_seconds'] = processing_time
            return results
        
        # Process each cluster
        for cluster in clusters:
            try:
                success = await self.process_cluster(cluster)
                results['processed'] += 1
                
                if success:
                    results['successful'] += 1
                    status = 'success'
                    # Track if this was a regeneration or new generation
                    if include_updates and 'regeneration_reason' in cluster:
                        if cluster['regeneration_reason'] != 'never_generated':
                            results['regenerated'] += 1
                        else:
                            results['new'] += 1
                else:
                    results['failed'] += 1
                    status = 'failed'
                
                results['clusters'].append({
                    'cluster_id': cluster['clusters_id'],
                    'cluster_name': cluster['clusters_name'],
                    'status': status,
                    'reason': cluster.get('regeneration_reason', 'new')
                })
                
                # Add delay between API calls to avoid rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing cluster {cluster['clusters_id']}: {e}")
                results['failed'] += 1
                results['clusters'].append({
                    'cluster_id': cluster['clusters_id'],
                    'cluster_name': cluster['clusters_name'],
                    'status': 'error',
                    'error': str(e)
                })
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        results['processing_time_seconds'] = processing_time
        
        logger.info(f"Batch processing complete: {results['successful']} successful, "
                   f"{results['failed']} failed, {processing_time:.2f} seconds")
        
        return results


async def test_ai_summary_service():
    """Test the AI summary service with a single cluster"""
    service = AISummaryService()
    
    # Process one cluster as a test
    results = await service.process_clusters_batch(limit=1)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    # Test the service
    asyncio.run(test_ai_summary_service())