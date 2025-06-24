#!/usr/bin/env python3
"""
Article Ranking Module

Calculates comprehensive ranking scores for articles and clusters using multiple
factors including recency, source credibility, entity importance, and keyword severity.
"""

import psycopg2
import psycopg2.extras
import json
import yaml
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict, Counter
import structlog
import re

from src.config.settings import settings


# Configure structured logging
logger = structlog.get_logger(__name__)


class ArticleRanker:
    """Ranks articles and clusters based on multiple importance signals."""
    
    def __init__(self):
        """Initialize the article ranker with configuration."""
        self.config = settings.app_config
        self.ranking_config = self.config.get('ranking', {})
        
        # Load ranking weights
        weights = self.ranking_config.get('weights', {})
        self.weight_recency = weights.get('recency', 0.20)
        self.weight_source = weights.get('source_credibility', 0.30)
        self.weight_entity = weights.get('entity_importance', 0.30)
        self.weight_keyword = weights.get('keyword_severity', 0.20)
        
        # Validate weights sum to 1.0
        weight_sum = self.weight_recency + self.weight_source + self.weight_entity + self.weight_keyword
        if abs(weight_sum - 1.0) > 0.01:
            logger.warning("ranking_weights_invalid", 
                         sum=weight_sum,
                         weights=weights)
            # Normalize weights
            self.weight_recency /= weight_sum
            self.weight_source /= weight_sum
            self.weight_entity /= weight_sum
            self.weight_keyword /= weight_sum
        
        # Recency parameters
        self.recency_decay_hours = self.ranking_config.get('recency_decay_hours', 24)
        
        # Load keyword weights
        self._load_keyword_weights()
        
        # Cache for entity weights
        self.entity_weights_cache = {}
        self.cache_timestamp = None
        self.cache_duration = timedelta(hours=1)
        
        # Statistics
        self.stats = {
            'articles_ranked': 0,
            'clusters_ranked': 0,
            'average_score': 0,
            'component_averages': {
                'recency': 0,
                'source': 0,
                'entity': 0,
                'keyword': 0
            }
        }
    
    def _load_keyword_weights(self):
        """Load keyword weights from keywords.yaml."""
        keywords_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
            'config', 'keywords.yaml'
        )
        
        try:
            with open(keywords_path, 'r') as f:
                keywords_config = yaml.safe_load(f)
            
            # Flatten all keyword categories into one dict
            self.keyword_weights = {}
            
            for category, keywords in keywords_config.items():
                if isinstance(keywords, dict):
                    for keyword, weight in keywords.items():
                        # Store both original and normalized versions
                        self.keyword_weights[keyword.lower()] = weight
                        self.keyword_weights[keyword.replace('_', ' ').lower()] = weight
                        self.keyword_weights[keyword.replace('-', ' ').lower()] = weight
            
            logger.info("loaded_keyword_weights", count=len(self.keyword_weights))
            
        except Exception as e:
            logger.error("keyword_weights_load_error", error=str(e))
            self.keyword_weights = {}
    
    def _load_entity_weights(self):
        """Load entity importance weights from database."""
        # Check cache
        if (self.entity_weights_cache and 
            self.cache_timestamp and 
            datetime.now(timezone.utc) - self.cache_timestamp < self.cache_duration):
            return
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT entities_name, entities_category, entities_importance_weight
                FROM cluster_data.entities
            """)
            
            self.entity_weights_cache = {}
            for row in cursor:
                key = f"{row[1]}:{row[0]}"  # category:name
                self.entity_weights_cache[key] = row[2]
            
            self.cache_timestamp = datetime.now(timezone.utc)
            logger.info("loaded_entity_weights", count=len(self.entity_weights_cache))
            
        finally:
            cursor.close()
            conn.close()
    
    def calculate_recency_score(self, published_date: datetime) -> float:
        """Calculate recency score based on time elapsed since publication."""
        if not published_date:
            return 0.0
        
        # Ensure timezone awareness
        if published_date.tzinfo is None:
            published_date = published_date.replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        hours_elapsed = (now - published_date).total_seconds() / 3600
        
        # Linear decay over configured hours
        if hours_elapsed <= 0:
            return 100.0
        elif hours_elapsed >= self.recency_decay_hours:
            return 0.0
        else:
            # Linear decay from 100 to 0 over decay period
            score = 100 * (1 - hours_elapsed / self.recency_decay_hours)
            return max(0, min(100, score))
    
    def calculate_source_credibility_score(self, source_credibility: Optional[int]) -> float:
        """Get source credibility score."""
        if source_credibility is None:
            return 50.0  # Default credibility
        
        return float(min(100, max(0, source_credibility)))
    
    def calculate_entity_importance_score(self, entities_data: Optional[Dict]) -> Tuple[float, Dict]:
        """Calculate entity importance score and return contributing entities."""
        if not entities_data or 'entities' not in entities_data:
            return 0.0, {}
        
        # Load entity weights if needed
        self._load_entity_weights()
        
        entities = entities_data.get('entities', [])
        if not entities:
            return 0.0, {}
        
        # Calculate weighted average
        total_weight = 0
        weight_sum = 0
        high_importance_count = 0
        contributing_entities = {}
        
        for entity in entities:
            entity_name = entity.get('entity_name', '')
            entity_category = entity.get('entity_category', '')
            
            # Look up weight
            key = f"{entity_category}:{entity_name}"
            weight = self.entity_weights_cache.get(key, 50)  # Default weight 50
            
            total_weight += weight
            weight_sum += 1
            
            if weight >= 80:
                high_importance_count += 1
            
            # Track contributing entities
            if weight >= 70:  # Only track significant entities
                contributing_entities[entity_name] = {
                    'category': entity_category,
                    'weight': weight
                }
        
        if weight_sum == 0:
            return 0.0, {}
        
        # Base score is weighted average
        base_score = total_weight / weight_sum
        
        # Apply bonus for multiple high-importance entities
        if high_importance_count >= 3:
            bonus = min(20, high_importance_count * 5)
            base_score = min(100, base_score + bonus)
        
        return base_score, contributing_entities
    
    def calculate_keyword_severity_score(self, title: str, content: str) -> Tuple[float, List[str]]:
        """Calculate keyword severity score and return matching keywords."""
        if not title and not content:
            return 0.0, []
        
        # Normalize text
        title_lower = title.lower() if title else ""
        content_lower = content.lower() if content else ""
        
        # Track keyword matches and their weights
        keyword_matches = {}
        
        # Check each keyword
        for keyword, weight in self.keyword_weights.items():
            # Check title (double weight)
            if keyword in title_lower:
                keyword_matches[keyword] = weight * 2
            # Check content (standard weight)
            elif keyword in content_lower:
                keyword_matches[keyword] = weight
        
        if not keyword_matches:
            return 0.0, []
        
        # Calculate weighted score
        # Use top 5 keywords to avoid over-weighting
        top_keywords = sorted(keyword_matches.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Average of top keyword weights, normalized to 0-100
        total_weight = sum(weight for _, weight in top_keywords)
        max_possible = 200 * len(top_keywords)  # Max weight (100) * 2 for title match
        
        score = (total_weight / max_possible) * 100 if max_possible > 0 else 0
        
        # Extract keyword names
        matching_keywords = [kw for kw, _ in top_keywords]
        
        return min(100, score), matching_keywords
    
    def calculate_article_score(self, article: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
        """Calculate comprehensive ranking score for an article."""
        # Extract article data
        published_date = article.get('rss_feeds_raw_published_date')
        source_credibility = article.get('rss_feeds_credibility')
        entities_data = article.get('rss_feeds_clean_extracted_entities')
        
        # Get title and content
        title_data = article.get('rss_feeds_clean_title', {})
        content_data = article.get('rss_feeds_clean_content', {})
        
        title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
        content = content_data.get('content', '') if isinstance(content_data, dict) else str(content_data)
        
        # Calculate component scores
        recency_score = self.calculate_recency_score(published_date)
        source_score = self.calculate_source_credibility_score(source_credibility)
        entity_score, contributing_entities = self.calculate_entity_importance_score(entities_data)
        keyword_score, matching_keywords = self.calculate_keyword_severity_score(title, content)
        
        # Calculate final weighted score
        final_score = (
            recency_score * self.weight_recency +
            source_score * self.weight_source +
            entity_score * self.weight_entity +
            keyword_score * self.weight_keyword
        )
        
        # Cap at 100
        final_score = min(100, max(0, final_score))
        
        # Build ranking factors
        ranking_factors = {
            'recency_score': round(recency_score, 2),
            'source_credibility': round(source_score, 2),
            'entity_importance': round(entity_score, 2),
            'keyword_severity': round(keyword_score, 2),
            'entity_count': len(entities_data.get('entities', [])) if entities_data else 0,
            'keyword_matches': matching_keywords,
            'contributing_entities': contributing_entities,
            'calculation_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        return round(final_score, 1), ranking_factors
    
    def calculate_cluster_score(self, cluster_data: Dict[str, Any], 
                              article_scores: List[Tuple[int, float]]) -> float:
        """Calculate cluster-level ranking score."""
        if not article_scores:
            return 0.0
        
        # Get cluster coherence
        coherence = cluster_data.get('clusters_coherence_score', 0.7)
        
        # Calculate average article score
        avg_article_score = sum(score for _, score in article_scores) / len(article_scores)
        
        # Weight by coherence
        cluster_score = (avg_article_score * 0.70) + (coherence * 100 * 0.30)
        
        # Apply size bonus for significant clusters
        if len(article_scores) >= 5:
            size_bonus = min(10, len(article_scores))
            cluster_score = min(100, cluster_score + size_bonus)
        
        return round(cluster_score, 1)
    
    def get_articles_to_rank(self, time_window_hours: Optional[int] = None,
                           process_all: bool = False) -> List[Dict[str, Any]]:
        """Fetch articles that need ranking."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            if process_all:
                # Process all articles without rankings
                query = """
                    SELECT 
                        rfc.rss_feeds_clean_id,
                        rfc.rss_feeds_clean_title,
                        rfc.rss_feeds_clean_content,
                        rfc.rss_feeds_clean_extracted_entities,
                        rfr.rss_feeds_raw_published_date,
                        rf.rss_feeds_credibility,
                        ca.cluster_articles_cluster_id
                    FROM cluster_data.rss_feeds_clean rfc
                    JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                    JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                    LEFT JOIN cluster_data.cluster_articles ca ON rfc.rss_feeds_clean_id = ca.cluster_articles_clean_id
                    LEFT JOIN cluster_data.article_rankings ar ON rfc.rss_feeds_clean_id = ar.article_rankings_clean_id
                    WHERE ar.article_rankings_id IS NULL
                    ORDER BY rfr.rss_feeds_raw_published_date DESC
                """
                cursor.execute(query)
            else:
                # Process articles from time window
                if time_window_hours is None:
                    time_window_hours = 72  # Default 3 days
                
                cutoff_date = datetime.now(timezone.utc) - timedelta(hours=time_window_hours)
                
                query = """
                    SELECT 
                        rfc.rss_feeds_clean_id,
                        rfc.rss_feeds_clean_title,
                        rfc.rss_feeds_clean_content,
                        rfc.rss_feeds_clean_extracted_entities,
                        rfr.rss_feeds_raw_published_date,
                        rf.rss_feeds_credibility,
                        ca.cluster_articles_cluster_id
                    FROM cluster_data.rss_feeds_clean rfc
                    JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                    JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                    LEFT JOIN cluster_data.cluster_articles ca ON rfc.rss_feeds_clean_id = ca.cluster_articles_clean_id
                    WHERE rfr.rss_feeds_raw_published_date >= %s
                    ORDER BY rfr.rss_feeds_raw_published_date DESC
                """
                cursor.execute(query, (cutoff_date,))
            
            articles = [dict(row) for row in cursor.fetchall()]
            logger.info("fetched_articles_to_rank", count=len(articles))
            
            return articles
            
        finally:
            cursor.close()
            conn.close()
    
    def store_article_ranking(self, article_id: int, cluster_id: Optional[int],
                            score: float, factors: Dict[str, Any]):
        """Store or update article ranking in database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            # Delete existing ranking if any
            cursor.execute("""
                DELETE FROM cluster_data.article_rankings
                WHERE article_rankings_clean_id = %s
            """, (article_id,))
            
            # Insert new ranking
            cursor.execute("""
                INSERT INTO cluster_data.article_rankings
                (article_rankings_clean_id, article_rankings_cluster_id,
                 article_rankings_score, article_rankings_factors)
                VALUES (%s, %s, %s, %s)
            """, (article_id, cluster_id, int(score), json.dumps(factors)))
            
            conn.commit()
            
        except Exception as e:
            logger.error("ranking_storage_error", 
                       article_id=article_id,
                       error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    
    def rank_articles(self, batch_size: int = 100,
                     time_window_hours: Optional[int] = None,
                     process_all: bool = False) -> Dict[str, Any]:
        """Rank articles and clusters in batches."""
        logger.info("starting_article_ranking",
                   batch_size=batch_size,
                   time_window_hours=time_window_hours,
                   process_all=process_all)
        
        # Get articles to rank
        articles = self.get_articles_to_rank(time_window_hours, process_all)
        
        if not articles:
            logger.info("no_articles_to_rank")
            return self.stats
        
        # Track scores for statistics
        all_scores = []
        component_scores = {
            'recency': [],
            'source': [],
            'entity': [],
            'keyword': []
        }
        
        # Track cluster scores
        cluster_articles = defaultdict(list)  # cluster_id -> [(article_id, score)]
        
        # Process in batches
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            logger.info("processing_batch", 
                       batch_num=i//batch_size + 1,
                       batch_size=len(batch))
            
            for article in batch:
                try:
                    # Calculate article score
                    score, factors = self.calculate_article_score(article)
                    
                    # Store ranking
                    self.store_article_ranking(
                        article['rss_feeds_clean_id'],
                        article.get('cluster_articles_cluster_id'),
                        score,
                        factors
                    )
                    
                    # Track statistics
                    all_scores.append(score)
                    component_scores['recency'].append(factors['recency_score'])
                    component_scores['source'].append(factors['source_credibility'])
                    component_scores['entity'].append(factors['entity_importance'])
                    component_scores['keyword'].append(factors['keyword_severity'])
                    
                    # Track for cluster scoring
                    cluster_id = article.get('cluster_articles_cluster_id')
                    if cluster_id:
                        cluster_articles[cluster_id].append(
                            (article['rss_feeds_clean_id'], score)
                        )
                    
                    self.stats['articles_ranked'] += 1
                    
                except Exception as e:
                    logger.error("article_ranking_error",
                               article_id=article.get('rss_feeds_clean_id'),
                               error=str(e))
        
        # Track cluster statistics (but don't store scores)
        if cluster_articles:
            logger.info("cluster_statistics", 
                       cluster_count=len(cluster_articles))
            self.stats['clusters_ranked'] = len(cluster_articles)
        
        # Calculate statistics
        if all_scores:
            self.stats['average_score'] = sum(all_scores) / len(all_scores)
            
            for component in component_scores:
                if component_scores[component]:
                    avg = sum(component_scores[component]) / len(component_scores[component])
                    self.stats['component_averages'][component] = round(avg, 2)
        
        logger.info("ranking_completed", **self.stats)
        
        return self.stats
    
    def get_ranking_distribution(self) -> Dict[str, Any]:
        """Analyze ranking score distribution."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    AVG(article_rankings_score) as avg_score,
                    MIN(article_rankings_score) as min_score,
                    MAX(article_rankings_score) as max_score,
                    STDDEV(article_rankings_score) as stddev_score,
                    COUNT(CASE WHEN article_rankings_score >= 90 THEN 1 END) as critical_count,
                    COUNT(CASE WHEN article_rankings_score >= 70 AND article_rankings_score < 90 THEN 1 END) as high_count,
                    COUNT(CASE WHEN article_rankings_score >= 50 AND article_rankings_score < 70 THEN 1 END) as medium_count,
                    COUNT(CASE WHEN article_rankings_score < 50 THEN 1 END) as low_count
                FROM cluster_data.article_rankings
            """)
            
            result = cursor.fetchone()
            
            return {
                'total_articles': result[0] or 0,
                'average_score': round(result[1] or 0, 2),
                'min_score': result[2] or 0,
                'max_score': result[3] or 0,
                'std_deviation': round(result[4] or 0, 2),
                'distribution': {
                    'critical': result[5] or 0,
                    'high': result[6] or 0,
                    'medium': result[7] or 0,
                    'low': result[8] or 0
                }
            }
            
        finally:
            cursor.close()
            conn.close()


def main():
    """Run article ranking as standalone script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Rank articles based on multiple importance signals')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of articles to process per batch')
    parser.add_argument('--time-window', type=int,
                       help='Time window in hours (default: from config)')
    parser.add_argument('--all', action='store_true',
                       help='Process all unranked articles')
    
    args = parser.parse_args()
    
    ranker = ArticleRanker()
    
    print("Starting article ranking...")
    stats = ranker.rank_articles(
        batch_size=args.batch_size,
        time_window_hours=args.time_window,
        process_all=args.all
    )
    
    print(f"\nRanking completed:")
    print(f"  Articles ranked: {stats['articles_ranked']}")
    print(f"  Clusters ranked: {stats['clusters_ranked']}")
    print(f"  Average score: {stats['average_score']:.2f}")
    print(f"\nComponent averages:")
    for component, avg in stats['component_averages'].items():
        print(f"  {component}: {avg}")
    
    # Show distribution
    print("\nScore distribution:")
    dist = ranker.get_ranking_distribution()
    print(f"  Critical (90+): {dist['distribution']['critical']}")
    print(f"  High (70-89): {dist['distribution']['high']}")
    print(f"  Medium (50-69): {dist['distribution']['medium']}")
    print(f"  Low (<50): {dist['distribution']['low']}")


if __name__ == "__main__":
    main()