#!/usr/bin/env python3
"""
Cluster Manager Module

Handles cluster deduplication, naming, storage, and management. Detects duplicate
clusters, generates meaningful names from entities and keywords, and manages
database operations for cluster storage.
"""
import psycopg2
import psycopg2.extras
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict, Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import structlog

from src.config.settings import settings


# Configure structured logging
logger = structlog.get_logger(__name__)


class ClusterManager:
    """Manages cluster storage, deduplication, and naming."""
    
    def __init__(self):
        """Initialize the cluster manager."""
        self.config = settings.app_config
        self.clustering_config = self.config.get('clustering', {})
        
        # Duplicate detection threshold
        self.duplicate_threshold = 0.75
        
        # Cache for existing clusters
        self.existing_clusters_cache = None
        self.cache_expiry = None
        self.cache_duration_hours = 1
        
        # TF-IDF vectorizer for title similarity
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=100,
            stop_words='english',
            ngram_range=(1, 2)
        )
    
    def get_existing_clusters(self, time_window_days: int = 14) -> List[Dict[str, Any]]:
        """Fetch existing active clusters from extended time window."""
        # Check cache first
        if (self.existing_clusters_cache is not None and 
            self.cache_expiry and datetime.now(timezone.utc) < self.cache_expiry):
            return self.existing_clusters_cache
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=time_window_days)
            
            query = """
                SELECT 
                    c.clusters_id,
                    c.clusters_name,
                    c.clusters_summary,
                    c.clusters_coherence_score,
                    c.clusters_created_at,
                    array_agg(ca.cluster_articles_clean_id) as article_ids,
                    array_agg(rfc.rss_feeds_clean_title) as article_titles,
                    array_agg(rfc.rss_feeds_clean_extracted_entities) as article_entities,
                    array_agg(rf.rss_feeds_id) as feed_ids
                FROM cluster_data.clusters c
                JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                WHERE c.clusters_is_active = true
                AND c.clusters_created_at >= %s
                GROUP BY c.clusters_id
            """
            cursor.execute(query, (cutoff_date,))
            
            clusters = []
            for row in cursor:
                cluster_data = dict(row)
                # Parse entities from JSONB
                parsed_entities = []
                for entity_data in cluster_data['article_entities']:
                    if entity_data and 'entities' in entity_data:
                        parsed_entities.extend(entity_data['entities'])
                cluster_data['parsed_entities'] = parsed_entities
                clusters.append(cluster_data)
            
            # Update cache
            self.existing_clusters_cache = clusters
            self.cache_expiry = datetime.now(timezone.utc) + timedelta(hours=self.cache_duration_hours)
            
            logger.info("loaded_existing_clusters", count=len(clusters))
            return clusters
            
        finally:
            cursor.close()
            conn.close()
    
    def calculate_article_overlap(self, cluster1_articles: Set[int], 
                                cluster2_articles: Set[int]) -> float:
        """Calculate Jaccard similarity of article sets."""
        if not cluster1_articles or not cluster2_articles:
            return 0.0
        
        intersection = len(cluster1_articles & cluster2_articles)
        union = len(cluster1_articles | cluster2_articles)
        
        return intersection / union if union > 0 else 0.0
    
    def calculate_entity_similarity(self, entities1: List[Dict], 
                                  entities2: List[Dict]) -> float:
        """Calculate entity similarity between two sets of entities."""
        if not entities1 or not entities2:
            return 0.0
        
        # Extract entity names and categories
        entity_set1 = set()
        entity_set2 = set()
        
        for entity in entities1:
            key = f"{entity.get('entity_category', '')}:{entity.get('entity_name', '')}"
            entity_set1.add(key)
        
        for entity in entities2:
            key = f"{entity.get('entity_category', '')}:{entity.get('entity_name', '')}"
            entity_set2.add(key)
        
        # Calculate Jaccard similarity
        if not entity_set1 or not entity_set2:
            return 0.0
        
        intersection = len(entity_set1 & entity_set2)
        union = len(entity_set1 | entity_set2)
        
        return intersection / union if union > 0 else 0.0
    
    def calculate_title_similarity(self, titles1: List[str], titles2: List[str]) -> float:
        """Calculate TF-IDF similarity between title sets."""
        if not titles1 or not titles2:
            return 0.0
        
        # Combine titles for each cluster
        text1 = ' '.join(titles1)
        text2 = ' '.join(titles2)
        
        try:
            # Fit and transform
            tfidf_matrix = self.tfidf_vectorizer.fit_transform([text1, text2])
            
            # Calculate cosine similarity
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            return float(similarity)
        except Exception as e:
            logger.warning("title_similarity_error", error=str(e))
            return 0.0
    
    def calculate_source_overlap(self, feeds1: List[int], feeds2: List[int]) -> float:
        """Calculate overlap in news sources."""
        if not feeds1 or not feeds2:
            return 0.0
        
        feed_set1 = set(feeds1)
        feed_set2 = set(feeds2)
        
        intersection = len(feed_set1 & feed_set2)
        union = len(feed_set1 | feed_set2)
        
        return intersection / union if union > 0 else 0.0
    
    def check_key_entity_match(self, entities1: List[Dict], entities2: List[Dict]) -> float:
        """Check for matches in key entities (companies, threat actors, malware)."""
        if not entities1 or not entities2:
            return 0.0
        
        # Key entity categories for evolving stories
        key_categories = {
            'company': 1.0,           # Company names are very distinctive
            'apt_group': 1.0,         # APT groups are specific
            'ransomware_group': 1.0,  # Ransomware groups are specific
            'malware_family': 0.9,    # Malware families are important
            'vulnerability_type': 0.8, # Vulnerability types matter
            'attack_type': 0.7        # Attack types are somewhat generic
        }
        
        # Extract key entities from both sets
        key_entities1 = {}
        key_entities2 = {}
        
        for entity in entities1:
            category = entity.get('entity_category', '')
            if category in key_categories:
                name = entity.get('entity_name', '').lower()
                if name:
                    key_entities1[name] = category
        
        for entity in entities2:
            category = entity.get('entity_category', '')
            if category in key_categories:
                name = entity.get('entity_name', '').lower()
                if name:
                    key_entities2[name] = category
        
        if not key_entities1 or not key_entities2:
            return 0.0
        
        # Calculate weighted match score
        total_weight = 0.0
        match_weight = 0.0
        
        for name, category in key_entities1.items():
            weight = key_categories.get(category, 0.5)
            total_weight += weight
            if name in key_entities2:
                match_weight += weight
        
        # Also check reverse direction
        for name, category in key_entities2.items():
            if name not in key_entities1:  # Avoid double counting
                weight = key_categories.get(category, 0.5)
                total_weight += weight
        
        return match_weight / total_weight if total_weight > 0 else 0.0
    
    def is_duplicate_cluster(self, new_cluster: Dict[str, Any], 
                           existing_cluster: Dict[str, Any]) -> Tuple[bool, float]:
        """Check if new cluster is duplicate of existing cluster."""
        # Extract data from new cluster
        new_articles = set(article['rss_feeds_clean_id'] for article in new_cluster['articles'])
        new_entities = []
        new_titles = []
        new_feeds = []
        
        for article in new_cluster['articles']:
            # Extract title
            title_data = article['rss_feeds_clean_title']
            title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
            new_titles.append(title)
            
            # Extract entities
            entity_data = article.get('rss_feeds_clean_extracted_entities', {})
            if entity_data and 'entities' in entity_data:
                new_entities.extend(entity_data['entities'])
            
            # Extract feed
            new_feeds.append(article['rss_feeds_id'])
        
        # Extract data from existing cluster
        existing_articles = set(existing_cluster['article_ids'])
        existing_entities = existing_cluster.get('parsed_entities', [])
        existing_titles = [
            t.get('title', '') if isinstance(t, dict) else str(t) 
            for t in existing_cluster['article_titles']
        ]
        existing_feeds = existing_cluster['feed_ids']
        
        # Calculate similarity signals
        article_overlap = self.calculate_article_overlap(new_articles, existing_articles)
        entity_similarity = self.calculate_entity_similarity(new_entities, existing_entities)
        title_similarity = self.calculate_title_similarity(new_titles, existing_titles)
        source_overlap = self.calculate_source_overlap(new_feeds, existing_feeds)
        
        # Check for key entity matches (evolving stories)
        key_entity_match = self.check_key_entity_match(new_entities, existing_entities)
        
        # Adjusted weights for evolving stories
        if key_entity_match > 0.8:  # Strong entity match
            # For evolving stories, reduce article overlap weight
            weights = {
                'article': 0.05,  # Very low weight for article overlap
                'entity': 0.50,   # High weight for entity similarity
                'title': 0.30,    # Medium weight for title similarity
                'source': 0.05,   # Low weight for source overlap
                'key_entity': 0.10  # Bonus for key entity matches
            }
            overall_similarity = (
                weights['article'] * article_overlap +
                weights['entity'] * entity_similarity +
                weights['title'] * title_similarity +
                weights['source'] * source_overlap +
                weights['key_entity'] * key_entity_match
            )
        else:
            # Standard weights for unrelated stories
            weights = {
                'article': 0.2,   # Reduced from 0.3
                'entity': 0.4,    # Increased from 0.3
                'title': 0.3,     # Same as before
                'source': 0.1     # Same as before
            }
            overall_similarity = (
                weights['article'] * article_overlap +
                weights['entity'] * entity_similarity +
                weights['title'] * title_similarity +
                weights['source'] * source_overlap
            )
        
        # Lower threshold for strong entity matches
        threshold = 0.65 if key_entity_match > 0.8 else self.duplicate_threshold
        is_duplicate = overall_similarity >= threshold
        
        if overall_similarity >= 0.5:  # Log potential matches for debugging
            logger.info("cluster_similarity_check",
                       article_overlap=article_overlap,
                       entity_similarity=entity_similarity,
                       title_similarity=title_similarity,
                       key_entity_match=key_entity_match,
                       overall_similarity=overall_similarity,
                       is_duplicate=is_duplicate,
                       threshold=threshold)
        
        return is_duplicate, overall_similarity
    
    def extract_cluster_entities(self, cluster: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """Extract and organize entities from cluster articles."""
        entities_by_category = defaultdict(list)
        entity_counts = defaultdict(lambda: defaultdict(int))
        
        for article in cluster['articles']:
            entity_data = article.get('rss_feeds_clean_extracted_entities', {})
            if entity_data and 'entities' in entity_data:
                for entity in entity_data['entities']:
                    category = entity.get('entity_category', 'unknown')
                    name = entity.get('entity_name', '')
                    entity_counts[category][name] += 1
        
        # Get most frequent entities per category
        for category, name_counts in entity_counts.items():
            sorted_entities = sorted(name_counts.items(), key=lambda x: x[1], reverse=True)
            entities_by_category[category] = [
                {'name': name, 'count': count} for name, count in sorted_entities[:5]
            ]
        
        return dict(entities_by_category)
    
    def generate_cluster_name(self, cluster: Dict[str, Any]) -> str:
        """Generate meaningful cluster name from entities and keywords."""
        # Extract entities
        entities_by_category = self.extract_cluster_entities(cluster)
        
        # Get entity importance weights
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        name_parts = []
        
        try:
            # Priority order for entity categories
            priority_categories = [
                'apt_group', 'ransomware_group', 'malware_family',
                'cve', 'vulnerability_type', 'attack_type',
                'company', 'platform', 'security_vendor'
            ]
            
            # Get high-importance entities
            for category in priority_categories:
                if category in entities_by_category:
                    for entity_info in entities_by_category[category][:2]:  # Top 2 per category
                        entity_name = entity_info['name']
                        
                        # Get importance weight
                        cursor.execute("""
                            SELECT entities_importance_weight 
                            FROM cluster_data.entities 
                            WHERE entities_name = %s AND entities_category = %s
                        """, (entity_name, category))
                        
                        result = cursor.fetchone()
                        if result and result[0] >= 70:  # High importance threshold
                            name_parts.append(entity_name)
                            
                            if len(name_parts) >= 3:  # Limit name length
                                break
                
                if len(name_parts) >= 3:
                    break
            
            # If not enough entities, use TF-IDF keywords from titles
            if len(name_parts) < 2:
                titles = []
                for article in cluster['articles']:
                    title_data = article['rss_feeds_clean_title']
                    title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
                    titles.append(title)
                
                if titles:
                    # Extract keywords using TF-IDF
                    try:
                        vectorizer = TfidfVectorizer(
                            max_features=5,
                            stop_words='english',
                            ngram_range=(1, 2)
                        )
                        tfidf_matrix = vectorizer.fit_transform(titles)
                        feature_names = vectorizer.get_feature_names_out()
                        
                        # Get top keywords
                        tfidf_scores = tfidf_matrix.sum(axis=0).A1
                        top_indices = tfidf_scores.argsort()[-3:][::-1]
                        
                        for idx in top_indices:
                            keyword = feature_names[idx]
                            if len(keyword.split()) == 1 and len(keyword) > 3:  # Single words only
                                name_parts.append(keyword.title())
                                if len(name_parts) >= 3:
                                    break
                    except Exception as e:
                        logger.warning("keyword_extraction_error", error=str(e))
            
        finally:
            cursor.close()
            conn.close()
        
        # Generate final name
        if name_parts:
            cluster_name = " - ".join(name_parts[:3])
        else:
            # Fallback to generic name with timestamp
            cluster_name = f"Security Cluster {datetime.now().strftime('%Y%m%d%H%M')}"
        
        # Truncate if too long
        if len(cluster_name) > 200:
            cluster_name = cluster_name[:197] + "..."
        
        return cluster_name
    
    def generate_cluster_summary(self, cluster: Dict[str, Any]) -> str:
        """Generate cluster summary from primary article."""
        # Get primary article
        primary_idx = cluster['primary_article_idx']
        primary_article = None
        
        for i, idx in enumerate(cluster['article_indices']):
            if idx == primary_idx:
                primary_article = cluster['articles'][i]
                break
        
        if not primary_article:
            primary_article = cluster['articles'][0]
        
        # Extract title and content
        title_data = primary_article['rss_feeds_clean_title']
        content_data = primary_article['rss_feeds_clean_content']
        
        title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
        content = content_data.get('content', '') if isinstance(content_data, dict) else str(content_data)
        
        # Create summary
        summary = f"{title}\n\n{content[:500]}..."
        
        return summary
    
    def store_cluster(self, cluster: Dict[str, Any], 
                     existing_cluster_id: Optional[int] = None) -> int:
        """Store cluster and article relationships in database."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            if existing_cluster_id:
                # Add articles to existing cluster
                cluster_id = existing_cluster_id
                
                # Update cluster coherence if better
                cursor.execute("""
                    UPDATE cluster_data.clusters
                    SET clusters_coherence_score = GREATEST(clusters_coherence_score, %s)
                    WHERE clusters_id = %s
                """, (float(cluster['coherence_score']), cluster_id))
                
                logger.info("adding_to_existing_cluster", cluster_id=cluster_id)
            else:
                # Create new cluster
                cluster_name = self.generate_cluster_name(cluster)
                cluster_summary = self.generate_cluster_summary(cluster)
                
                cursor.execute("""
                    INSERT INTO cluster_data.clusters
                    (clusters_name, clusters_summary, clusters_coherence_score)
                    VALUES (%s, %s, %s)
                    RETURNING clusters_id
                """, (cluster_name, cluster_summary, float(cluster['coherence_score'])))
                
                cluster_id = cursor.fetchone()[0]
                logger.info("created_new_cluster", 
                          cluster_id=cluster_id,
                          name=cluster_name)
            
            # Store article relationships
            articles_stored = 0
            for i, article_idx in enumerate(cluster['article_indices']):
                article = cluster['articles'][i]
                article_id = article['rss_feeds_clean_id']
                is_primary = (article_idx == cluster['primary_article_idx'])
                similarity_score = cluster['article_similarities'].get(article_idx, 1.0)
                
                try:
                    cursor.execute("""
                        INSERT INTO cluster_data.cluster_articles
                        (cluster_articles_cluster_id, cluster_articles_clean_id,
                         cluster_articles_is_primary, cluster_articles_similarity_score)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (cluster_articles_cluster_id, cluster_articles_clean_id)
                        DO NOTHING
                    """, (cluster_id, article_id, is_primary, float(similarity_score)))
                    
                    if cursor.rowcount > 0:
                        articles_stored += 1
                        
                except psycopg2.IntegrityError as e:
                    logger.warning("article_already_in_cluster",
                                 article_id=article_id,
                                 cluster_id=cluster_id)
            
            conn.commit()
            logger.info("cluster_stored",
                       cluster_id=cluster_id,
                       articles_stored=articles_stored)
            
            return cluster_id
            
        except Exception as e:
            logger.error("cluster_storage_error", error=str(e))
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    def process_clusters(self, clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process and store clusters with deduplication."""
        stats = {
            'clusters_created': 0,
            'clusters_merged': 0,
            'articles_assigned': 0,
            'duplicate_clusters_prevented': 0,
            'articles_added_to_existing': 0
        }
        
        # Get existing clusters for comparison
        existing_clusters = self.get_existing_clusters()
        
        for cluster in clusters:
            # Check if this is an assignment to existing cluster
            if 'existing_cluster_id' in cluster:
                # Just add article to existing cluster
                self.store_cluster(cluster, cluster['existing_cluster_id'])
                stats['articles_added_to_existing'] += len(cluster['article_indices'])
                stats['articles_assigned'] += len(cluster['article_indices'])
                continue
            
            # For new clusters, check for duplicates
            duplicate_found = False
            existing_cluster_id = None
            
            for existing in existing_clusters:
                is_duplicate, similarity = self.is_duplicate_cluster(cluster, existing)
                if is_duplicate:
                    duplicate_found = True
                    existing_cluster_id = existing['clusters_id']
                    stats['duplicate_clusters_prevented'] += 1
                    break
            
            # Store cluster
            if duplicate_found:
                self.store_cluster(cluster, existing_cluster_id)
                stats['clusters_merged'] += 1
            else:
                self.store_cluster(cluster)
                stats['clusters_created'] += 1
            
            stats['articles_assigned'] += len(cluster['article_indices'])
        
        # Clear cache after processing
        self.existing_clusters_cache = None
        
        return stats