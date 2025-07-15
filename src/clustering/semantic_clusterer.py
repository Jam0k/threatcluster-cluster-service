#!/usr/bin/env python3
"""
Semantic Clustering Module

Generates semantic embeddings from article content and clusters related articles
using similarity algorithms. Groups articles about similar cybersecurity topics,
threats, or events for better organization and analysis.
"""
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import psycopg2
import psycopg2.extras
import json
import time
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict
import structlog

from src.config.settings import settings


# Configure structured logging
logger = structlog.get_logger(__name__)


class SemanticClusterer:
    """Performs semantic clustering on articles with extracted entities."""
    
    def __init__(self):
        """Initialize the semantic clusterer with configuration."""
        self.config = settings.app_config
        self.clustering_config = self.config.get('clustering', {})
        self.pipeline_config = self.config.get('pipeline', {})
        
        # Clustering parameters
        self.model_name = self.clustering_config.get('model_name', 'sentence-transformers/all-mpnet-base-v2')
        self.similarity_threshold = self.clustering_config.get('similarity_threshold', 0.75)
        self.min_cluster_size = self.clustering_config.get('min_cluster_size', 2)
        self.max_cluster_size = self.clustering_config.get('max_cluster_size', 12)
        self.time_window_hours = self.clustering_config.get('time_window_hours', 72)
        self.coherence_threshold = self.clustering_config.get('coherence_threshold', 0.65)
        self.batch_size = self.clustering_config.get('batch_size', 50)
        
        # Set cache directories from environment
        self.cache_dir = os.environ.get('TRANSFORMERS_CACHE', '/tmp/transformers_cache')
        os.environ['TRANSFORMERS_CACHE'] = self.cache_dir
        os.environ['HF_HOME'] = os.environ.get('HF_HOME', self.cache_dir)
        
        # Initialize sentence transformer model
        self.model = None
        self._initialize_model()
        
        # Statistics tracking
        self.stats = {
            'articles_processed': 0,
            'clusters_created': 0,
            'articles_clustered': 0,
            'clustering_errors': 0,
            'duplicate_clusters_prevented': 0
        }
    
    def _initialize_model(self):
        """Initialize the sentence transformer model."""
        try:
            logger.info("initializing_sentence_transformer", model=self.model_name)
            self.model = SentenceTransformer(self.model_name, cache_folder=self.cache_dir)
            logger.info("model_initialized_successfully")
        except Exception as e:
            logger.error("model_initialization_error", error=str(e))
            raise
    
    def get_unclustered_articles(self, time_window_hours: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch articles that haven't been clustered yet."""
        if time_window_hours is None:
            time_window_hours = self.time_window_hours * 2  # Extended window for initial processing
        
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            # Calculate time window
            cutoff_date = datetime.now(timezone.utc) - timedelta(hours=time_window_hours)
            
            query = """
                SELECT 
                    rfc.rss_feeds_clean_id,
                    rfc.rss_feeds_clean_title,
                    rfc.rss_feeds_clean_content,
                    rfc.rss_feeds_clean_extracted_entities,
                    rfr.rss_feeds_raw_published_date,
                    rf.rss_feeds_id,
                    rf.rss_feeds_name,
                    rf.rss_feeds_credibility
                FROM cluster_data.rss_feeds_clean rfc
                JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
                JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
                WHERE rfc.rss_feeds_clean_extracted_entities IS NOT NULL
                AND rfr.rss_feeds_raw_published_date >= %s
                -- Exclude articles marked with no_clustering flag
                AND (rfc.rss_feeds_clean_extracted_entities->>'no_clustering' IS NULL 
                     OR rfc.rss_feeds_clean_extracted_entities->>'no_clustering' = 'false')
                AND NOT EXISTS (
                    SELECT 1 FROM cluster_data.cluster_articles ca
                    JOIN cluster_data.clusters c ON ca.cluster_articles_cluster_id = c.clusters_id
                    WHERE ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                    AND c.clusters_is_active = true
                )
                ORDER BY rfr.rss_feeds_raw_published_date DESC
            """
            cursor.execute(query, (cutoff_date,))
            articles = [dict(row) for row in cursor.fetchall()]
            
            logger.info("fetched_unclustered_articles", 
                       count=len(articles),
                       time_window_hours=time_window_hours)
            return articles
            
        finally:
            cursor.close()
            conn.close()
    
    def prepare_text_for_embedding(self, article: Dict[str, Any]) -> str:
        """Prepare article text for embedding generation."""
        # Extract title and content
        title_data = article['rss_feeds_clean_title']
        content_data = article['rss_feeds_clean_content']
        
        title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
        content = content_data.get('content', '') if isinstance(content_data, dict) else str(content_data)
        
        # Weight title more heavily by including it twice
        # This gives more importance to title similarity
        weighted_text = f"{title}\n{title}\n\n{content}"
        
        return weighted_text
    
    def generate_embeddings(self, articles: List[Dict[str, Any]]) -> np.ndarray:
        """Generate embeddings for a batch of articles."""
        texts = [self.prepare_text_for_embedding(article) for article in articles]
        
        try:
            # Generate embeddings in batches
            embeddings = []
            for i in range(0, len(texts), self.batch_size):
                batch_texts = texts[i:i + self.batch_size]
                batch_embeddings = self.model.encode(batch_texts, convert_to_numpy=True)
                embeddings.extend(batch_embeddings)
            
            return np.array(embeddings)
            
        except Exception as e:
            logger.error("embedding_generation_error", error=str(e))
            raise
    
    def calculate_similarity_matrix(self, embeddings: np.ndarray) -> np.ndarray:
        """Calculate cosine similarity matrix between embeddings."""
        # Calculate cosine similarity
        similarity_matrix = cosine_similarity(embeddings)
        
        # Ensure diagonal is 1.0 (self-similarity)
        np.fill_diagonal(similarity_matrix, 1.0)
        
        # Clip values to ensure they're in valid range [0, 1]
        similarity_matrix = np.clip(similarity_matrix, 0.0, 1.0)
        
        return similarity_matrix
    
    def validate_cluster_time_window(self, articles: List[Dict[str, Any]], 
                                   cluster_indices: List[int]) -> bool:
        """Check if all articles in cluster are within time window."""
        if len(cluster_indices) < 2:
            return True
        
        # Get publication dates for cluster articles
        dates = [articles[idx]['rss_feeds_raw_published_date'] for idx in cluster_indices]
        
        # Convert to datetime if needed
        parsed_dates = []
        for date in dates:
            if isinstance(date, str):
                parsed_dates.append(datetime.fromisoformat(date.replace('Z', '+00:00')))
            else:
                parsed_dates.append(date)
        
        # Check time difference
        min_date = min(parsed_dates)
        max_date = max(parsed_dates)
        time_diff = (max_date - min_date).total_seconds() / 3600  # Convert to hours
        
        return time_diff <= self.time_window_hours
    
    def calculate_cluster_coherence(self, similarity_matrix: np.ndarray, 
                                  cluster_indices: List[int]) -> float:
        """Calculate average intra-cluster similarity."""
        if len(cluster_indices) < 2:
            return 1.0
        
        # Get similarities between all pairs in cluster
        similarities = []
        for i in range(len(cluster_indices)):
            for j in range(i + 1, len(cluster_indices)):
                idx1, idx2 = cluster_indices[i], cluster_indices[j]
                similarities.append(similarity_matrix[idx1, idx2])
        
        return np.mean(similarities) if similarities else 0.0
    
    def cluster_articles_dbscan(self, articles: List[Dict[str, Any]], 
                               similarity_matrix: np.ndarray) -> Dict[int, List[int]]:
        """Cluster articles using DBSCAN algorithm."""
        # Convert similarity to distance
        distance_matrix = 1 - similarity_matrix
        
        # Ensure no negative values
        distance_matrix = np.maximum(distance_matrix, 0.0)
        
        # Apply DBSCAN
        eps = 1 - self.similarity_threshold
        clustering = DBSCAN(
            eps=eps,
            min_samples=self.min_cluster_size,
            metric='precomputed'
        ).fit(distance_matrix)
        
        # Group articles by cluster
        clusters = defaultdict(list)
        for idx, label in enumerate(clustering.labels_):
            if label != -1:  # Ignore noise points
                clusters[label].append(idx)
        
        # Validate clusters
        valid_clusters = {}
        cluster_id = 0
        
        for label, indices in clusters.items():
            # Check size constraints
            if self.min_cluster_size <= len(indices) <= self.max_cluster_size:
                # Check time window
                if self.validate_cluster_time_window(articles, indices):
                    # Check coherence
                    coherence = self.calculate_cluster_coherence(similarity_matrix, indices)
                    if coherence >= self.coherence_threshold:
                        valid_clusters[cluster_id] = {
                            'indices': indices,
                            'coherence': coherence
                        }
                        cluster_id += 1
        
        logger.info("dbscan_clustering_complete",
                   total_clusters=len(clusters),
                   valid_clusters=len(valid_clusters))
        
        return valid_clusters
    
    def cluster_articles_hierarchical(self, articles: List[Dict[str, Any]], 
                                    similarity_matrix: np.ndarray) -> Dict[int, List[int]]:
        """Fallback clustering using Agglomerative Hierarchical Clustering."""
        # Convert similarity to distance
        distance_matrix = 1 - similarity_matrix
        
        # Ensure no negative values
        distance_matrix = np.maximum(distance_matrix, 0.0)
        
        # Apply Agglomerative Clustering
        distance_threshold = 1 - self.similarity_threshold
        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=distance_threshold,
            metric='precomputed',
            linkage='average'
        ).fit(distance_matrix)
        
        # Group articles by cluster
        clusters = defaultdict(list)
        for idx, label in enumerate(clustering.labels_):
            clusters[label].append(idx)
        
        # Validate clusters (same as DBSCAN)
        valid_clusters = {}
        cluster_id = 0
        
        for label, indices in clusters.items():
            if self.min_cluster_size <= len(indices) <= self.max_cluster_size:
                if self.validate_cluster_time_window(articles, indices):
                    coherence = self.calculate_cluster_coherence(similarity_matrix, indices)
                    if coherence >= self.coherence_threshold:
                        valid_clusters[cluster_id] = {
                            'indices': indices,
                            'coherence': coherence
                        }
                        cluster_id += 1
        
        logger.info("hierarchical_clustering_complete",
                   total_clusters=len(clusters),
                   valid_clusters=len(valid_clusters))
        
        return valid_clusters
    
    def find_primary_article(self, articles: List[Dict[str, Any]], 
                           cluster_indices: List[int],
                           embeddings: np.ndarray) -> int:
        """Find the most representative article in a cluster (closest to centroid)."""
        if len(cluster_indices) == 1:
            return cluster_indices[0]
        
        # Get cluster embeddings
        cluster_embeddings = embeddings[cluster_indices]
        
        # Calculate centroid
        centroid = np.mean(cluster_embeddings, axis=0)
        
        # Find article closest to centroid
        distances = [np.linalg.norm(cluster_embeddings[i] - centroid) 
                    for i in range(len(cluster_indices))]
        
        primary_idx = cluster_indices[np.argmin(distances)]
        return primary_idx
    
    def get_existing_cluster_embeddings(self, days: int = 3) -> Tuple[List[Dict], np.ndarray]:
        """Get embeddings for existing clusters from recent days."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            
            # Get primary articles from recent clusters
            query = """
                SELECT DISTINCT ON (c.clusters_id)
                    c.clusters_id,
                    c.clusters_name,
                    rfc.rss_feeds_clean_id,
                    rfc.rss_feeds_clean_title,
                    rfc.rss_feeds_clean_content,
                    rfc.rss_feeds_clean_extracted_entities
                FROM cluster_data.clusters c
                JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
                JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
                WHERE c.clusters_is_active = true
                AND c.clusters_created_at >= %s
                AND ca.cluster_articles_is_primary = true
                ORDER BY c.clusters_id
            """
            cursor.execute(query, (cutoff_date,))
            
            existing_clusters = []
            cluster_texts = []
            
            for row in cursor:
                cluster_info = dict(row)
                existing_clusters.append(cluster_info)
                # Generate text for embedding
                cluster_texts.append(self.prepare_text_for_embedding(cluster_info))
            
            if not existing_clusters:
                return [], np.array([])
            
            # Generate embeddings for existing clusters
            cluster_embeddings = self.model.encode(cluster_texts, convert_to_numpy=True)
            
            logger.info("loaded_existing_clusters", count=len(existing_clusters))
            return existing_clusters, cluster_embeddings
            
        finally:
            cursor.close()
            conn.close()
    
    def assign_to_existing_clusters(self, articles: List[Dict[str, Any]], 
                                  embeddings: np.ndarray) -> Tuple[Dict[int, List[int]], List[int]]:
        """Try to assign articles to existing clusters first."""
        # Get existing cluster embeddings
        existing_clusters, cluster_embeddings = self.get_existing_cluster_embeddings()
        
        if len(existing_clusters) == 0:
            return {}, list(range(len(articles)))
        
        # Calculate similarities between articles and existing clusters
        similarities = cosine_similarity(embeddings, cluster_embeddings)
        
        # Assign articles to clusters or mark as unassigned
        cluster_assignments = defaultdict(list)
        unassigned_indices = []
        
        for article_idx in range(len(articles)):
            # Find best matching cluster
            cluster_similarities = similarities[article_idx]
            best_cluster_idx = np.argmax(cluster_similarities)
            best_similarity = cluster_similarities[best_cluster_idx]
            
            if best_similarity >= self.similarity_threshold:
                # Assign to existing cluster
                cluster_id = existing_clusters[best_cluster_idx]['clusters_id']
                cluster_assignments[cluster_id].append(article_idx)
                logger.debug("assigned_to_existing_cluster", 
                           article_idx=article_idx,
                           cluster_id=cluster_id,
                           similarity=best_similarity)
            else:
                # Mark as unassigned
                unassigned_indices.append(article_idx)
        
        logger.info("existing_cluster_assignment_complete",
                   assigned_count=len(articles) - len(unassigned_indices),
                   unassigned_count=len(unassigned_indices))
        
        return dict(cluster_assignments), unassigned_indices

    def process_batch(self, articles: List[Dict[str, Any]]) -> Tuple[Dict, List[Dict]]:
        """Process a batch of articles for clustering."""
        if not articles:
            return {}, []
        
        logger.info("processing_clustering_batch", article_count=len(articles))
        
        # Generate embeddings
        embeddings = self.generate_embeddings(articles)
        
        # FIRST: Try to assign articles to existing clusters
        existing_assignments, unassigned_indices = self.assign_to_existing_clusters(articles, embeddings)
        
        # Prepare data for existing cluster assignments
        cluster_data = []
        for cluster_id, article_indices in existing_assignments.items():
            # Create assignment data for existing clusters
            for idx in article_indices:
                cluster_data.append({
                    'existing_cluster_id': cluster_id,
                    'article_indices': [idx],
                    'primary_article_idx': idx,
                    'coherence_score': 1.0,  # Single article assignment
                    'article_similarities': {idx: 1.0},
                    'articles': [articles[idx]]
                })
            self.stats['articles_clustered'] += len(article_indices)
        
        # SECOND: Cluster only unassigned articles
        if unassigned_indices:
            unassigned_articles = [articles[idx] for idx in unassigned_indices]
            unassigned_embeddings = embeddings[unassigned_indices]
            
            # Calculate similarity matrix for unassigned articles only
            similarity_matrix = self.calculate_similarity_matrix(unassigned_embeddings)
            
            # Try DBSCAN first
            clusters = self.cluster_articles_dbscan(unassigned_articles, similarity_matrix)
            
            # If too few clusters, try hierarchical as fallback
            if len(clusters) < len(unassigned_articles) * 0.1:
                logger.info("trying_hierarchical_clustering_fallback")
                clusters_hierarchical = self.cluster_articles_hierarchical(unassigned_articles, similarity_matrix)
                if len(clusters_hierarchical) > len(clusters):
                    clusters = clusters_hierarchical
            
            # Prepare cluster data for new clusters
            for cluster_id, cluster_info in clusters.items():
                indices = cluster_info['indices']
                coherence = cluster_info['coherence']
                
                # Map back to original article indices
                original_indices = [unassigned_indices[idx] for idx in indices]
                
                # Find primary article
                primary_idx = self.find_primary_article(unassigned_articles, indices, unassigned_embeddings)
                primary_original_idx = unassigned_indices[primary_idx]
                
                # Calculate similarity scores for each article
                article_similarities = {}
                if len(indices) > 1:
                    centroid_embedding = np.mean(unassigned_embeddings[indices], axis=0)
                    for i, idx in enumerate(indices):
                        similarity = cosine_similarity(
                            unassigned_embeddings[idx].reshape(1, -1),
                            centroid_embedding.reshape(1, -1)
                        )[0][0]
                        article_similarities[original_indices[i]] = float(similarity)
                else:
                    article_similarities[original_indices[0]] = 1.0
                
                cluster_data.append({
                    'article_indices': original_indices,
                    'primary_article_idx': primary_original_idx,
                    'coherence_score': coherence,
                    'article_similarities': article_similarities,
                    'articles': [articles[idx] for idx in original_indices]
                })
            
            # Update statistics
            self.stats['clusters_created'] += len(clusters)
            for cluster in clusters.values():
                self.stats['articles_clustered'] += len(cluster['indices'])
        
        return {}, cluster_data


def main():
    """Run semantic clustering as standalone script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Semantic Clustering of Articles')
    parser.add_argument('--limit', type=int, help='Limit number of articles to process')
    parser.add_argument('--test', action='store_true', help='Test clustering with small batch')
    
    args = parser.parse_args()
    
    clusterer = SemanticClusterer()
    
    if args.test:
        # Test mode - process small batch
        articles = clusterer.get_unclustered_articles(time_window_hours=24)[:10]
        if articles:
            print(f"\nTesting with {len(articles)} articles")
            clusters, cluster_data = clusterer.process_batch(articles)
            
            print(f"\nCreated {len(cluster_data)} clusters:")
            for i, cluster in enumerate(cluster_data):
                print(f"\nCluster {i+1}:")
                print(f"  Articles: {len(cluster['article_indices'])}")
                print(f"  Coherence: {cluster['coherence_score']:.3f}")
                print(f"  Primary article: {cluster['articles'][0]['rss_feeds_clean_title'].get('title', 'Unknown')[:60]}...")
    else:
        # Normal processing
        articles = clusterer.get_unclustered_articles()
        if args.limit:
            articles = articles[:args.limit]
        
        print(f"\nProcessing {len(articles)} unclustered articles")
        
        # Process in batches
        all_clusters = []
        for i in range(0, len(articles), clusterer.batch_size):
            batch = articles[i:i + clusterer.batch_size]
            clusters, cluster_data = clusterer.process_batch(batch)
            all_clusters.extend(cluster_data)
        
        print(f"\nClustering complete:")
        print(f"  Total clusters: {len(all_clusters)}")
        print(f"  Articles clustered: {sum(len(c['article_indices']) for c in all_clusters)}")


if __name__ == "__main__":
    main()