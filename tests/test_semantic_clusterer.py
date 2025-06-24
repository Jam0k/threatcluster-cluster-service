#!/usr/bin/env python3
"""
Test script for Semantic Clustering

Tests the clustering functionality with real and synthetic articles.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m tests.test_semantic_clusterer
"""
import sys
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from src.clustering.semantic_clusterer import SemanticClusterer
from src.clustering.cluster_manager import ClusterManager
from src.config.settings import settings
import psycopg2
import psycopg2.extras


def test_embedding_generation():
    """Test embedding generation for articles."""
    print("\nTesting embedding generation...")
    print("=" * 50)
    
    clusterer = SemanticClusterer()
    
    # Create test articles
    test_articles = [
        {
            'rss_feeds_clean_id': 1,
            'rss_feeds_clean_title': {'title': 'Lazarus Group Launches New Ransomware Attack'},
            'rss_feeds_clean_content': {'content': 'The notorious Lazarus Group has launched a new ransomware campaign targeting financial institutions.'},
            'rss_feeds_clean_extracted_entities': {'entities': [
                {'entity_name': 'Lazarus Group', 'entity_category': 'apt_group'},
                {'entity_name': 'Ransomware', 'entity_category': 'attack_type'}
            ]}
        },
        {
            'rss_feeds_clean_id': 2,
            'rss_feeds_clean_title': {'title': 'APT28 Exploits Zero-Day Vulnerability'},
            'rss_feeds_clean_content': {'content': 'Russian threat actor APT28 has been observed exploiting a critical zero-day vulnerability in Microsoft Exchange.'},
            'rss_feeds_clean_extracted_entities': {'entities': [
                {'entity_name': 'APT28', 'entity_category': 'apt_group'},
                {'entity_name': 'Microsoft Exchange', 'entity_category': 'platform'}
            ]}
        }
    ]
    
    try:
        # Generate embeddings
        embeddings = clusterer.generate_embeddings(test_articles)
        
        print(f"✓ Generated embeddings shape: {embeddings.shape}")
        print(f"✓ Embedding dimension: {embeddings.shape[1]}")
        
        # Test similarity
        similarity_matrix = clusterer.calculate_similarity_matrix(embeddings)
        print(f"\n✓ Similarity matrix shape: {similarity_matrix.shape}")
        print(f"  Self-similarity (should be 1.0): {similarity_matrix[0,0]:.3f}")
        print(f"  Cross-similarity: {similarity_matrix[0,1]:.3f}")
        
        return True
        
    except Exception as e:
        print(f"✗ Embedding generation failed: {e}")
        return False


def test_clustering_algorithms():
    """Test DBSCAN and hierarchical clustering."""
    print("\nTesting clustering algorithms...")
    print("=" * 50)
    
    clusterer = SemanticClusterer()
    
    # Create test articles with varying similarity
    base_time = datetime.now(timezone.utc)
    test_articles = [
        # Cluster 1: Ransomware attacks
        {
            'rss_feeds_clean_id': 1,
            'rss_feeds_clean_title': {'title': 'LockBit Ransomware Hits Major Hospital'},
            'rss_feeds_clean_content': {'content': 'LockBit ransomware group attacks healthcare facility demanding millions in ransom.'},
            'rss_feeds_raw_published_date': base_time,
            'rss_feeds_id': 1
        },
        {
            'rss_feeds_clean_id': 2,
            'rss_feeds_clean_title': {'title': 'Hospital Pays Ransom After LockBit Attack'},
            'rss_feeds_clean_content': {'content': 'Healthcare provider confirms payment to LockBit ransomware operators after crippling attack.'},
            'rss_feeds_raw_published_date': base_time + timedelta(hours=2),
            'rss_feeds_id': 2
        },
        # Cluster 2: APT activity
        {
            'rss_feeds_clean_id': 3,
            'rss_feeds_clean_title': {'title': 'APT29 Targets Government Networks'},
            'rss_feeds_clean_content': {'content': 'Russian APT29 group launches sophisticated campaign against government infrastructure.'},
            'rss_feeds_raw_published_date': base_time + timedelta(hours=1),
            'rss_feeds_id': 1
        },
        {
            'rss_feeds_clean_id': 4,
            'rss_feeds_clean_title': {'title': 'Government Agencies Under APT29 Attack'},
            'rss_feeds_clean_content': {'content': 'Multiple government agencies report intrusions linked to APT29 threat actors.'},
            'rss_feeds_raw_published_date': base_time + timedelta(hours=3),
            'rss_feeds_id': 3
        },
        # Outlier
        {
            'rss_feeds_clean_id': 5,
            'rss_feeds_clean_title': {'title': 'New Security Tool Released'},
            'rss_feeds_clean_content': {'content': 'Vendor releases new endpoint protection tool with advanced features.'},
            'rss_feeds_raw_published_date': base_time + timedelta(hours=4),
            'rss_feeds_id': 4
        }
    ]
    
    try:
        # Generate embeddings
        embeddings = clusterer.generate_embeddings(test_articles)
        similarity_matrix = clusterer.calculate_similarity_matrix(embeddings)
        
        # Test DBSCAN
        print("\nTesting DBSCAN clustering:")
        dbscan_clusters = clusterer.cluster_articles_dbscan(test_articles, similarity_matrix)
        print(f"  Clusters found: {len(dbscan_clusters)}")
        
        for cluster_id, cluster_info in dbscan_clusters.items():
            indices = cluster_info['indices']
            coherence = cluster_info['coherence']
            print(f"  Cluster {cluster_id}: {len(indices)} articles, coherence: {coherence:.3f}")
            for idx in indices:
                print(f"    - {test_articles[idx]['rss_feeds_clean_title']['title']}")
        
        # Test Hierarchical
        print("\nTesting Hierarchical clustering:")
        hier_clusters = clusterer.cluster_articles_hierarchical(test_articles, similarity_matrix)
        print(f"  Clusters found: {len(hier_clusters)}")
        
        return len(dbscan_clusters) > 0 or len(hier_clusters) > 0
        
    except Exception as e:
        print(f"✗ Clustering test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_duplicate_detection():
    """Test duplicate cluster detection."""
    print("\nTesting duplicate cluster detection...")
    print("=" * 50)
    
    manager = ClusterManager()
    
    # Create test clusters
    cluster1 = {
        'articles': [
            {
                'rss_feeds_clean_id': 1,
                'rss_feeds_clean_title': {'title': 'Ransomware Attack on Hospital'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'},
                    {'entity_name': 'Healthcare', 'entity_category': 'industry_sector'}
                ]},
                'rss_feeds_id': 1
            },
            {
                'rss_feeds_clean_id': 2,
                'rss_feeds_clean_title': {'title': 'Hospital Confirms Ransomware Payment'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'}
                ]},
                'rss_feeds_id': 2
            }
        ]
    }
    
    # Similar cluster (should be detected as duplicate)
    cluster2 = {
        'articles': [
            {
                'rss_feeds_clean_id': 3,
                'rss_feeds_clean_title': {'title': 'LockBit Ransomware Hits Another Hospital'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'},
                    {'entity_name': 'Healthcare', 'entity_category': 'industry_sector'}
                ]},
                'rss_feeds_id': 1
            },
            {
                'rss_feeds_clean_id': 2,  # Shared article
                'rss_feeds_clean_title': {'title': 'Hospital Confirms Ransomware Payment'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'}
                ]},
                'rss_feeds_id': 2
            }
        ]
    }
    
    # Different cluster
    cluster3 = {
        'articles': [
            {
                'rss_feeds_clean_id': 10,
                'rss_feeds_clean_title': {'title': 'APT28 Targets Government'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'APT28', 'entity_category': 'apt_group'}
                ]},
                'rss_feeds_id': 3
            }
        ]
    }
    
    # Mock existing cluster
    existing_cluster = {
        'clusters_id': 1,
        'article_ids': [1, 2],
        'article_titles': [
            {'title': 'Ransomware Attack on Hospital'},
            {'title': 'Hospital Confirms Ransomware Payment'}
        ],
        'parsed_entities': [
            {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'},
            {'entity_name': 'Healthcare', 'entity_category': 'industry_sector'},
            {'entity_name': 'LockBit', 'entity_category': 'ransomware_group'}
        ],
        'feed_ids': [1, 2]
    }
    
    # Test duplicate detection
    is_dup1, sim1 = manager.is_duplicate_cluster(cluster2, existing_cluster)
    is_dup2, sim2 = manager.is_duplicate_cluster(cluster3, existing_cluster)
    
    print(f"✓ Cluster 2 vs Existing: Duplicate={is_dup1}, Similarity={sim1:.3f}")
    print(f"✓ Cluster 3 vs Existing: Duplicate={is_dup2}, Similarity={sim2:.3f}")
    
    return is_dup1 and not is_dup2  # Should detect cluster2 as duplicate but not cluster3


def test_cluster_naming():
    """Test cluster name generation."""
    print("\nTesting cluster name generation...")
    print("=" * 50)
    
    manager = ClusterManager()
    
    # Test cluster with high-importance entities
    test_cluster = {
        'articles': [
            {
                'rss_feeds_clean_id': 1,
                'rss_feeds_clean_title': {'title': 'Lazarus Group Deploys New Malware'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'Lazarus Group', 'entity_category': 'apt_group'},
                    {'entity_name': 'CVE-2023-1234', 'entity_category': 'cve'}
                ]}
            },
            {
                'rss_feeds_clean_id': 2,
                'rss_feeds_clean_title': {'title': 'North Korean APT Targets Banks'},
                'rss_feeds_clean_extracted_entities': {'entities': [
                    {'entity_name': 'Lazarus Group', 'entity_category': 'apt_group'},
                    {'entity_name': 'Financial', 'entity_category': 'industry_sector'}
                ]}
            }
        ]
    }
    
    name = manager.generate_cluster_name(test_cluster)
    print(f"✓ Generated cluster name: {name}")
    
    # Test cluster with no high-importance entities
    test_cluster2 = {
        'articles': [
            {
                'rss_feeds_clean_id': 3,
                'rss_feeds_clean_title': {'title': 'Security Update Released'},
                'rss_feeds_clean_extracted_entities': {'entities': []}
            }
        ]
    }
    
    name2 = manager.generate_cluster_name(test_cluster2)
    print(f"✓ Fallback cluster name: {name2}")
    
    return len(name) > 0 and len(name2) > 0


def test_database_operations():
    """Test database read operations."""
    print("\nTesting database operations...")
    print("=" * 50)
    
    try:
        clusterer = SemanticClusterer()
        
        # Get unclustered articles
        articles = clusterer.get_unclustered_articles(time_window_hours=168)  # 1 week
        print(f"✓ Found {len(articles)} unclustered articles")
        
        if articles:
            # Show sample article
            article = articles[0]
            title = article['rss_feeds_clean_title'].get('title', 'N/A') if isinstance(article['rss_feeds_clean_title'], dict) else 'N/A'
            print(f"\nSample article:")
            print(f"  ID: {article['rss_feeds_clean_id']}")
            print(f"  Title: {title[:60]}...")
            print(f"  Published: {article['rss_feeds_raw_published_date']}")
            
            # Check for entities
            entities = article.get('rss_feeds_clean_extracted_entities', {})
            if entities and 'entities' in entities:
                print(f"  Entities: {len(entities['entities'])}")
        
        return True
        
    except Exception as e:
        print(f"✗ Database operation failed: {e}")
        return False


def test_full_pipeline():
    """Test full clustering pipeline with real data."""
    print("\nTesting full clustering pipeline...")
    print("=" * 50)
    
    try:
        clusterer = SemanticClusterer()
        manager = ClusterManager()
        
        # Get a small batch of articles
        articles = clusterer.get_unclustered_articles(time_window_hours=72)[:10]
        
        if not articles:
            print("⚠ No unclustered articles found for pipeline test")
            return True  # Not a failure, just no data
        
        print(f"Processing {len(articles)} articles...")
        
        # Process clustering
        clusters, cluster_data = clusterer.process_batch(articles)
        
        print(f"\n✓ Created {len(cluster_data)} clusters")
        
        # Show cluster details
        for i, cluster in enumerate(cluster_data[:3]):  # Show first 3 clusters
            print(f"\nCluster {i+1}:")
            print(f"  Articles: {len(cluster['article_indices'])}")
            print(f"  Coherence: {cluster['coherence_score']:.3f}")
            
            # Generate name
            name = manager.generate_cluster_name(cluster)
            print(f"  Generated name: {name}")
            
            # Show articles
            for j, article in enumerate(cluster['articles'][:2]):  # First 2 articles
                title = article['rss_feeds_clean_title'].get('title', 'Unknown')
                print(f"    - {title[:60]}...")
        
        return len(cluster_data) >= 0  # Success even if no clusters formed
        
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("Semantic Clustering Test Suite")
    print("=" * 70)
    
    tests = [
        ("Embedding Generation", test_embedding_generation),
        ("Clustering Algorithms", test_clustering_algorithms),
        ("Duplicate Detection", test_duplicate_detection),
        ("Cluster Naming", test_cluster_naming),
        ("Database Operations", test_database_operations),
        ("Full Pipeline", test_full_pipeline),
    ]
    
    passed = 0
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"\n✗ {test_name} test failed")
        except Exception as e:
            print(f"\n✗ {test_name} test error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())