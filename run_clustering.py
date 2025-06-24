#!/usr/bin/env python3
"""
Run Semantic Clustering

Quick script to run semantic clustering on unclustered articles.

Usage:
    python run_clustering.py
"""

from src.clustering.cluster_scheduler import ClusterScheduler

def main():
    """Run clustering once on recent articles."""
    scheduler = ClusterScheduler()
    
    print("Running semantic clustering...")
    stats = scheduler.run_once(mode='incremental')
    
    print("\nClustering Results:")
    print(f"  Articles processed: {stats.get('articles_processed', 0)}")
    print(f"  Clusters created: {stats.get('clusters_created', 0)}")
    print(f"  Articles clustered: {stats.get('articles_assigned', 0)}")
    print(f"  Duplicate clusters prevented: {stats.get('duplicate_clusters_prevented', 0)}")
    print(f"  Processing time: {stats.get('processing_time_seconds', 0):.2f} seconds")

if __name__ == "__main__":
    main()