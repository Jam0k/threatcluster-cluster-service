# Semantic Clustering Documentation

This document explains the semantic clustering system used in ThreatCluster to group related cybersecurity articles.

## Table of Contents
- [Overview](#overview)
- [Technical Architecture](#technical-architecture)
- [The Clustering Process](#the-clustering-process)
- [Understanding Clusters](#understanding-clusters)
- [Running the System](#running-the-system)
- [Configuration](#configuration)
- [Interpreting Results](#interpreting-results)

## Overview

### What is Semantic Clustering?
Semantic clustering is an AI-powered technique that groups similar articles together based on their meaning, not just keywords. Think of it as an intelligent filing system that understands when two articles are about the same cyberattack, even if they use different words to describe it.

### Why Use It?
- **Reduce Information Overload**: Instead of reading 10 articles about the same breach, see them grouped together
- **Track Campaign Evolution**: Follow how a cyberattack develops across multiple sources
- **Identify Patterns**: Spot trends when similar attacks target multiple organizations
- **Prioritize Response**: Focus on unique incidents rather than duplicate reporting

## Technical Architecture

The clustering system consists of three main components:

### 1. Semantic Clusterer (`src/clustering/semantic_clusterer.py`)
- **Purpose**: Generates AI embeddings and performs clustering algorithms
- **Key Technology**: Sentence Transformers (all-mpnet-base-v2 model)
- **Algorithms**: DBSCAN and Agglomerative Clustering

### 2. Cluster Manager (`src/clustering/cluster_manager.py`)
- **Purpose**: Handles cluster storage, naming, and duplicate detection
- **Features**: 
  - Intelligent naming based on extracted entities
  - Duplicate cluster prevention
  - Database persistence

### 3. Cluster Scheduler (`src/clustering/cluster_scheduler.py`)
- **Purpose**: Orchestrates periodic clustering runs
- **Modes**: 
  - One-time execution
  - Daemon mode (runs every 30 minutes)

## The Clustering Process

### Step 1: Article Preparation
```
Raw Articles → Entity Extraction → Ready for Clustering
```
Articles must have:
- Cleaned content (from web scraping)
- Extracted entities (APT groups, malware, CVEs, etc.)
- Published within the time window (default: 72 hours)

### Step 2: Embedding Generation
```
Article Text → AI Model → 768-dimensional Vector
```
The AI model converts each article into a mathematical representation (embedding) that captures its meaning:
- Title is weighted 3x more than content
- Entities provide additional context
- Similar articles have similar vectors

### Step 3: Similarity Calculation
```
Vector A × Vector B = Similarity Score (0-1)
```
- **1.0** = Identical content
- **0.75+** = Highly similar (same incident)
- **0.5-0.75** = Related but different
- **<0.5** = Unrelated

### Step 4: Cluster Formation
Two algorithms work together:

**DBSCAN (Primary)**
- Finds dense regions of similar articles
- Good at identifying outliers
- Parameters: min_samples=2, eps=0.25

**Agglomerative (Fallback)**
- Hierarchical clustering
- Used when DBSCAN finds too few clusters
- Better for sparse data

### Step 5: Cluster Validation
Clusters must pass quality checks:
- **Size**: 2-12 articles (configurable)
- **Time Window**: All articles within 72 hours
- **Coherence**: Average similarity ≥ 0.65
- **Uniqueness**: Not duplicate of existing cluster

### Step 6: Naming & Storage
```
Top Entities + Keywords → Cluster Name
```
Examples:
- "Lazarus Group - CVE-2023-1234 - Ransomware"
- "McLaren Health Care - ALPHV - Data Breach"

## Understanding Clusters

### Cluster Anatomy
Each cluster contains:
- **Primary Article**: Most representative (closest to center)
- **Secondary Articles**: Related coverage
- **Coherence Score**: How well articles fit together (0.65-1.0)
- **Entities**: Shared IOCs, threat actors, vulnerabilities

### Quality Indicators
Good clusters have:
- High coherence (>0.8)
- Clear entity overlap
- Similar publication times
- Consistent topic focus

## Running the System

### One-Time Clustering
```bash
# Process recent articles (last 72 hours)
python -m src.clustering.cluster_scheduler --once

# Process with extended window (2 weeks)
python -m src.clustering.cluster_scheduler --once --full

# Limit processing
python -m src.clustering.cluster_scheduler --once --limit 100
```

### Standalone Daemon Mode
```bash
# Run continuously (checks every 30 minutes)
python -m src.clustering.cluster_scheduler
```

### As Part of Full Pipeline (Recommended)
The clustering system is now integrated into the ThreatCluster background service:

```bash
# Run full pipeline in daemon mode
python -m src.main --daemon

# Or install as systemd service (production)
./scripts/install-service.sh
./scripts/threatcluster-ctl.sh start
```

When running as part of the full pipeline:
- Clustering runs automatically after entity extraction
- Default interval: 120 minutes (configurable)
- Integrated logging and monitoring
- Automatic restart on failure

### View Results
```bash
# Generate cluster analysis report
python -m tests.test_cluster_report

# View logs from background service
./scripts/threatcluster-ctl.sh logs
tail -f logs/threatcluster_daemon_$(date +%Y%m%d).log
```

## Configuration

Edit `config/config.yaml`:

```yaml
clustering:
  # AI model for embeddings
  model_name: "sentence-transformers/all-mpnet-base-v2"
  
  # Similarity threshold (0.75 = 75% similar)
  similarity_threshold: 0.75
  
  # Cluster size limits
  min_cluster_size: 2
  max_cluster_size: 12
  
  # Time window for clustering (hours)
  time_window_hours: 72
  
  # Minimum cluster quality
  coherence_threshold: 0.65
  
  # Processing batch size
  batch_size: 50

# Scheduler configuration (for background service)
scheduler:
  components:
    semantic_clusterer:
      enabled: true
      interval_minutes: 120  # Run every 2 hours
      description: "Group related articles into clusters"
```

## Interpreting Results

### Cluster Report Example
```
CLUSTER: REvil - ALPHV - WannaCry
ID: 3 | Coherence: 0.836 | Articles: 3
Sources: The Register, Cybersecurity News, GB Hackers
Top Entities: Healthcare (industry), ALPHV (ransomware), Discovery (MITRE)

PRIMARY ARTICLE:
  Title: McLaren Health Care Data Breach Exposes 743,000 Individuals
  Source: GB Hackers
  Published: 2025-06-23 15:05:00
```

### What This Tells You:
1. **Topic**: Healthcare ransomware attack by ALPHV group
2. **Quality**: 0.836 coherence = highly related articles
3. **Coverage**: 3 different sources reporting
4. **Scope**: 743,000 individuals affected
5. **Threat**: Known ransomware group active

### Common Patterns

**High-Quality Security Clusters** (Coherence >0.85):
- Major breaches with multiple sources
- APT campaigns with consistent TTPs
- Vulnerability disclosures with CVEs

**Lower-Quality Clusters** (Coherence 0.65-0.75):
- General security topics
- Mixed incident types
- Broader industry trends

**Non-Security Content**:
- Technical discussions (e.g., NANOG hardware questions)
- Product announcements
- Should be filtered at feed level

## Troubleshooting

### No Clusters Forming
- Check if articles have extracted entities
- Verify time windows overlap
- Lower similarity threshold

### Too Many Small Clusters
- Increase similarity threshold
- Extend time window
- Check for duplicate feeds

### Poor Cluster Names
- Ensure entity extraction is working
- Check entity importance weights
- Verify high-value entities exist

## Database Schema

### Clusters Table
```sql
clusters_id              -- Unique identifier
clusters_name            -- Generated name (up to 500 chars)
clusters_summary         -- Primary article summary
clusters_coherence_score -- Quality metric (0-1)
clusters_is_active       -- Active/archived flag
clusters_created_at      -- Timestamp
```

### Cluster Articles Table
```sql
cluster_articles_cluster_id    -- Link to cluster
cluster_articles_clean_id      -- Link to article
cluster_articles_is_primary    -- Primary article flag
cluster_articles_similarity_score -- Article's fit (0-1)
```

## Performance Considerations

- **Embedding Generation**: ~0.5 seconds per article
- **Clustering**: O(n²) complexity for similarity matrix
- **Batch Size**: 50 articles optimal for memory/speed
- **Model Cache**: First run downloads 420MB model

## Future Enhancements

1. **Real-time Clustering**: Process articles as they arrive
2. **Cross-lingual Support**: Cluster articles in different languages
3. **Trend Detection**: Identify emerging threat patterns
4. **Automated Alerts**: Notify on high-priority clusters
5. **Cluster Merging**: Combine related clusters over time