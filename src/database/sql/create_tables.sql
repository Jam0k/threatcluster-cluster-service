-- ThreatCluster Database Schema
-- Initial schema creation for cybersecurity news clustering system

-- Create the cluster_data schema
CREATE SCHEMA IF NOT EXISTS cluster_data;

-- RSS Feed Sources
CREATE TABLE cluster_data.rss_feeds (
    rss_feeds_id SERIAL PRIMARY KEY,
    rss_feeds_url VARCHAR(500) NOT NULL UNIQUE,
    rss_feeds_name VARCHAR(200) NOT NULL,
    rss_feeds_category VARCHAR(50) NOT NULL, -- 'cybersecurity', 'general_news'
    rss_feeds_credibility INTEGER DEFAULT 50 CHECK (rss_feeds_credibility BETWEEN 1 AND 100),
    rss_feeds_created_at TIMESTAMP DEFAULT NOW(),
    rss_feeds_is_active BOOLEAN DEFAULT TRUE
);

-- Raw RSS Data
CREATE TABLE cluster_data.rss_feeds_raw (
    rss_feeds_raw_id SERIAL PRIMARY KEY,
    rss_feeds_raw_feed_id INTEGER REFERENCES cluster_data.rss_feeds(rss_feeds_id),
    rss_feeds_raw_xml JSONB NOT NULL, -- {title, link, description, pubDate}
    rss_feeds_raw_published_date TIMESTAMP NOT NULL,
    rss_feeds_raw_created_at TIMESTAMP DEFAULT NOW(),
    rss_feeds_raw_processed BOOLEAN DEFAULT FALSE
);

-- Clean Article Data
CREATE TABLE cluster_data.rss_feeds_clean (
    rss_feeds_clean_id SERIAL PRIMARY KEY,
    rss_feeds_clean_raw_id INTEGER REFERENCES cluster_data.rss_feeds_raw(rss_feeds_raw_id),
    rss_feeds_clean_title JSONB NOT NULL,
    rss_feeds_clean_content JSONB NOT NULL,
    rss_feeds_clean_images JSONB,
    rss_feeds_clean_extracted_entities JSONB,
    rss_feeds_clean_created_at TIMESTAMP DEFAULT NOW(),
    rss_feeds_clean_processed BOOLEAN DEFAULT FALSE,
    CONSTRAINT unique_raw_id UNIQUE (rss_feeds_clean_raw_id)
);

-- Entity Dictionary
CREATE TABLE cluster_data.entities (
    entities_id SERIAL PRIMARY KEY,
    entities_name VARCHAR(200) NOT NULL,
    entities_category VARCHAR(50) NOT NULL, -- 'apt_group', 'malware_family', 'cve', etc.
    entities_source VARCHAR(50) NOT NULL, -- 'manual', 'dynamic'
    entities_importance_weight INTEGER DEFAULT 50 CHECK (entities_importance_weight BETWEEN 1 AND 100),
    entities_added_on TIMESTAMP DEFAULT NOW(),
    UNIQUE(entities_name, entities_category)
);

-- Keyword Weights for Dynamic Scoring
CREATE TABLE cluster_data.keyword_weights (
    keyword_weights_id SERIAL PRIMARY KEY,
    keyword_weights_keyword VARCHAR(100) NOT NULL UNIQUE,
    keyword_weights_category VARCHAR(50) NOT NULL, -- 'severity', 'attack_type', 'asset_type'
    keyword_weights_weight INTEGER NOT NULL CHECK (keyword_weights_weight BETWEEN 1 AND 100),
    keyword_weights_created_at TIMESTAMP DEFAULT NOW()
);

-- Semantic Clusters
CREATE TABLE cluster_data.clusters (
    clusters_id SERIAL PRIMARY KEY,
    clusters_name VARCHAR(200),
    clusters_summary TEXT,
    clusters_coherence_score FLOAT CHECK (clusters_coherence_score BETWEEN 0 AND 1),
    clusters_created_at TIMESTAMP DEFAULT NOW(),
    clusters_is_active BOOLEAN DEFAULT TRUE
);

-- Article-Cluster Relationships
CREATE TABLE cluster_data.cluster_articles (
    cluster_articles_id SERIAL PRIMARY KEY,
    cluster_articles_cluster_id INTEGER REFERENCES cluster_data.clusters(clusters_id),
    cluster_articles_clean_id INTEGER REFERENCES cluster_data.rss_feeds_clean(rss_feeds_clean_id),
    cluster_articles_is_primary BOOLEAN DEFAULT FALSE, -- representative article for cluster
    cluster_articles_similarity_score FLOAT CHECK (cluster_articles_similarity_score BETWEEN 0 AND 1),
    cluster_articles_added_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cluster_articles_cluster_id, cluster_articles_clean_id)
);

-- Article Rankings
CREATE TABLE cluster_data.article_rankings (
    article_rankings_id SERIAL PRIMARY KEY,
    article_rankings_clean_id INTEGER REFERENCES cluster_data.rss_feeds_clean(rss_feeds_clean_id),
    article_rankings_cluster_id INTEGER REFERENCES cluster_data.clusters(clusters_id),
    article_rankings_score INTEGER CHECK (article_rankings_score BETWEEN 1 AND 100),
    article_rankings_factors JSONB, -- {recency, entity_importance, source_credibility, keyword_severity}
    article_rankings_ranked_at TIMESTAMP DEFAULT NOW()
);

-- Performance Indexes
CREATE INDEX idx_rss_feeds_is_active ON cluster_data.rss_feeds(rss_feeds_is_active);
CREATE INDEX idx_rss_feeds_category ON cluster_data.rss_feeds(rss_feeds_category);
CREATE INDEX idx_rss_feeds_raw_created_at ON cluster_data.rss_feeds_raw(rss_feeds_raw_created_at);
CREATE INDEX idx_rss_feeds_raw_processed ON cluster_data.rss_feeds_raw(rss_feeds_raw_processed);
CREATE INDEX idx_rss_feeds_raw_feed_id ON cluster_data.rss_feeds_raw(rss_feeds_raw_feed_id);
CREATE INDEX idx_rss_feeds_clean_created_at ON cluster_data.rss_feeds_clean(rss_feeds_clean_created_at);
CREATE INDEX idx_rss_feeds_clean_processed ON cluster_data.rss_feeds_clean(rss_feeds_clean_processed);
CREATE INDEX idx_rss_feeds_clean_raw_id ON cluster_data.rss_feeds_clean(rss_feeds_clean_raw_id);
CREATE INDEX idx_cluster_articles_cluster_id ON cluster_data.cluster_articles(cluster_articles_cluster_id);
CREATE INDEX idx_cluster_articles_clean_id ON cluster_data.cluster_articles(cluster_articles_clean_id);
CREATE INDEX idx_entities_name_category ON cluster_data.entities(entities_name, entities_category);
CREATE INDEX idx_entities_category ON cluster_data.entities(entities_category);
CREATE INDEX idx_entities_source ON cluster_data.entities(entities_source);
CREATE INDEX idx_clusters_is_active ON cluster_data.clusters(clusters_is_active);
CREATE INDEX idx_clusters_created_at ON cluster_data.clusters(clusters_created_at);
CREATE INDEX idx_article_rankings_score ON cluster_data.article_rankings(article_rankings_score DESC);
CREATE INDEX idx_article_rankings_clean_id ON cluster_data.article_rankings(article_rankings_clean_id);
CREATE INDEX idx_keyword_weights_category ON cluster_data.keyword_weights(keyword_weights_category);

-- Comments for documentation
COMMENT ON SCHEMA cluster_data IS 'Schema for ThreatCluster cybersecurity news clustering system';
COMMENT ON TABLE cluster_data.rss_feeds IS 'RSS feed sources with credibility ratings';
COMMENT ON TABLE cluster_data.rss_feeds_raw IS 'Raw XML data from RSS feeds';
COMMENT ON TABLE cluster_data.rss_feeds_clean IS 'Cleaned and processed article content';
COMMENT ON TABLE cluster_data.entities IS 'Dictionary of cybersecurity entities with importance weights';
COMMENT ON TABLE cluster_data.keyword_weights IS 'Keyword weights for severity and importance scoring';
COMMENT ON TABLE cluster_data.clusters IS 'Semantic clusters of related articles';
COMMENT ON TABLE cluster_data.cluster_articles IS 'Many-to-many relationship between clusters and articles';
COMMENT ON TABLE cluster_data.article_rankings IS 'Final ranking scores for articles and clusters';

-- Create a view for easy article querying
CREATE VIEW cluster_data.articles_with_rankings AS
SELECT 
    rfc.rss_feeds_clean_id,
    rfc.rss_feeds_clean_title,
    rfc.rss_feeds_clean_content,
    rfc.rss_feeds_clean_images,
    rfc.rss_feeds_clean_extracted_entities,
    rfc.rss_feeds_clean_created_at,
    rfr.rss_feeds_raw_published_date,
    rf.rss_feeds_name,
    rf.rss_feeds_credibility,
    ar.article_rankings_score,
    ar.article_rankings_factors,
    c.clusters_id,
    c.clusters_name,
    c.clusters_coherence_score
FROM cluster_data.rss_feeds_clean rfc
JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
LEFT JOIN cluster_data.article_rankings ar ON rfc.rss_feeds_clean_id = ar.article_rankings_clean_id
LEFT JOIN cluster_data.cluster_articles ca ON rfc.rss_feeds_clean_id = ca.cluster_articles_clean_id
LEFT JOIN cluster_data.clusters c ON ca.cluster_articles_cluster_id = c.clusters_id AND c.clusters_is_active = true;

COMMENT ON VIEW cluster_data.articles_with_rankings IS 'Consolidated view of articles with rankings and cluster information';