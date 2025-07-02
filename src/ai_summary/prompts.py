"""
Cybersecurity-specific prompts for AI cluster summarization
"""

CLUSTER_SUMMARY_PROMPT = """You are a cybersecurity intelligence analyst. Analyze the following security articles from a cluster and create three distinct briefs tailored for different audiences.

Articles from this security cluster:
{article_content}

Generate three briefs based on the above content:

1. **Executive Brief** (max 500 characters): A high-level summary for senior leadership, focusing on strategic implications, business risk, and policy considerations. Avoid technical jargon.

2. **Technical Brief** (max 500 characters): A detailed explanation for cybersecurity professionals, covering the vulnerability's nature, attack vectors, affected systems, and technical severity. Include CVE identifiers if mentioned.

3. **Remediation Brief** (max 500 characters): A practical, action-oriented guide for IT and security teams, outlining immediate and long-term mitigation steps, patching guidance, and policy updates.

Return your response in the following JSON format:
{{
    "executive_brief": "Your executive brief here",
    "technical_brief": "Your technical brief here",
    "remediation_brief": "Your remediation brief here"
}}

Ensure each brief is exactly as specified, clear, accurate, and relevant to its target audience."""

def format_article_content(articles, max_chars_per_article=500):
    """
    Format article content for the prompt, limiting each article to max_chars_per_article.
    
    Args:
        articles: List of article dictionaries with 'title' and 'content' fields
        max_chars_per_article: Maximum characters per article (default 500)
    
    Returns:
        Formatted string of article content
    """
    formatted_articles = []
    
    for i, article in enumerate(articles, 1):
        title = article.get('title', 'Untitled')
        content = article.get('content', '')
        
        # Truncate content if necessary
        if len(content) > max_chars_per_article:
            content = content[:max_chars_per_article-3] + "..."
        
        formatted_articles.append(f"Article {i}: {title}\n{content}")
    
    return "\n\n".join(formatted_articles)

def build_cluster_prompt(articles):
    """
    Build the complete prompt for cluster summarization.
    
    Args:
        articles: List of article dictionaries
    
    Returns:
        Complete formatted prompt string
    """
    article_content = format_article_content(articles)
    return CLUSTER_SUMMARY_PROMPT.format(article_content=article_content)