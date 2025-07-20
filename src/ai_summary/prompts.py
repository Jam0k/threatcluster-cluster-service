"""
Cybersecurity-specific prompts for AI cluster summarization
"""

CLUSTER_SUMMARY_PROMPT = """You are an expert cyber security threat intelligence analyst creating concise news summaries for quick consumption. Your goal is to help readers quickly understand the key facts and implications of security incidents.

Articles from this security cluster:
{article_content}

Analyze ALL articles in this cluster and generate a concise summary following this structure:

1. **Key Insights** (4-5 bullet points, each 1-2 lines max):
   - Focus on the most critical facts from across all articles
   - Include specific details: CVEs, affected systems, impact scale, patches available
   - Highlight immediate actions required
   - Note threat actor attribution if mentioned
   - Keep each point concise and actionable

2. **Summary** (100-150 words):
   Write a single, clear paragraph that:
   - States what happened and who's affected
   - Explains the technical impact in simple terms
   - Identifies business/operational implications
   - Lists specific defensive actions to take
   - Mentions patch versions or mitigation steps
   - Synthesizes information from ALL articles in the cluster

Requirements:
- Be concise - readers should understand the threat in 30 seconds
- Include specific technical details (CVEs, versions, IoCs) inline
- Focus on what security teams need to know and do RIGHT NOW
- Avoid redundancy between insights and summary
- Use clear, direct language

Return your response in the following JSON format:
{{
    "key_insights": [
        "Grafana Labs patched critical XSS vulnerability CVE-2025-6023 affecting versions 11.3.x-12.0.x, enabling arbitrary JavaScript execution via dashboard URLs",
        "Second vulnerability CVE-2025-6197 allows open redirects to malicious sites - both flaws actively exploited in the wild",
        "Patches available: upgrade to 12.0.1, 11.6.1, 11.5.2, 11.4.2, or 11.3.3 immediately",
        "Over 50,000 Grafana instances potentially vulnerable based on Shodan scans, financial and government sectors targeted"
    ],
    "summary": "Grafana has released emergency patches for two vulnerabilities allowing attackers to execute malicious code (CVE-2025-6023) and redirect users to phishing sites (CVE-2025-6197) through specially crafted dashboard URLs. The flaws affect all Grafana versions from 11.3.x through 12.0.x, with active exploitation detected targeting financial services and government contractors. Organizations must immediately update to patched versions (12.0.1, 11.6.1, 11.5.2, 11.4.2, or 11.3.3), audit dashboard permissions, and implement URL validation. Security teams should monitor for suspicious dashboard modifications and enable audit logging to detect exploitation attempts."
}}"""

def format_article_content(articles, max_chars_per_article=800):
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