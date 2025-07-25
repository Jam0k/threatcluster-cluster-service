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
   - Include inline citations using [1], [2], etc. to reference article numbers
   - Place citations immediately after the relevant facts

3. **TTPs (Tactics, Techniques, and Procedures)** (4-6 bullet points):
   Extract specific threat actor behaviors and methods:
   - Attack techniques used (e.g., spear phishing, lateral movement, C2 channels)
   - Tools and malware deployed
   - Persistence mechanisms
   - Data exfiltration methods
   - Map to MITRE ATT&CK framework with both ID and name (e.g., T1566.001: Spearphishing Attachment)
   - Be specific about how attacks were conducted
   - Include article citations at the end of each TTP

4. **Timeline** (chronological list of key events):
   Extract and order significant events with dates:
   - Include discovery dates, attack dates, patch releases, disclosures
   - Format: "YYYY-MM-DD: Event description [source articles]"
   - If only month/year known, use "YYYY-MM": or "YYYY:"
   - Include "Ongoing:" for continuous activities
   - Cite the article numbers for each event

5. **Sources**:
   List the article numbers that support each key finding:
   - Format: "Key insight or TTP" - Articles 1, 3, 5
   - Group related findings together
   - Ensure every major claim is cited

Requirements:
- Be concise - readers should understand the threat in 30 seconds
- Include specific technical details (CVEs, versions, IoCs) inline
- Focus on what security teams need to know and do RIGHT NOW
- Avoid redundancy between sections
- Use clear, direct language
- Cite article numbers for verification

Return your response in the following JSON format:
{{{{
    "key_insights": [
        "Grafana Labs patched critical XSS vulnerability CVE-2025-6023 affecting versions 11.3.x-12.0.x, enabling arbitrary JavaScript execution via dashboard URLs",
        "Second vulnerability CVE-2025-6197 allows open redirects to malicious sites - both flaws actively exploited in the wild",
        "Patches available: upgrade to 12.0.1, 11.6.1, 11.5.2, 11.4.2, or 11.3.3 immediately",
        "Over 50,000 Grafana instances potentially vulnerable based on Shodan scans, financial and government sectors targeted"
    ],
    "summary": "Grafana has released emergency patches for two vulnerabilities allowing attackers to execute malicious code (CVE-2025-6023) [1][3][5] and redirect users to phishing sites (CVE-2025-6197) [2][4] through specially crafted dashboard URLs. The flaws affect all Grafana versions from 11.3.x through 12.0.x, with active exploitation detected targeting financial services and government contractors [2][4][5]. Organizations must immediately update to patched versions (12.0.1, 11.6.1, 11.5.2, 11.4.2, or 11.3.3) [1][3], audit dashboard permissions, and implement URL validation. Security teams should monitor for suspicious dashboard modifications and enable audit logging to detect exploitation attempts [3].",
    "ttps": [
        "T1566.002: Spearphishing Link - Phishing links embedded in dashboard URLs to harvest credentials - Articles 2, 4",
        "T1190: Exploit Public-Facing Application - External remote services exploitation via crafted Grafana dashboard parameters - Articles 1, 3",
        "T1055: Process Injection - XSS injection to execute JavaScript in victim browsers - Article 1",
        "T1059.007: JavaScript - Command execution for browser-based attacks - Articles 1, 5",
        "T1547: Boot or Logon Autostart Execution - Persistence via modified dashboard configurations - Article 3"
    ],
    "timeline": [
        "2025-06-15: Initial vulnerability discovered by security researchers [3]",
        "2025-06-20: First exploitation attempts observed in the wild [2][4]",
        "2025-06-25: Grafana Labs notified of vulnerabilities [1]",
        "2025-07-01: Emergency patches released (versions 12.0.1, 11.6.1, etc.) [1][3]",
        "2025-07-05: Mass scanning activity detected targeting vulnerable instances [5]",
        "Ongoing: Active exploitation targeting financial and government sectors [2][4][5]"
    ],
    "sources": {{
        "CVE-2025-6023 XSS vulnerability": "Articles 1, 3, 5",
        "CVE-2025-6197 open redirect flaw": "Articles 2, 4",
        "Active exploitation in the wild": "Articles 2, 4, 5",
        "50,000 vulnerable instances": "Article 5",
        "Financial sector targeting": "Articles 2, 4"
    }}
}}}}"""

def format_article_content(articles, max_chars_per_article=800):
    """
    Format article content for the prompt, limiting each article to max_chars_per_article.
    
    Args:
        articles: List of article dictionaries with 'title' and 'content' fields
        max_chars_per_article: Maximum characters per article (default 800)
    
    Returns:
        Formatted string of article content
    """
    formatted_articles = []
    
    for i, article in enumerate(articles, 1):
        title = article.get('title', 'Untitled')
        content = article.get('content', '')
        source = article.get('source', 'Unknown Source')
        published_date = article.get('published_date', '')
        
        # Truncate content if necessary
        if len(content) > max_chars_per_article:
            content = content[:max_chars_per_article-3] + "..."
        
        # Format with article number, source, and date for better citation
        article_header = f"Article {i}: {title}"
        if source or published_date:
            article_header += f"\nSource: {source}"
            if published_date:
                article_header += f" | Published: {published_date}"
        
        formatted_articles.append(f"{article_header}\n{content}")
    
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