"""
Cybersecurity-specific prompts for AI cluster summarization
"""

CLUSTER_SUMMARY_PROMPT = """You are a professional security news reporter creating factual threat intelligence reports. Your goal is to synthesize information from multiple sources, incorporating direct quotes and specific data to create informative, objective summaries.

Articles from this security cluster:
{article_content}

Analyze ALL articles and create a rich summary that tells the complete story:

1. **Key Insights** (5-6 bullet points):
   - Lead with the most critical finding, including specific details
   - Incorporate a key quote or data point in each insight when available
   - Format: Clear statement followed by supporting evidence
   - Include CVEs, affected versions, patch numbers, impact scale
   - Each point should be 1-2 lines but information-dense

2. **Summary** (450-550 words):
   Write a comprehensive, detailed security news article. This must be a complete, professional article that could be published on a security news site.
   
   Required structure:
   
   **Opening** (100-120 words): Report the main news event with key facts. Include a relevant quote from an official source or expert. State specific numbers, dates, affected systems, and scope of impact.
   
   **Background and Context** (120-140 words): Report the backstory and relevant context. Include previous incidents, discovery timeline, or related events. Add 2-3 expert quotes providing factual context or technical background.
   
   **Technical Analysis** (180-200 words): Explain in detail:
   - Exactly how the attack/vulnerability works
   - What systems and versions are affected
   - The attack chain or exploitation process
   - Real-world impact with specific examples
   - Include 2-3 technical quotes from researchers or analysts
   
   **Industry Response** (120-140 words): Detail how the industry is responding:
   - Vendor patches and mitigation steps
   - Security community reactions
   - Defensive measures being implemented
   - Include quotes from vendors, CISOs, or security teams
   
   **Next Steps and Recommendations** (100-120 words): Report on concrete actions and official recommendations:
   - Specific patch versions and update procedures
   - Official guidance from vendors or security organizations
   - Documented defensive measures being implemented
   - End with a quote from a security official or vendor about specific actions
   
   Throughout: Use [1], [2] citations immediately after each fact. Incorporate 5-7 direct quotes naturally. Keep a neutral, factual tone - report the news, don't editorialize.

3. **TTPs (Tactics, Techniques, and Procedures)** (5-7 detailed points):
   Extract specific attacker behaviors with supporting evidence:
   - Include exact attack methods described in articles
   - Add relevant quotes about how attacks were conducted when available
   - Format: "MITRE ID: Technique Name - Detailed description [source]"
   - Include tools, malware names, C2 infrastructure details
   - Describe the complete attack chain when possible

4. **Timeline** (comprehensive chronological sequence):
   Build a detailed timeline incorporating all significant events:
   - Use exact dates when available, approximate when not
   - Include contextual details where relevant
   - Format: "Date: Event description [source]"
   - Show the progression from discovery through disclosure to exploitation
   - Include ongoing activities and future dates (patches, deadlines)

5. **Sources**:
   Map all major claims to their sources:
   - Group by type: primary findings, technical details, expert analysis
   - Include article numbers for all key facts
   - Format as nested groups for better organization

6. **Entities** (Extract ALL entities mentioned across all articles):
   Organize entities by category. Include every unique entity found:
   
   **Technical Indicators**:
   - domains: Full domain names (e.g., "malicious-site.com", "c2-server.ru")
   - ip_addresses: IPv4 and IPv6 addresses
   - file_hashes: MD5, SHA1, SHA256 hashes
   - cves: CVE identifiers (e.g., "CVE-2025-1234")
   
   **Threat Intelligence**:
   - apt_groups: APT group names (e.g., "APT28", "Lazarus Group", "Cozy Bear")
   - ransomware_groups: Ransomware operators (e.g., "LockBit", "BlackCat", "Conti")
   - malware_families: Malware names (e.g., "Emotet", "Cobalt Strike", "Mimikatz")
   - attack_types: Attack methods (e.g., "SQL Injection", "Buffer Overflow", "XSS")
   - mitre_attack: MITRE ATT&CK techniques (e.g., "T1566", "T1190")
   - security_standards: Standards mentioned (e.g., "ISO 27001", "NIST", "PCI DSS")
   - vulnerability_types: Vulnerability categories (e.g., "Remote Code Execution", "Privilege Escalation")
   
   **Business Intelligence**:
   - companies: Company names mentioned (e.g., "Microsoft", "Apple", "Google")
   - industry_sectors: Industries affected (e.g., "Healthcare", "Financial Services", "Energy")
   - security_vendors: Security company names (e.g., "CrowdStrike", "Palo Alto Networks", "Kaspersky")
   - government_agencies: Government entities (e.g., "CISA", "FBI", "NSA")
   - countries: Country names mentioned
   - platforms: Technologies/platforms (e.g., "Windows", "Linux", "AWS", "Azure")
   
   For each entity, provide just the name as a string. Extract entities from article titles, content, and quotes.

Requirements:
- Incorporate specific quotes and data points naturally
- Use specific names, numbers, versions, and technical details throughout
- Create a cohesive narrative while maintaining technical accuracy
- Every significant claim must be cited with [#]
- Balance multiple perspectives when articles disagree
- Include enough detail that readers understand both the threat and the response


Return your response in the following JSON format:
{{{{
    "key_insights": [
        "Grafana Labs patched critical XSS vulnerability CVE-2025-6023 affecting versions 11.3.x-12.0.x - 'allows attackers to execute arbitrary JavaScript in the context of authenticated users'",
        "Active exploitation detected with 'over 400% increase in scanning activity targeting Grafana instances' according to CyberDefense Labs",
        "52,847 vulnerable instances identified through Shodan scans, with financial services comprising 35% of exposed targets",
        "Second vulnerability CVE-2025-6197 enables credential harvesting - 'attackers are chaining this with phishing campaigns' per CISA advisory",
        "Emergency patches released: versions 12.0.1, 11.6.1, 11.5.2, 11.4.2, or 11.3.3 - Grafana urges 'immediate deployment'",
        "Public exploit code now available, 'significantly lowering the barrier for attackers' warns threat intelligence community"
    ],
    "summary": "[WRITE YOUR COMPLETE 450-550 WORD ARTICLE HERE - DO NOT USE THIS EXAMPLE TEXT]",
    "ttps": [
        "T1566.002: Spearphishing Link - Attackers embed malicious dashboard URLs in emails targeting internal users [2][4]",
        "T1190: Exploit Public-Facing Application - Direct exploitation via crafted API calls achieving 'full compromise in under 30 seconds' [1][3]",
        "T1059.007: JavaScript/JScript - XSS enables 'session hijacking and lateral movement through stolen tokens' [1][5]",
        "T1557: Adversary-in-the-Middle - Open redirect allows 'transparent credential interception' during redirects [2][4]",
        "T1053: Scheduled Task/Job - Persistence via 'modified Grafana alert rules executing periodic JavaScript' [3][5]",
        "T1105: Ingress Tool Transfer - Post-compromise downloading of 'credential stealers and network scanners' [4]",
        "T1003: OS Credential Dumping - Memory scraping tools deployed to 'harvest credentials from Grafana processes' [5]"
    ],
    "timeline": [
        "2025-06-10: Independent researcher discovers XSS vulnerability during security assessment [3]",
        "2025-06-15: Grafana internal team identifies second vulnerability during patch development [1]",
        "2025-06-20: First exploitation attempts detected by CyberDefense Labs honeypots [2]",
        "2025-06-22: Attack volume 'doubling every 24 hours' per threat intelligence [4]",
        "2025-06-25: Grafana Labs publicly announces vulnerabilities and releases patches [1][3]",
        "2025-06-26: CISA issues emergency directive giving agencies 48 hours to patch [4]",
        "2025-06-27: Public exploit code released on GitHub [5]",
        "Ongoing: Active exploitation with 'new variations emerging daily' [2][4][5]"
    ],
    "sources": {{
        "primary_findings": {{
            "CVE details and patches": "Articles 1, 3",
            "Exploitation evidence": "Articles 2, 4, 5",
            "Vulnerable instance count": "Article 5"
        }},
        "technical_details": {{
            "Attack methods": "Articles 1, 2, 5",
            "Persistence techniques": "Articles 3, 5"
        }},
        "expert_quotes": {{
            "Grafana Labs": "Article 1",
            "CyberDefense Labs": "Article 2",
            "CISA": "Article 4"
        }}
    }},
    "entities": {{
        "technical_indicators": {{
            "domains": [],
            "ip_addresses": [],
            "file_hashes": [],
            "cves": ["CVE-2025-6023", "CVE-2025-6197"]
        }},
        "threat_intelligence": {{
            "apt_groups": [],
            "ransomware_groups": [],
            "malware_families": [],
            "attack_types": ["Cross-Site Scripting", "Credential Harvesting", "Open Redirect"],
            "mitre_attack": ["T1566", "T1190", "T1059", "T1557", "T1053", "T1105", "T1003"],
            "security_standards": [],
            "vulnerability_types": ["Remote Code Execution", "Cross-Site Scripting", "Authentication Bypass"]
        }},
        "business_intelligence": {{
            "companies": ["Grafana Labs", "GitHub"],
            "industry_sectors": ["Financial Services", "Government"],
            "security_vendors": ["CyberDefense Labs", "Palo Alto Networks", "CrowdStrike"],
            "government_agencies": ["CISA"],
            "countries": [],
            "platforms": ["Grafana", "Linux", "Windows"]
        }}
    }}
}}}}"""

def format_article_content(articles, max_chars_per_article=1500):
    """
    Format article content for the prompt, including more content and extracting key quotes.
    
    Args:
        articles: List of article dictionaries with 'title' and 'content' fields
        max_chars_per_article: Maximum characters per article (default 1000, increased for richer content)
    
    Returns:
        Formatted string of article content
    """
    formatted_articles = []
    
    for i, article in enumerate(articles, 1):
        title = article.get('title', 'Untitled')
        content = article.get('content', '')
        source = article.get('source', 'Unknown Source')
        published_date = article.get('published_date', '')
        
        # Extract potential quotes (sentences with quotation marks)
        quotes = []
        if content:
            import re
            quote_pattern = r'"([^"]+)"'
            found_quotes = re.findall(quote_pattern, content)
            quotes = [q for q in found_quotes if len(q) > 30 and len(q) < 200][:2]
        
        # Truncate content if necessary but keep more
        if len(content) > max_chars_per_article:
            content = content[:max_chars_per_article-3] + "..."
        
        # Format with article number, source, and date for better citation
        article_header = f"Article {i}: {title}"
        if source or published_date:
            article_header += f"\nSource: {source}"
            if published_date:
                article_header += f" | Published: {published_date}"
        
        # Add any extracted quotes separately for easier reference
        if quotes:
            article_header += f"\nKey Quotes:"
            for quote in quotes:
                article_header += f'\n- "{quote}"'
        
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