#!/usr/bin/env python3
"""
ThreatCluster Interactive CLI

Interactive menu-driven interface for controlling ThreatCluster components.

Usage:
    python -m src.main [--no-clear] [--debug]
    
Options:
    --no-clear  Don't clear screen between operations
    --debug     Enable debug mode with verbose logging
"""
import os
import sys
import time
import asyncio
import threading
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2
import structlog
from colorama import init, Fore, Style

from src.config.settings import settings
from src.feeds.rss_fetcher import RSSFeedFetcher
from src.scraper.article_scraper import ArticleScraper
from src.entity_extraction.entity_extractor import EntityExtractor
from src.clustering.semantic_clusterer import SemanticClusterer
from src.clustering.cluster_manager import ClusterManager
from src.ranking.article_ranker import ArticleRanker

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Set up log file
LOG_DIR = Path(settings.base_dir) / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"threatcluster_{datetime.now().strftime('%Y%m%d')}.log"

# Configure file logging
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)

# Add file handler to root logger
logging.root.addHandler(file_handler)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ThreatClusterCLI:
    """Interactive CLI for ThreatCluster."""
    
    def __init__(self, no_clear=False, debug=False):
        """Initialize the CLI."""
        self.running = False
        self.continuous_thread = None
        self.no_clear = no_clear
        self.debug = debug
        self.pipeline_results = {}  # Track results from pipeline runs
        
        logger.info("cli_initialized", no_clear=no_clear, debug=debug, log_file=str(LOG_FILE))
        
        print(f"\n{Fore.GREEN}ThreatCluster CLI initialized{Style.RESET_ALL}")
        print(f"Log file: {LOG_FILE}")
        if self.debug:
            print(f"{Fore.YELLOW}Debug mode enabled - verbose logging active{Style.RESET_ALL}")
        if self.no_clear:
            print(f"{Fore.YELLOW}Screen clearing disabled{Style.RESET_ALL}")
        
        self.components = {
            'fetcher': RSSFeedFetcher(),
            'scraper': ArticleScraper(),
            'extractor': EntityExtractor(),
            'clusterer': SemanticClusterer(),
            'cluster_manager': ClusterManager(),
            'ranker': ArticleRanker()
        }
    
    def clear_screen(self):
        """Clear the terminal screen."""
        if not self.no_clear:
            os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_header(self):
        """Print the application header."""
        self.clear_screen()
        print(f"{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}||{' '*26}THREATCLUSTER CONTROL CENTER{' '*24}||")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        print()
    
    def print_menu(self):
        """Print the main menu."""
        print(f"{Fore.YELLOW}Main Menu:{Style.RESET_ALL}")
        print(f"{Fore.GREEN}1.{Style.RESET_ALL} Fetch RSS Feeds")
        print(f"{Fore.GREEN}2.{Style.RESET_ALL} Scrape Articles")
        print(f"{Fore.GREEN}3.{Style.RESET_ALL} Extract Entities")
        print(f"{Fore.GREEN}4.{Style.RESET_ALL} Cluster Articles")
        print(f"{Fore.GREEN}5.{Style.RESET_ALL} Rank Articles")
        print(f"{Fore.GREEN}6.{Style.RESET_ALL} Run Full Pipeline Once")
        print(f"{Fore.GREEN}7.{Style.RESET_ALL} Start Continuous Processing")
        print(f"{Fore.GREEN}8.{Style.RESET_ALL} Stop Continuous Processing")
        print(f"{Fore.GREEN}9.{Style.RESET_ALL} System Status")
        print(f"{Fore.GREEN}10.{Style.RESET_ALL} View Recent Results")
        print(f"{Fore.GREEN}11.{Style.RESET_ALL} View Recent Log Entries")
        print(f"{Fore.RED}0.{Style.RESET_ALL} Exit")
        print()
    
    def get_system_status(self):
        """Get current system status."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            status = {}
            
            # Get article counts
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM cluster_data.rss_feeds_raw WHERE rss_feeds_raw_created_at > NOW() - INTERVAL '24 hours') as feeds_24h,
                    (SELECT COUNT(*) FROM cluster_data.rss_feeds_clean WHERE rss_feeds_clean_created_at > NOW() - INTERVAL '24 hours') as articles_24h,
                    (SELECT COUNT(*) FROM cluster_data.rss_feeds_clean WHERE rss_feeds_clean_extracted_entities IS NOT NULL) as articles_with_entities,
                    (SELECT COUNT(*) FROM cluster_data.clusters WHERE clusters_is_active = true) as active_clusters,
                    (SELECT COUNT(*) FROM cluster_data.article_rankings) as ranked_articles
            """)
            
            result = cursor.fetchone()
            status['feeds_24h'] = result[0]
            status['articles_24h'] = result[1]
            status['articles_with_entities'] = result[2]
            status['active_clusters'] = result[3]
            status['ranked_articles'] = result[4]
            
            return status
            
        finally:
            cursor.close()
            conn.close()
    
    def display_status(self):
        """Display system status."""
        print(f"\n{Fore.YELLOW}System Status:{Style.RESET_ALL}")
        print("-" * 50)
        
        status = self.get_system_status()
        
        print(f"Feeds fetched (24h):      {Fore.CYAN}{status['feeds_24h']}{Style.RESET_ALL}")
        print(f"Articles scraped (24h):   {Fore.CYAN}{status['articles_24h']}{Style.RESET_ALL}")
        print(f"Articles with entities:   {Fore.CYAN}{status['articles_with_entities']}{Style.RESET_ALL}")
        print(f"Active clusters:          {Fore.CYAN}{status['active_clusters']}{Style.RESET_ALL}")
        print(f"Ranked articles:          {Fore.CYAN}{status['ranked_articles']}{Style.RESET_ALL}")
        
        if self.continuous_thread and self.continuous_thread.is_alive():
            print(f"\nContinuous Processing:    {Fore.GREEN}RUNNING{Style.RESET_ALL}")
        else:
            print(f"\nContinuous Processing:    {Fore.RED}STOPPED{Style.RESET_ALL}")
    
    def run_component(self, component_name, description):
        """Run a single component."""
        print(f"\n{Fore.YELLOW}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Running {description}...{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}{'='*60}{Style.RESET_ALL}")
        start_time = time.time()
        
        try:
            if component_name == 'fetcher':
                print(f"\n{Fore.CYAN}Fetching RSS feeds from configured sources...{Style.RESET_ALL}")
                print(f"Time window: Last {settings.app_config.get('pipeline', {}).get('time_window_hours', 72)} hours")
                print(f"Applying security keyword filtering...")
                
                result = self.components['fetcher'].process_all_feeds()
                
                print(f"\n{Fore.GREEN}✓ RESULTS:{Style.RESET_ALL}")
                print(f"  - Feeds processed: {result['feeds_processed']}")
                print(f"  - Total articles fetched: {result.get('articles_fetched', 0)}")
                print(f"  - Articles stored: {result.get('articles_stored', 0)}")
                print(f"  - Articles filtered out: {result.get('articles_filtered', 0)}")
                print(f"  - Duplicates skipped: {result.get('duplicates_skipped', 0)}")
                print(f"  - Articles too old: {result.get('articles_too_old', 0)}")
                if result.get('errors'):
                    print(f"  - Errors: {len(result['errors'])}")
            
            elif component_name == 'scraper':
                print(f"\n{Fore.CYAN}Scraping full article content from URLs...{Style.RESET_ALL}")
                print(f"Processing batch size: 50 articles")
                print(f"Rate limiting enabled to avoid blocking...")
                
                result = self.components['scraper'].process_batch(limit=50)
                
                print(f"\n{Fore.GREEN}✓ RESULTS:{Style.RESET_ALL}")
                print(f"  - Articles attempted: {result.get('articles_attempted', 0)}")
                print(f"  - Successfully scraped: {result.get('articles_success', 0)}")
                print(f"  - Failed scrapes: {result.get('articles_failed', 0)}")
                print(f"  - Fallback to RSS content: {result.get('content_fallback', 0)}")
                print(f"  - Average content length: {result.get('avg_content_length', 'N/A')} chars")
                print(f"  - Average chars removed: {result.get('avg_chars_removed', 0)}")
            
            elif component_name == 'extractor':
                print(f"\n{Fore.CYAN}Extracting security entities from articles...{Style.RESET_ALL}")
                print(f"Processing batch size: 50 articles")
                print(f"Entity categories: CVEs, IPs, domains, malware, APT groups, etc.")
                
                result = self.components['extractor'].process_batch(limit=50)
                
                print(f"\n{Fore.GREEN}✓ RESULTS:{Style.RESET_ALL}")
                print(f"  - Articles processed: {result.get('articles_processed', 0)}")
                print(f"  - Total entities extracted: {result.get('entities_extracted', 0)}")
                print(f"  - New entities discovered: {result.get('new_entities_discovered', 0)}")
                print(f"  - Extraction errors: {result.get('extraction_errors', 0)}")
                if result.get('entity_categories'):
                    print(f"  - Entity types found:")
                    for entity_type, count in result['entity_categories'].items():
                        print(f"    • {entity_type}: {count}")
            
            elif component_name == 'clusterer':
                print(f"\n{Fore.CYAN}Creating semantic clusters of related articles...{Style.RESET_ALL}")
                print(f"Time window: Last 72 hours")
                print(f"Using AI model: sentence-transformers/all-mpnet-base-v2")
                print(f"Similarity threshold: {settings.app_config.get('clustering', {}).get('similarity_threshold', 0.75)}")
                
                # Get unclustered articles
                articles = self.components['clusterer'].get_unclustered_articles(time_window_hours=72)
                if articles:
                    _, cluster_data = self.components['clusterer'].process_batch(articles)
                    
                    # Store clusters in database
                    if cluster_data:
                        stats = self.components['cluster_manager'].process_clusters(cluster_data)
                        result = {
                            'articles_processed': len(articles),
                            'clusters_created': stats.get('clusters_created', 0),
                            'articles_clustered': stats.get('articles_assigned', 0),
                            'duplicate_clusters_prevented': stats.get('duplicate_clusters_prevented', 0)
                        }
                    else:
                        result = {
                            'articles_processed': len(articles),
                            'clusters_created': 0,
                            'articles_clustered': 0,
                            'duplicate_clusters_prevented': 0
                        }
                    clusters = cluster_data
                else:
                    result = {
                        'articles_processed': 0,
                        'clusters_created': 0,
                        'articles_clustered': 0,
                        'duplicate_clusters_prevented': 0
                    }
                    clusters = []
                
                print(f"\n{Fore.GREEN}✓ RESULTS:{Style.RESET_ALL}")
                print(f"  - Articles analyzed: {result.get('articles_processed', 0)}")
                print(f"  - New clusters created: {result.get('clusters_created', 0)}")
                print(f"  - Articles clustered: {result.get('articles_clustered', 0)}")
                print(f"  - Duplicate clusters prevented: {result.get('duplicate_clusters_prevented', 0)}")
                if clusters:
                    print(f"  - New cluster topics:")
                    for cluster in clusters[:5]:
                        # Get the primary article title as cluster name
                        primary_idx = cluster.get('primary_article_idx', 0)
                        primary_article = cluster['articles'][primary_idx] if primary_idx < len(cluster['articles']) else cluster['articles'][0]
                        title_data = primary_article.get('rss_feeds_clean_title', {})
                        title = title_data.get('title', 'Unnamed cluster') if isinstance(title_data, dict) else 'Unnamed cluster'
                        print(f"    • {title[:60]}...")
            
            elif component_name == 'ranker':
                print(f"\n{Fore.CYAN}Ranking articles by importance...{Style.RESET_ALL}")
                print(f"Time window: Last 72 hours")
                print(f"Scoring factors: recency (20%), source credibility (30%), entities (30%), keywords (20%)")
                
                result = self.components['ranker'].rank_articles(time_window_hours=72)
                
                print(f"\n{Fore.GREEN}✓ RESULTS:{Style.RESET_ALL}")
                print(f"  - Articles ranked: {result.get('articles_ranked', 0)}")
                print(f"  - Average score: {result.get('avg_score', 0):.1f}")
                print(f"  - High priority (score >= 70): {result.get('high_priority_count', 0)}")
                print(f"  - Medium priority (50-69): {result.get('medium_priority_count', 0)}")
                print(f"  - Low priority (< 50): {result.get('low_priority_count', 0)}")
                print(f"  - Ranking errors: {result.get('ranking_errors', 0)}")
            
            elapsed = time.time() - start_time
            print(f"\n{Fore.GREEN}Completed in {elapsed:.1f} seconds{Style.RESET_ALL}")
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"\n{Fore.RED}✗ ERROR after {elapsed:.1f} seconds:{Style.RESET_ALL}")
            print(f"{Fore.RED}  {str(e)}{Style.RESET_ALL}")
            
            if self.debug:
                print(f"\n{Fore.RED}Full traceback:{Style.RESET_ALL}")
                print(traceback.format_exc())
            
            logger.error(f"component_error", 
                        component=component_name, 
                        error=str(e),
                        traceback=traceback.format_exc())
            
            # Store error in results
            if not hasattr(self, 'last_error'):
                self.last_error = {}
            self.last_error[component_name] = {
                'error': str(e),
                'traceback': traceback.format_exc(),
                'timestamp': datetime.now()
            }
    
    def run_full_pipeline(self):
        """Run the complete pipeline once."""
        print(f"\n{Fore.YELLOW}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}RUNNING FULL PIPELINE{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}{'='*80}{Style.RESET_ALL}")
        print(f"\nThis will execute all components in sequence:")
        print(f"1. Fetch RSS feeds")
        print(f"2. Scrape article content")
        print(f"3. Extract entities") 
        print(f"4. Create semantic clusters")
        print(f"5. Rank articles by importance")
        
        pipeline_start = time.time()
        self.pipeline_results = {
            'start_time': datetime.now(),
            'components': {},
            'success_count': 0,
            'error_count': 0
        }
        
        components = [
            ('fetcher', 'RSS Feed Fetcher'),
            ('scraper', 'Article Scraper'),
            ('extractor', 'Entity Extractor'),
            ('clusterer', 'Semantic Clusterer'),
            ('ranker', 'Article Ranker')
        ]
        
        for i, (component_name, description) in enumerate(components, 1):
            print(f"\n{Fore.CYAN}[Step {i}/5]{Style.RESET_ALL}")
            
            component_start = time.time()
            try:
                self.run_component(component_name, description)
                self.pipeline_results['components'][component_name] = {
                    'status': 'success',
                    'duration': time.time() - component_start
                }
                self.pipeline_results['success_count'] += 1
            except Exception as e:
                self.pipeline_results['components'][component_name] = {
                    'status': 'error',
                    'error': str(e),
                    'duration': time.time() - component_start
                }
                self.pipeline_results['error_count'] += 1
                logger.error("pipeline_component_failed", component=component_name, error=str(e))
            
            if component_name == 'fetcher' and i < len(components):
                wait_seconds = 30
                print(f"\n{Fore.YELLOW}Waiting {wait_seconds} seconds before scraping to avoid rate limiting...{Style.RESET_ALL}")
                for j in range(wait_seconds, 0, -5):
                    print(f"  {j} seconds remaining...", end='\r')
                    time.sleep(5)
                print(" " * 30, end='\r')  # Clear the line
        
        pipeline_elapsed = time.time() - pipeline_start
        self.pipeline_results['total_duration'] = pipeline_elapsed
        
        # Print summary
        print(f"\n{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}PIPELINE COMPLETE - Total time: {pipeline_elapsed:.1f} seconds{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        
        print(f"\n{Fore.YELLOW}Summary:{Style.RESET_ALL}")
        print(f"  Components succeeded: {self.pipeline_results['success_count']}/5")
        print(f"  Components failed: {self.pipeline_results['error_count']}/5")
        
        if self.pipeline_results['error_count'] > 0:
            print(f"\n{Fore.RED}Failed components:{Style.RESET_ALL}")
            for comp, result in self.pipeline_results['components'].items():
                if result['status'] == 'error':
                    print(f"  - {comp}: {result['error']}")
        
        logger.info("pipeline_completed", 
                   success_count=self.pipeline_results['success_count'],
                   error_count=self.pipeline_results['error_count'],
                   duration=pipeline_elapsed)
    
    def continuous_processing(self):
        """Run continuous processing in background."""
        logger.info("Starting continuous processing")
        self.running = True
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
                print(f"{Fore.MAGENTA}CONTINUOUS PROCESSING - Cycle #{cycle_count}{Style.RESET_ALL}")
                print(f"{Fore.MAGENTA}Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
                print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
                
                # Run pipeline
                self.run_full_pipeline()
                
                # Wait for next cycle (default 1 hour)
                wait_minutes = settings.app_config.get('scheduler', {}).get('components', {}).get('rss_fetcher', {}).get('interval_minutes', 60)
                next_run = datetime.now() + timedelta(minutes=wait_minutes)
                
                print(f"\n{Fore.MAGENTA}Cycle #{cycle_count} complete!{Style.RESET_ALL}")
                print(f"{Fore.MAGENTA}Next cycle will start at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
                print(f"{Fore.MAGENTA}Waiting {wait_minutes} minutes...{Style.RESET_ALL}")
                
                # Check every minute if we should stop
                for minute in range(wait_minutes):
                    if not self.running:
                        print(f"\n{Fore.YELLOW}Continuous processing stopped by user.{Style.RESET_ALL}")
                        break
                    time.sleep(60)
                    remaining = wait_minutes - minute - 1
                    if remaining > 0 and remaining % 10 == 0:
                        print(f"{Fore.MAGENTA}  {remaining} minutes until next cycle...{Style.RESET_ALL}")
                    
            except Exception as e:
                print(f"\n{Fore.RED}ERROR in continuous processing: {str(e)}{Style.RESET_ALL}")
                logger.error("continuous_processing_error", error=str(e))
                print(f"{Fore.YELLOW}Waiting 5 minutes before retry...{Style.RESET_ALL}")
                time.sleep(300)  # Wait 5 minutes on error
    
    def start_continuous(self):
        """Start continuous processing in background thread."""
        if self.continuous_thread and self.continuous_thread.is_alive():
            print(f"{Fore.YELLOW}Continuous processing is already running!{Style.RESET_ALL}")
            return
        
        self.continuous_thread = threading.Thread(target=self.continuous_processing, daemon=True)
        self.continuous_thread.start()
        print(f"{Fore.GREEN}✓ Started continuous processing{Style.RESET_ALL}")
    
    def stop_continuous(self):
        """Stop continuous processing."""
        if not self.continuous_thread or not self.continuous_thread.is_alive():
            print(f"{Fore.YELLOW}Continuous processing is not running!{Style.RESET_ALL}")
            return
        
        self.running = False
        print(f"{Fore.GREEN}✓ Stopping continuous processing...{Style.RESET_ALL}")
    
    def view_recent_results(self):
        """Display recent articles and clusters."""
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        try:
            print(f"\n{Fore.YELLOW}Recent High-Scoring Articles:{Style.RESET_ALL}")
            print("-" * 80)
            
            cursor.execute("""
                SELECT 
                    article_title,
                    article_rankings_score,
                    source_name,
                    clusters_name
                FROM cluster_data.articles_with_rankings
                WHERE article_rankings_score >= 60
                ORDER BY ranked_at DESC
                LIMIT 10
            """)
            
            for article in cursor:
                print(f"\n[{Fore.CYAN}{article[1]}{Style.RESET_ALL}] {article[0][:60]}...")
                print(f"   Source: {article[2]} | Cluster: {article[3] or 'Unclustered'}")
            
            print(f"\n{Fore.YELLOW}Recent Clusters:{Style.RESET_ALL}")
            print("-" * 80)
            
            cursor.execute("""
                SELECT 
                    clusters_name,
                    cluster_score,
                    article_count
                FROM cluster_data.cluster_rankings
                LIMIT 5
            """)
            
            for cluster in cursor:
                print(f"\n[{Fore.CYAN}{cluster[1]}{Style.RESET_ALL}] {cluster[0]}")
                print(f"   Articles: {cluster[2]}")
                
        finally:
            cursor.close()
            conn.close()
    
    def view_logs(self, lines=50):
        """View recent log entries."""
        print(f"\n{Fore.YELLOW}Recent Log Entries (last {lines} lines):{Style.RESET_ALL}")
        print("-" * 80)
        
        try:
            with open(LOG_FILE, 'r') as f:
                log_lines = f.readlines()
                
            # Get last N lines
            recent_lines = log_lines[-lines:] if len(log_lines) > lines else log_lines
            
            for line in recent_lines:
                line = line.strip()
                if 'ERROR' in line:
                    print(f"{Fore.RED}{line}{Style.RESET_ALL}")
                elif 'WARNING' in line:
                    print(f"{Fore.YELLOW}{line}{Style.RESET_ALL}")
                elif 'component_error' in line or 'failed' in line.lower():
                    print(f"{Fore.RED}{line}{Style.RESET_ALL}")
                else:
                    print(line)
                    
        except FileNotFoundError:
            print(f"{Fore.YELLOW}Log file not found: {LOG_FILE}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error reading log file: {str(e)}{Style.RESET_ALL}")
    
    def run(self):
        """Main interactive loop."""
        self.print_header()
        
        while True:
            self.print_menu()
            
            try:
                choice = input(f"{Fore.YELLOW}Enter your choice: {Style.RESET_ALL}")
                
                if choice == '0':
                    if self.continuous_thread and self.continuous_thread.is_alive():
                        self.stop_continuous()
                        time.sleep(2)
                    print(f"\n{Fore.CYAN}Goodbye!{Style.RESET_ALL}")
                    break
                
                elif choice == '1':
                    self.run_component('fetcher', 'RSS Feed Fetcher')
                
                elif choice == '2':
                    self.run_component('scraper', 'Article Scraper')
                
                elif choice == '3':
                    self.run_component('extractor', 'Entity Extractor')
                
                elif choice == '4':
                    self.run_component('clusterer', 'Semantic Clusterer')
                
                elif choice == '5':
                    self.run_component('ranker', 'Article Ranker')
                
                elif choice == '6':
                    self.run_full_pipeline()
                
                elif choice == '7':
                    self.start_continuous()
                
                elif choice == '8':
                    self.stop_continuous()
                
                elif choice == '9':
                    self.display_status()
                
                elif choice == '10':
                    self.view_recent_results()
                
                elif choice == '11':
                    self.view_logs()
                
                else:
                    print(f"{Fore.RED}Invalid choice. Please try again.{Style.RESET_ALL}")
                
                if choice != '0':
                    input(f"\n{Fore.YELLOW}Press Enter to continue...{Style.RESET_ALL}")
                    self.print_header()
                    
            except KeyboardInterrupt:
                print(f"\n\n{Fore.YELLOW}Interrupted. Use option 0 to exit properly.{Style.RESET_ALL}")
                time.sleep(1)
                self.print_header()
            except Exception as e:
                print(f"{Fore.RED}Error: {str(e)}{Style.RESET_ALL}")
                input(f"\n{Fore.YELLOW}Press Enter to continue...{Style.RESET_ALL}")
                self.print_header()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ThreatCluster Interactive CLI')
    parser.add_argument('--no-clear', action='store_true', 
                       help="Don't clear screen between operations")
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with verbose logging')
    args = parser.parse_args()
    
    try:
        # Suppress transformer warnings unless in debug mode
        if not args.debug:
            os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
            logging.getLogger('transformers').setLevel(logging.ERROR)
        else:
            logging.getLogger().setLevel(logging.DEBUG)
        
        cli = ThreatClusterCLI(no_clear=args.no_clear, debug=args.debug)
        cli.run()
        
    except Exception as e:
        print(f"{Fore.RED}Fatal error: {str(e)}{Style.RESET_ALL}")
        logger.error("fatal_error", error=str(e), traceback=traceback.format_exc())
        if args.debug:
            print(f"\n{Fore.RED}Traceback:{Style.RESET_ALL}")
            print(traceback.format_exc())
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())