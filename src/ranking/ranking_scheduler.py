#!/usr/bin/env python3
"""
Ranking Scheduler

Schedules periodic article and cluster ranking updates.
Can be run as a daemon or triggered manually.
"""

import time
import signal
import sys
from datetime import datetime
from typing import Optional
import structlog

from src.config.settings import settings
from src.ranking.article_ranker import ArticleRanker


logger = structlog.get_logger(__name__)


class RankingScheduler:
    """Manages scheduled article and cluster ranking."""
    
    def __init__(self):
        """Initialize the scheduler."""
        self.config = settings.app_config.get('pipeline', {})
        self.ranking_config = settings.app_config.get('ranking', {})
        
        self.batch_size = self.config.get('processing_batch_size', 100)
        self.time_window_hours = self.config.get('time_window_hours', 72)
        
        self.running = False
        self.ranker = ArticleRanker()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Handle shutdown gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        sys.exit(0)
    
    def run_once(self, process_all: bool = False) -> dict:
        """Run a single ranking cycle."""
        logger.info("starting_ranking_cycle", process_all=process_all)
        start_time = time.time()
        
        try:
            # Run ranking
            stats = self.ranker.rank_articles(
                batch_size=self.batch_size,
                time_window_hours=None if process_all else self.time_window_hours,
                process_all=process_all
            )
            
            # Calculate processing time
            processing_time = time.time() - start_time
            stats['processing_time_seconds'] = round(processing_time, 2)
            
            logger.info("ranking_cycle_completed", **stats)
            return stats
            
        except Exception as e:
            logger.error("ranking_cycle_error", error=str(e))
            raise
    
    def run_daemon(self):
        """Run as a daemon, ranking articles periodically."""
        logger.info("starting_ranking_daemon",
                   check_interval_minutes=60)
        
        self.running = True
        
        while self.running:
            try:
                # Run ranking
                self.run_once(process_all=False)
                
                # Wait before next check (60 minutes)
                for _ in range(60):
                    if not self.running:
                        break
                    time.sleep(60)
                    
            except Exception as e:
                logger.error("daemon_error", error=str(e))
                # Continue running after errors
                time.sleep(300)  # Wait 5 minutes after error


def main():
    """Run the ranking scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Article Ranking Scheduler')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit')
    parser.add_argument('--all', action='store_true',
                       help='Process all unranked articles')
    
    args = parser.parse_args()
    
    scheduler = RankingScheduler()
    
    if args.once:
        # Run once
        stats = scheduler.run_once(process_all=args.all)
        
        print("\nRanking completed!")
        print(f"Articles ranked: {stats.get('articles_ranked', 0)}")
        print(f"Clusters ranked: {stats.get('clusters_ranked', 0)}")
        print(f"Average score: {stats.get('average_score', 0):.2f}")
        print(f"Processing time: {stats.get('processing_time_seconds', 0):.2f} seconds")
    else:
        # Run as daemon
        print("Starting ranking scheduler daemon")
        print("Checking for new articles every 60 minutes...")
        print("Press Ctrl+C to stop...")
        scheduler.run_daemon()


if __name__ == "__main__":
    main()