#!/usr/bin/env python3
"""
Test script to actually send a cluster notification email
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.email_service.cluster_notification_service import ClusterNotificationService
from src.config.settings import settings
import asyncpg


async def send_test_notification():
    """Send a test notification email"""
    print("=== Sending Test Cluster Notification Email ===\n")
    
    try:
        # Initialize service
        service = ClusterNotificationService()
        print("✓ ClusterNotificationService initialized")
        
        # Connect to databases
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        print("✓ Connected to databases")
        
        # Find a cluster with followers and recent articles
        query = """
            SELECT 
                c.clusters_id, 
                c.clusters_name,
                COUNT(DISTINCT ucf.user_id) as followers,
                COUNT(DISTINCT ca.cluster_articles_clean_id) as articles
            FROM cluster_data.clusters c
            JOIN cluster_user.user_cluster_follows ucf ON c.clusters_id = ucf.cluster_id
            LEFT JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            WHERE ucf.notification_enabled = true
                AND c.clusters_is_active = true
            GROUP BY c.clusters_id, c.clusters_name
            HAVING COUNT(DISTINCT ucf.user_id) > 0 
                AND COUNT(DISTINCT ca.cluster_articles_clean_id) > 0
            ORDER BY COUNT(DISTINCT ucf.user_id) DESC
            LIMIT 5
        """
        
        clusters_with_followers = await user_conn.fetch(query)
        
        if not clusters_with_followers:
            print("✗ No clusters found with followers and articles")
            return
        
        print(f"✓ Found {len(clusters_with_followers)} clusters with followers:")
        for cluster in clusters_with_followers:
            print(f"  - Cluster {cluster['clusters_id']}: {cluster['clusters_name']} ({cluster['followers']} followers, {cluster['articles']} articles)")
        
        # Use the first cluster
        test_cluster_id = clusters_with_followers[0]['clusters_id']
        print(f"\n📧 Testing with cluster {test_cluster_id}: {clusters_with_followers[0]['clusters_name']}")
        
        # Get cluster details
        cluster_details = await service.get_cluster_details(cluster_conn, test_cluster_id)
        print(f"✓ Cluster details: {cluster_details['cluster_name']}")
        
        # Get followers
        followers = await service.get_cluster_followers(user_conn, test_cluster_id)
        print(f"✓ Found {len(followers)} followers with notifications enabled")
        
        # Get recent articles (last 7 days)
        since = datetime.now() - timedelta(days=7)
        articles = await service.get_new_articles_for_cluster(cluster_conn, test_cluster_id, since)
        print(f"✓ Found {len(articles)} articles in last 7 days")
        
        if not articles:
            print("⚠️  No recent articles found, creating mock articles for email test")
            # Create mock articles for testing
            articles = [
                {
                    'rss_feeds_raw_id': 1,
                    'rss_feeds_raw_title': 'Test Article: Critical Security Vulnerability Discovered',
                    'rss_feeds_raw_link': 'https://example.com/test-article-1',
                    'rss_feeds_raw_published_date': datetime.now() - timedelta(hours=2),
                    'rss_feeds_clean_content': 'This is a test article about a critical security vulnerability that requires immediate attention from security teams.',
                    'rss_feeds_name': 'Test Security Blog',
                    'added_to_cluster_at': datetime.now() - timedelta(hours=1)
                },
                {
                    'rss_feeds_raw_id': 2,
                    'rss_feeds_raw_title': 'Test Article: Patch Released for Critical CVE',
                    'rss_feeds_raw_link': 'https://example.com/test-article-2',
                    'rss_feeds_raw_published_date': datetime.now() - timedelta(hours=1),
                    'rss_feeds_clean_content': 'A patch has been released for the critical vulnerability. All users are advised to update immediately.',
                    'rss_feeds_name': 'Test Vendor Updates',
                    'added_to_cluster_at': datetime.now() - timedelta(minutes=30)
                }
            ]
        
        # Send notification to each follower
        for follower in followers:
            print(f"\n📧 Sending notification to: {follower['users_email']}")
            
            # Render email content
            html_content, text_content = service.render_notification_email(
                follower, 
                cluster_details, 
                articles[:5]  # Limit to first 5 articles
            )
            
            print(f"✓ Email content rendered ({len(html_content)} chars HTML, {len(text_content)} chars text)")
            
            # Send email
            subject = f"🔔 New updates: {cluster_details['cluster_name']}"
            success = await service.send_notification_email(
                follower['users_email'],
                subject,
                html_content,
                text_content
            )
            
            if success:
                print(f"✅ Email sent successfully to {follower['users_email']}")
                
                # Update last notified timestamp (optional for testing)
                update_last_notified = input("Update last_notified_at timestamp? (y/n): ").strip().lower()
                if update_last_notified == 'y':
                    await service.update_last_notified(user_conn, follower['users_id'], test_cluster_id)
                    print("✓ Updated last_notified_at timestamp")
                
            else:
                print(f"❌ Failed to send email to {follower['users_email']}")
        
        await cluster_conn.close()
        await user_conn.close()
        print("\n🎉 Test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Main function"""
    print("This script will send REAL notification emails to users!")
    print("Make sure you want to do this before proceeding.\n")
    
    confirm = input("Do you want to send test notification emails? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Test cancelled")
        return
    
    await send_test_notification()


if __name__ == "__main__":
    asyncio.run(main())