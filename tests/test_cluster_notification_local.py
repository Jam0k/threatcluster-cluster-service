#!/usr/bin/env python3
"""
Local test script for cluster notification service with correct database schema
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
import json

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config.settings import settings
from src.email_service.cluster_notification_service import ClusterNotificationService
import asyncpg


async def test_database_schema():
    """Test database connections and schema"""
    print("=== Database Schema Test ===")
    
    try:
        # Test cluster database connection
        cluster_conn = await asyncpg.connect(settings.database_url)
        print("✓ Connected to cluster database")
        
        # Test user database connection
        user_conn = await asyncpg.connect(settings.user_database_url)
        print("✓ Connected to user database")
        
        # Check if we have any clusters
        clusters = await cluster_conn.fetch("SELECT clusters_id, clusters_name FROM cluster_data.clusters WHERE clusters_is_active = true LIMIT 5")
        print(f"✓ Found {len(clusters)} active clusters")
        for cluster in clusters:
            print(f"  - Cluster {cluster['clusters_id']}: {cluster['clusters_name']}")
        
        # Check if we have any user follows
        follows = await user_conn.fetch("SELECT cluster_id, COUNT(*) as followers FROM cluster_user.user_cluster_follows WHERE notification_enabled = true GROUP BY cluster_id LIMIT 5")
        print(f"✓ Found {len(follows)} clusters with followers")
        for follow in follows:
            print(f"  - Cluster {follow['cluster_id']}: {follow['followers']} followers")
        
        await cluster_conn.close()
        await user_conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        return False


async def test_corrected_queries():
    """Test the corrected SQL queries"""
    print("\n=== Corrected SQL Queries Test ===")
    
    try:
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        
        # Test get_cluster_details with correct column names
        cluster_details_query = """
            SELECT 
                c.clusters_id as cluster_id,
                c.clusters_name as cluster_name,
                c.clusters_created_at as cluster_created_at,
                COUNT(DISTINCT ca.cluster_articles_clean_id) as total_articles
            FROM cluster_data.clusters c
            LEFT JOIN cluster_data.cluster_articles ca ON c.clusters_id = ca.cluster_articles_cluster_id
            WHERE c.clusters_id = $1
            GROUP BY c.clusters_id, c.clusters_name, c.clusters_created_at
        """
        
        # Test with first available cluster
        clusters = await cluster_conn.fetch("SELECT clusters_id FROM cluster_data.clusters WHERE clusters_is_active = true LIMIT 1")
        if clusters:
            cluster_id = clusters[0]['clusters_id']
            cluster_details = await cluster_conn.fetchrow(cluster_details_query, cluster_id)
            print(f"✓ get_cluster_details query works for cluster {cluster_id}")
            print(f"  - Name: {cluster_details['cluster_name']}")
            print(f"  - Articles: {cluster_details['total_articles']}")
        
        # Test get_new_articles_for_cluster with correct column names
        new_articles_query = """
            SELECT 
                rfr.rss_feeds_raw_id,
                rfr.rss_feeds_raw_xml->>'title' as rss_feeds_raw_title,
                rfr.rss_feeds_raw_xml->>'link' as rss_feeds_raw_link,
                rfr.rss_feeds_raw_published_date,
                rfc.rss_feeds_clean_content,
                rf.rss_feeds_name,
                ca.cluster_articles_added_at as added_to_cluster_at
            FROM cluster_data.cluster_articles ca
            JOIN cluster_data.rss_feeds_clean rfc ON ca.cluster_articles_clean_id = rfc.rss_feeds_clean_id
            JOIN cluster_data.rss_feeds_raw rfr ON rfc.rss_feeds_clean_raw_id = rfr.rss_feeds_raw_id
            JOIN cluster_data.rss_feeds rf ON rfr.rss_feeds_raw_feed_id = rf.rss_feeds_id
            WHERE ca.cluster_articles_cluster_id = $1
                AND ca.cluster_articles_added_at > $2
            ORDER BY ca.cluster_articles_added_at DESC
            LIMIT 10
        """
        
        if clusters:
            since = datetime.now() - timedelta(days=7)  # Look back 7 days (timezone-naive to match DB)
            articles = await cluster_conn.fetch(new_articles_query, cluster_id, since)
            print(f"✓ get_new_articles_for_cluster query works for cluster {cluster_id}")
            print(f"  - Found {len(articles)} articles in last 7 days")
            for article in articles[:2]:  # Show first 2
                print(f"    - {article['rss_feeds_raw_title'][:60]}...")
        
        # Test get_cluster_followers
        followers_query = """
            SELECT 
                u.users_id,
                u.users_email,
                u.users_name,
                ucf.last_notified_at
            FROM cluster_user.user_cluster_follows ucf
            JOIN cluster_user.users u ON ucf.user_id = u.users_id
            WHERE ucf.cluster_id = $1
                AND ucf.notification_enabled = true
                AND u.users_is_active = true
                AND u.users_email IS NOT NULL
        """
        
        if clusters:
            followers = await user_conn.fetch(followers_query, cluster_id)
            print(f"✓ get_cluster_followers query works for cluster {cluster_id}")
            print(f"  - Found {len(followers)} followers with notifications enabled")
        
        await cluster_conn.close()
        await user_conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Query test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_notification_service():
    """Test the notification service with corrected queries"""
    print("\n=== Notification Service Test ===")
    
    try:
        service = ClusterNotificationService()
        print("✓ ClusterNotificationService initialized")
        
        # Test database connections
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        
        # Get a test cluster
        clusters = await cluster_conn.fetch("SELECT clusters_id FROM cluster_data.clusters WHERE clusters_is_active = true LIMIT 1")
        if not clusters:
            print("✗ No active clusters found")
            return False
        
        cluster_id = clusters[0]['clusters_id']
        
        # Test individual methods
        cluster_details = await service.get_cluster_details(cluster_conn, cluster_id)
        print(f"✓ get_cluster_details works: {cluster_details['cluster_name'] if cluster_details else 'None'}")
        
        followers = await service.get_cluster_followers(user_conn, cluster_id)
        print(f"✓ get_cluster_followers works: {len(followers)} followers")
        
        since = datetime.now() - timedelta(days=7)  # Use timezone-naive to match DB
        # Get a test user ID from followers
        test_user_id = followers[0]['users_id'] if followers else 'test-user'
        articles = await service.get_new_articles_for_cluster(cluster_conn, user_conn, cluster_id, test_user_id, since)
        print(f"✓ get_new_articles_for_cluster works: {len(articles)} articles")
        
        await cluster_conn.close()
        await user_conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Notification service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_send_real_notifications():
    """Send real notification emails to users with followers"""
    print("\n=== Sending Real Cluster Notification Emails ===")
    
    # Ask for confirmation
    confirm = input("Do you want to send REAL notification emails to users? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Email sending cancelled")
        return
    
    try:
        service = ClusterNotificationService()
        cluster_conn = await asyncpg.connect(settings.database_url)
        user_conn = await asyncpg.connect(settings.user_database_url)
        
        # Find clusters with followers
        query = """
            SELECT DISTINCT cluster_id 
            FROM cluster_user.user_cluster_follows 
            WHERE notification_enabled = true
            ORDER BY cluster_id
        """
        
        rows = await user_conn.fetch(query)
        cluster_ids = [row['cluster_id'] for row in rows]
        
        print(f"✓ Found {len(cluster_ids)} clusters with followers")
        
        total_sent = 0
        
        for cluster_id in cluster_ids:
            print(f"\n📧 Processing cluster {cluster_id}...")
            
            # Get cluster details
            cluster = await service.get_cluster_details(cluster_conn, cluster_id)
            if not cluster:
                print(f"  ⚠️  Cluster {cluster_id} not found, skipping")
                continue
            
            # Get followers
            followers = await service.get_cluster_followers(user_conn, cluster_id)
            if not followers:
                print(f"  ⚠️  No followers for cluster {cluster_id}, skipping")
                continue
            
            print(f"  ✓ Cluster: {cluster['cluster_name']}")
            print(f"  ✓ Followers: {len(followers)}")
            
            # Process each follower
            for follower in followers:
                try:
                    # Get new articles since last notification
                    last_notified = follower.get('last_notified_at') or datetime.now() - timedelta(days=7)
                    new_articles = await service.get_new_articles_for_cluster(
                        cluster_conn,
                        user_conn,
                        cluster_id,
                        follower['users_id'],
                        last_notified
                    )
                    
                    if not new_articles:
                        print(f"    ⚠️  No new articles for {follower['users_email']}, skipping")
                        continue
                    
                    print(f"    📧 Sending to {follower['users_email']} ({len(new_articles)} new articles)")
                    
                    # Render email
                    html_content, text_content = service.render_notification_email(
                        follower,
                        cluster,
                        new_articles
                    )
                    
                    # Send email
                    subject = f"🔔 New updates: {cluster['cluster_name']}"
                    success = await service.send_notification_email(
                        follower['users_email'],
                        subject,
                        html_content,
                        text_content
                    )
                    
                    if success:
                        print(f"    ✅ Email sent successfully to {follower['users_email']}")
                        
                        # Update last notified timestamp
                        await service.update_last_notified(
                            user_conn,
                            follower['users_id'],
                            cluster_id
                        )
                        
                        # Record notification history
                        await service.record_notification(
                            user_conn,
                            follower['users_id'],
                            cluster_id,
                            new_articles[0]['rss_feeds_raw_id'],
                            'sent'
                        )
                        
                        total_sent += 1
                        
                    else:
                        print(f"    ❌ Failed to send email to {follower['users_email']}")
                        
                        # Record failure
                        await service.record_notification(
                            user_conn,
                            follower['users_id'],
                            cluster_id,
                            new_articles[0]['rss_feeds_raw_id'],
                            'failed',
                            'Email send failed'
                        )
                    
                    # asyncpg auto-commits, no need for explicit commit
                    
                except Exception as e:
                    print(f"    ❌ Error processing {follower['users_email']}: {e}")
        
        await cluster_conn.close()
        await user_conn.close()
        
        print(f"\n🎉 Notification sending complete! Sent {total_sent} emails total.")
        
    except Exception as e:
        print(f"❌ Error sending notifications: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Run all local tests"""
    print("=== ThreatCluster Cluster Notification Local Test ===\n")
    
    # Test 1: Database schema
    schema_ok = await test_database_schema()
    if not schema_ok:
        return
    
    # Test 2: Corrected queries
    queries_ok = await test_corrected_queries()
    if not queries_ok:
        return
    
    # Test 3: Notification service
    service_ok = await test_notification_service()
    if not service_ok:
        return
    
    print("\n=== All Local Tests Passed! ===")
    print("The notification service should work in production now.")
    
    # Test 4: Send real notifications (optional)
    send_emails = input("\nDo you want to send real notification emails? (y/n): ").strip().lower()
    if send_emails == 'y':
        await test_send_real_notifications()


if __name__ == "__main__":
    asyncio.run(main())