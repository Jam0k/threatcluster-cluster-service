#!/usr/bin/env python3
"""
Test script to manually trigger email sending
"""
import asyncio
import sys
from src.email_service.email_service import EmailService

async def test_email():
    """Send test email to specific user or all users"""
    email_service = EmailService()
    
    # Option 1: Send to all subscribed users
    await email_service.send_daily_bulletin()
    
    # Option 2: Send to specific email (uncomment to use)
    # You would need to modify the EmailService class to support this
    # await email_service.send_test_email("james@threatcluster.io")

if __name__ == "__main__":
    asyncio.run(test_email())