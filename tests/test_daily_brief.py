#!/usr/bin/env python3
"""
Test script for Daily Brief generation
"""
import asyncio
import sys
import os
from datetime import date

# Add the src directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ai_summary.daily_brief_service import DailyBriefService


async def test_daily_brief():
    """Test generating a daily brief"""
    print("Testing Daily Brief Generation...")
    
    try:
        service = DailyBriefService()
        
        # Generate today's brief
        print(f"Generating daily brief for {date.today()}...")
        result = await service.generate_daily_brief()
        
        print(f"\nResult: {result}")
        
        if result['status'] == 'success':
            print(f"✓ Successfully generated daily brief!")
            print(f"  Article ID: {result['article_id']}")
            print(f"  Severity Score: {result['severity_score']}")
            print(f"  Threats Analyzed: {result['threats_analyzed']}")
        elif result['status'] == 'exists':
            print("✓ Daily brief already exists for today")
        elif result['status'] == 'no_data':
            print("⚠ No threat data available for today")
        else:
            print(f"✗ Unexpected status: {result['status']}")
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("Daily Brief Test Script")
    print("=" * 50)
    asyncio.run(test_daily_brief())