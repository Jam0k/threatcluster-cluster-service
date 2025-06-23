#!/usr/bin/env python3
"""
Simple PostgreSQL database connection test.

This script tests the connection to the PostgreSQL database configured in the .env file.
It uses psycopg2 directly for a basic connection test without any ORM overhead.

Usage:
    Run from the project root directory (cluster-service):
    $ python -m tests.test_db
    
    Or using pytest:
    $ pytest tests/test_db.py
    
    Note: Running this script directly from other directories will fail due to import paths.
    Always run from the cluster-service directory to ensure proper module resolution.
"""
import psycopg2
import sys
from src.config.settings import settings


def test_db_connection():
    """Test PostgreSQL database connection using settings from .env file."""
    try:
        # Attempt to connect to the database using the connection URL from settings
        conn = psycopg2.connect(settings.database_url)
        cursor = conn.cursor()
        
        # Query the PostgreSQL version to verify the connection is working
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✓ Successfully connected to PostgreSQL")
        print(f"  Database version: {version.split(',')[0]}")
        
        # Query the current database name to confirm we're connected to the right database
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"  Connected to database: {db_name}")
        
        # Clean up - close cursor and connection
        cursor.close()
        conn.close()
        print("\n✓ Database connection test passed!")
        return True
        
    except psycopg2.Error as e:
        # Handle database-specific errors (connection failures, auth issues, etc.)
        print(f"✗ Database connection failed: {e}")
        return False
    except Exception as e:
        # Handle any other unexpected errors (missing config, import errors, etc.)
        print(f"✗ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    # Run the test and exit with appropriate code (0 for success, 1 for failure)
    success = test_db_connection()
    sys.exit(0 if success else 1)