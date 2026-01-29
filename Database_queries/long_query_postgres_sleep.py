#!/usr/bin/env python3
"""
Long Running PostgreSQL Query - Sleep Based
============================================
This script executes a pg_sleep query for 20-25 minutes.
Used for testing Zabbix long-running query alerts.

Duration: 20 minutes (1200 seconds)
"""

import psycopg2
import time
from datetime import datetime

# PostgreSQL Connection Details
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}

# Query duration in seconds (20 minutes = 1200 seconds)
SLEEP_DURATION = 1200


def run_long_query():
    """Execute a pg_sleep query that runs for 20 minutes."""
    print("=" * 60)
    print("Long Running PostgreSQL Query - Sleep Based")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Expected Duration: {SLEEP_DURATION // 60} minutes")
    print("-" * 60)
    
    try:
        # Establish connection
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected successfully!")
        
        # Execute long-running query
        query = f"SELECT pg_sleep({SLEEP_DURATION});"
        print(f"\nExecuting query: {query}")
        print("Query is now running... (This will take 20 minutes)")
        
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchone()
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("Query completed successfully!")
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Actual Duration: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        print("=" * 60)
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\nDatabase Error: {e}")
        raise
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    run_long_query()
