#!/usr/bin/env python3
"""
Count long running queries in PostgreSQL.
Returns a single integer for Zabbix monitoring.
"""

import psycopg2
import sys
import os

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}

# Threshold in seconds (queries running longer than this)
THRESHOLD_SECONDS = int(os.environ.get("LONG_QUERY_THRESHOLD", 5))

SQL_QUERY = """
SELECT COUNT(*)
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT ILIKE '%%pg_stat_activity%%'
  AND NOW() - query_start > INTERVAL '%s seconds';
"""


def count_long_queries():
    """Count long running queries in PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY % THRESHOLD_SECONDS)
        
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return -1


if __name__ == "__main__":
    count = count_long_queries()
    print(count)