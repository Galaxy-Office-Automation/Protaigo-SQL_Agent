#!/usr/bin/env python3
"""
Detect exact long running queries in PostgreSQL.
Returns details of queries running longer than the threshold.
For use with Zabbix monitoring.
"""

import psycopg2
import json
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
SELECT 
    pid,
    usename AS username,
    datname AS database,
    client_addr::text,
    query_start::text,
    EXTRACT(EPOCH FROM (NOW() - query_start))::int AS duration_seconds,
    state,
    LEFT(query, 200) AS query_text
FROM pg_stat_activity
WHERE state = 'active'
  AND query NOT ILIKE '%%pg_stat_activity%%'
  AND NOW() - query_start > INTERVAL '%s seconds'
ORDER BY duration_seconds DESC;
"""


def get_long_queries():
    """Fetch long running queries from PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY % THRESHOLD_SECONDS)
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    queries = get_long_queries()
    print(json.dumps(queries, indent=2, default=str))
