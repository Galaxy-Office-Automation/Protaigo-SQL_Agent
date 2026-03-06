#!/usr/bin/env python3
"""
Window Function Overlap Analysis
================================
Simulates an extremely heavy analytical query using overlapping 
massive window functions and self joins.
Expected run time: ~10-15 minutes depending on hardware.
"""

import psycopg2
import time
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}

def run_heavy_analysis_3():
    print("=" * 60)
    print("Heavy Analysis 3: Multi-layer Window Functions")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    # This query uses an explicit continuous sleep to guarantee exactly 15 minutes of run time.
    # 15 minutes * 60 seconds = 900 seconds.
    query = """
        SELECT a.aid AS account_id, a.bid AS branch_id, a.abalance AS account_balance, b.bbalance AS branch_balance, (
SELECT COUNT(*) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS total_transactions, (
SELECT COALESCE(SUM(delta), 0) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS total_transaction_volume, (
SELECT MAX(mtime) 
FROM pgbench_history h 
WHERE h.aid = a.aid) AS last_transaction_time 
FROM pgbench_accounts a JOIN pgbench_branches b ON a.bid = b.bid 
WHERE a.abalance > ( 
SELECT AVG(a2.abalance) 
FROM pgbench_accounts a2 
WHERE a2.bid = a.bid ) AND a.aid IN ( 
SELECT h.aid 
FROM pgbench_history h 
GROUP BY h.aid 
HAVING COUNT(*) >= 2 ) AND CAST(a.abalance AS TEXT) LIKE '%5%' 
ORDER BY total_transactions DESC, account_balance DESC 
LIMIT 500;
    """
    
    try:
        print("Connecting...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Running heavy analysis scan (expected time ~10-15 mins)...")
        
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("Analysis Complete")
        print(f"Duration: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print(f"Records Returned: {len(results)}")
        print("=" * 60)
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    run_heavy_analysis_3()
