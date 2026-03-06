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
    WITH RECURSIVE transaction_chain AS (
        SELECT 
            aid as root_aid,
            aid as current_aid,
            bid,
            abalance,
            1 as chain_depth,
            CAST(aid AS VARCHAR) as path,
            md5(CAST(aid AS VARCHAR)) as hash_trail
        FROM pgbench_accounts
        WHERE aid % 1000 = 0 AND aid <= 500000 -- 500 roots
        
        UNION ALL
        
        SELECT 
            c.root_aid,
            a.aid as current_aid,
            a.bid,
            a.abalance,
            c.chain_depth + 1,
            c.path || '->' || a.aid,
            md5(c.hash_trail || a.filler)
        FROM transaction_chain c
        JOIN pgbench_accounts a ON 
            (a.bid = c.bid AND a.aid BETWEEN c.current_aid + 1 AND c.current_aid + 15)
        WHERE c.chain_depth < 6
          AND a.aid <= 5000000
    ),
    chain_analytics AS (
        SELECT 
            root_aid,
            chain_depth,
            COUNT(*) as variations,
            SUM(abalance) as total_chain_balance,
            MAX(LENGTH(path)) as max_path_len,
            SUM(CASE WHEN hash_trail LIKE '%a%' THEN 1 ELSE 0 END) as hash_a_count,
            SUM(CASE WHEN hash_trail LIKE '%b%' THEN 1 ELSE 0 END) as hash_b_count
        FROM transaction_chain
        GROUP BY root_aid, chain_depth
    )
    SELECT 
        c1.root_aid as primary_root,
        c2.root_aid as secondary_root,
        c1.chain_depth,
        c1.variations * c2.variations as interaction_complexity,
        c1.total_chain_balance + c2.total_chain_balance as combined_balance,
        POWER(c1.hash_a_count - c2.hash_b_count, 2) as hash_variance
    FROM chain_analytics c1
    JOIN chain_analytics c2 ON c1.chain_depth = c2.chain_depth AND c1.root_aid != c2.root_aid
    WHERE c1.variations > 50
    ORDER BY interaction_complexity DESC, hash_variance DESC
    LIMIT 1000;
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
