#!/usr/bin/env python3
"""
Recursive CTE Depth Analysis
=============================
Simulates an extremely heavy analytical query using Deep Recursive CTEs
and string aggregations over large combinations to stress CPU.
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

def run_heavy_analysis_2():
    print("=" * 60)
    print("Heavy Analysis 2: Recursive CTEs and String Aggregations")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    # This query combines actual table access with sleep delays to simulate
    # a long-running row-by-row operation taking ~12 minutes
    query = """
    WITH base_data AS (
        SELECT 
            aid,
            bid,
            abalance,
            (CURRENT_DATE - (aid % 365) * INTERVAL '1 day') as synthetic_date,
            LENGTH(filler) as filler_len
        FROM pgbench_accounts
        WHERE aid <= 2500000 -- 2.5 Million rows to process
    ),
    rolling_metrics AS (
        SELECT 
            aid,
            bid,
            abalance,
            synthetic_date,
            SUM(abalance) OVER (
                PARTITION BY bid 
                ORDER BY synthetic_date 
                ROWS BETWEEN 2000 PRECEDING AND 2000 FOLLOWING
            ) as rolling_sum_4000,
            AVG(abalance) OVER (
                PARTITION BY bid 
                ORDER BY synthetic_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_avg,
            STDDEV(abalance) OVER (
                PARTITION BY bid
                ORDER BY synthetic_date 
                ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW
            ) as rolling_volatility,
            MAX(abalance) OVER (
                PARTITION BY bid
                ORDER BY synthetic_date
                ROWS BETWEEN 5000 PRECEDING AND 5000 FOLLOWING
            ) as rolling_max
        FROM base_data
    ),
    volatility_clusters AS (
        SELECT 
            r1.aid as aid_1,
            r2.aid as aid_2,
            r1.bid,
            r1.synthetic_date,
            r1.rolling_volatility as vol_1,
            r2.rolling_volatility as vol_2,
            ABS(r1.rolling_sum_4000 - r2.rolling_sum_4000) as sum_divergence
        FROM rolling_metrics r1
        JOIN rolling_metrics r2 ON 
            r1.bid = r2.bid AND 
            r1.synthetic_date = r2.synthetic_date AND
            r1.aid != r2.aid AND
            ABS(r1.abalance - r2.abalance) < 10
    )
    SELECT 
        bid,
        synthetic_date,
        COUNT(*) as cluster_density,
        AVG(sum_divergence) as avg_divergence,
        CORR(vol_1, vol_2) as volatility_correlation,
        MAX(vol_1) * MAX(vol_2) as max_volatility_product
    FROM volatility_clusters
    GROUP BY bid, synthetic_date
    HAVING COUNT(*) > 100
    ORDER BY volatility_correlation DESC NULLS LAST, avg_divergence DESC
    LIMIT 2000;
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
    run_heavy_analysis_2()
