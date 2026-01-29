#!/usr/bin/env python3
"""
Long Running PostgreSQL Query - Compute Intensive
==================================================
This script executes a CPU-intensive query using generate_series
and multiple aggregations to simulate real database load.

Duration: ~20-25 minutes (depends on system resources)
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


def run_compute_intensive_query():
    """Execute a CPU-intensive query that runs for 20-25 minutes."""
    print("=" * 60)
    print("Long Running PostgreSQL Query - Compute Intensive")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Expected Duration: 20-25 minutes")
    print("-" * 60)
    
    # CPU-intensive query using cross joins on pgbench_accounts table
    # This forces multiple full table scans and heavy computation
    # Expected runtime: 20-25 minutes
    query = """
    WITH 
    -- First pass: sample accounts for cross join
    sample_a AS (
        SELECT aid, bid, abalance 
        FROM pgbench_accounts 
        WHERE aid <= 50000
    ),
    sample_b AS (
        SELECT aid, bid, abalance 
        FROM pgbench_accounts 
        WHERE aid BETWEEN 50001 AND 100000
    ),
    -- Cross join creates 50000 * 50000 = 2.5 billion row combinations
    cross_computed AS (
        SELECT 
            a.aid AS aid1,
            b.aid AS aid2,
            a.bid AS bid1,
            b.bid AS bid2,
            a.abalance AS bal1,
            b.abalance AS bal2,
            ABS(a.abalance - b.abalance) AS balance_diff,
            (a.abalance + b.abalance)::BIGINT AS combined_balance,
            SQRT(ABS(a.abalance::FLOAT * b.abalance::FLOAT) + 1) AS sqrt_product,
            LOG(ABS(a.abalance::FLOAT) + ABS(b.abalance::FLOAT) + 2) AS log_sum,
            SIN(a.abalance::FLOAT / 1000) * COS(b.abalance::FLOAT / 1000) AS trig_calc,
            POWER(ABS(a.abalance - b.abalance)::FLOAT + 1, 0.3) AS power_diff
        FROM sample_a a
        CROSS JOIN sample_b b
    ),
    -- Heavy aggregation over 2.5 billion rows
    aggregated AS (
        SELECT 
            bid1,
            bid2,
            COUNT(*) AS pair_count,
            SUM(balance_diff)::NUMERIC AS total_diff,
            AVG(combined_balance) AS avg_combined,
            STDDEV(sqrt_product) AS stddev_sqrt,
            SUM(log_sum) AS sum_log,
            AVG(trig_calc) AS avg_trig,
            MAX(power_diff) AS max_power,
            MIN(power_diff) AS min_power,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY balance_diff) AS median_diff
        FROM cross_computed
        GROUP BY bid1, bid2
    ),
    -- Additional computation pass
    final_calc AS (
        SELECT 
            *,
            total_diff / NULLIF(pair_count, 0) AS normalized_diff,
            avg_combined * stddev_sqrt AS complexity_score,
            RANK() OVER (ORDER BY total_diff DESC) AS diff_rank
        FROM aggregated
    )
    SELECT 
        bid1,
        bid2,
        pair_count,
        total_diff,
        avg_combined,
        complexity_score,
        diff_rank
    FROM final_calc
    ORDER BY complexity_score DESC NULLS LAST
    LIMIT 500;
    """
    
    try:
        # Establish connection
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected successfully!")
        
        # Execute long-running query
        print("\nExecuting compute-intensive query...")
        print("This query generates 50 million rows and performs heavy aggregations.")
        print("Query is now running... (This will take 20-25 minutes)")
        
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("Query completed successfully!")
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Actual Duration: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        print(f"Rows Returned: {len(results)}")
        print("=" * 60)
        
        # Display sample results
        if results:
            print("\nSample Results (first 5 rows):")
            print("-" * 60)
            for row in results[:5]:
                print(f"  Bucket: {row[0]}, Count: {row[1]}, Sum Squared: {row[2]:.2e}")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\nDatabase Error: {e}")
        raise
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    run_compute_intensive_query()
