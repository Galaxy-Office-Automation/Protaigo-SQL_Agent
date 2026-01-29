#!/usr/bin/env python3
"""
Account Transaction Analysis Report
====================================
This script generates a comprehensive account activity report
analyzing transaction patterns across all branches.

Report: Monthly Account Balance Reconciliation
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


def generate_account_analysis_report():
    """
    Generate comprehensive account analysis report.
    This query analyzes account balance distributions and 
    identifies outlier patterns across all branches.
    """
    print("=" * 60)
    print("Account Transaction Analysis Report")
    print("=" * 60)
    print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Analyzing account balance patterns across all branches...")
    print("-" * 60)
    
    # Business analytics query - Account balance analysis with peer comparison
    # This is a realistic reporting query that financial analysts might run
    query = """
    WITH account_stats AS (
        SELECT 
            a.aid,
            a.bid,
            a.abalance,
            b.bbalance as branch_balance,
            t.tbalance as teller_balance,
            AVG(a.abalance) OVER (PARTITION BY a.bid) as branch_avg_balance,
            STDDEV(a.abalance) OVER (PARTITION BY a.bid) as branch_stddev,
            RANK() OVER (PARTITION BY a.bid ORDER BY a.abalance DESC) as rank_in_branch,
            PERCENT_RANK() OVER (ORDER BY a.abalance) as percentile_rank
        FROM pgbench_accounts a
        JOIN pgbench_branches b ON a.bid = b.bid
        LEFT JOIN pgbench_tellers t ON a.bid = t.bid
    ),
    branch_summary AS (
        SELECT 
            bid,
            COUNT(*) as account_count,
            SUM(abalance) as total_balance,
            AVG(abalance) as avg_balance,
            MAX(abalance) as max_balance,
            MIN(abalance) as min_balance,
            STDDEV(abalance) as balance_volatility
        FROM pgbench_accounts
        GROUP BY bid
    ),
    outlier_detection AS (
        SELECT 
            s.aid,
            s.bid,
            s.abalance,
            s.branch_avg_balance,
            s.branch_stddev,
            CASE 
                WHEN s.abalance > s.branch_avg_balance + (2 * s.branch_stddev) THEN 'HIGH_OUTLIER'
                WHEN s.abalance < s.branch_avg_balance - (2 * s.branch_stddev) THEN 'LOW_OUTLIER'
                ELSE 'NORMAL'
            END as outlier_status,
            ABS(s.abalance - s.branch_avg_balance) / NULLIF(s.branch_stddev, 0) as z_score
        FROM account_stats s
    ),
    cross_branch_comparison AS (
        SELECT 
            o1.bid as branch_1,
            o2.bid as branch_2,
            COUNT(*) as comparison_pairs,
            AVG(ABS(o1.abalance - o2.abalance)) as avg_balance_difference,
            CORR(o1.abalance::FLOAT, o2.abalance::FLOAT) as balance_correlation
        FROM outlier_detection o1
        JOIN outlier_detection o2 ON o1.aid < o2.aid
        WHERE o1.outlier_status = 'NORMAL' AND o2.outlier_status = 'NORMAL'
        GROUP BY o1.bid, o2.bid
        HAVING COUNT(*) > 100
    )
    SELECT 
        c.branch_1,
        c.branch_2,
        c.comparison_pairs,
        c.avg_balance_difference,
        c.balance_correlation,
        b1.total_balance as branch_1_total,
        b2.total_balance as branch_2_total,
        b1.balance_volatility as branch_1_volatility,
        b2.balance_volatility as branch_2_volatility
    FROM cross_branch_comparison c
    JOIN branch_summary b1 ON c.branch_1 = b1.bid
    JOIN branch_summary b2 ON c.branch_2 = b2.bid
    ORDER BY c.balance_correlation DESC NULLS LAST, c.avg_balance_difference
    LIMIT 1000;
    """
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected. Running analysis...")
        
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("Report Generation Complete")
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        print(f"Records Analyzed: {len(results)}")
        print("=" * 60)
        
        if results:
            print("\nTop Branch Correlations:")
            print("-" * 60)
            for i, row in enumerate(results[:5], 1):
                print(f"  {i}. Branch {row[0]} <-> Branch {row[1]}: "
                      f"Correlation={row[4]:.3f if row[4] else 'N/A'}, "
                      f"Pairs={row[2]}")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\nDatabase Error: {e}")
        raise


if __name__ == "__main__":
    generate_account_analysis_report()
