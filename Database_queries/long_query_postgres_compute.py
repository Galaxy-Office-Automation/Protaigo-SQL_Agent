#!/usr/bin/env python3
"""
Fraud Detection Analysis
=========================
Identifies accounts with unusual patterns compared to peers.
Used by compliance team for quarterly risk assessment.
"""

import psycopg2
import time
from datetime import datetime

DB_CONFIG = {
    "host": "10.10.90.92",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}


def run_fraud_analysis():
    """Analyze accounts for suspicious patterns."""
    print("=" * 60)
    print("Fraud Detection Analysis")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    query = """
    WITH account_data AS (
        SELECT 
            a.aid,
            a.bid,
            a.abalance,
            b.bbalance,
            NTILE(100) OVER (ORDER BY a.abalance) as percentile
        FROM pgbench_accounts a
        JOIN pgbench_branches b ON a.bid = b.bid
    ),
    peer_comparison AS (
        SELECT 
            a1.aid as account_id,
            a1.bid as branch_id,
            a1.abalance as balance,
            a2.aid as peer_id,
            a2.abalance as peer_balance,
            ABS(a1.abalance - a2.abalance) as difference,
            CASE 
                WHEN a1.abalance > 0 AND a2.abalance > 0 
                THEN LEAST(a1.abalance, a2.abalance)::FLOAT / 
                     GREATEST(a1.abalance, a2.abalance)
                ELSE 0 
            END as similarity
        FROM account_data a1
        JOIN account_data a2 ON a1.bid = a2.bid 
            AND a1.aid != a2.aid
            AND ABS(a1.percentile - a2.percentile) <= 5
        WHERE a1.aid <= 80000 AND a2.aid <= 80000
    ),
    risk_metrics AS (
        SELECT 
            account_id,
            branch_id,
            balance,
            COUNT(DISTINCT peer_id) as peer_count,
            AVG(difference) as avg_diff,
            AVG(similarity) as avg_similarity,
            STDDEV(difference) as diff_volatility
        FROM peer_comparison
        GROUP BY account_id, branch_id, balance
        HAVING COUNT(DISTINCT peer_id) >= 10
    ),
    risk_scores AS (
        SELECT 
            r.*,
            CASE 
                WHEN avg_similarity < 0.3 THEN 'HIGH_RISK'
                WHEN avg_similarity < 0.5 THEN 'MEDIUM_RISK'
                WHEN avg_similarity < 0.7 THEN 'LOW_RISK'
                ELSE 'NORMAL'
            END as risk_level,
            (1 - avg_similarity) * 100 as anomaly_score
        FROM risk_metrics r
    )
    SELECT 
        account_id,
        branch_id,
        balance,
        peer_count,
        avg_diff,
        avg_similarity,
        anomaly_score,
        risk_level
    FROM risk_scores
    WHERE risk_level IN ('HIGH_RISK', 'MEDIUM_RISK')
    ORDER BY anomaly_score DESC
    LIMIT 500;
    """
    
    try:
        print("Connecting...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Running fraud detection scan...")
        
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("Analysis Complete")
        print(f"Duration: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print(f"Suspicious Accounts: {len(results)}")
        print("=" * 60)
        
        if results:
            high = len([r for r in results if r[7] == 'HIGH_RISK'])
            med = len([r for r in results if r[7] == 'MEDIUM_RISK'])
            print(f"\nRisk Summary: HIGH={high}, MEDIUM={med}")
            
            print("\nTop Flagged Accounts:")
            for row in results[:5]:
                print(f"  Account {row[0]}: Score={row[6]:.1f}, Risk={row[7]}")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_fraud_analysis()
