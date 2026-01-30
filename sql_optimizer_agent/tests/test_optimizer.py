#!/usr/bin/env python3
"""
Test the SQL Query Optimizer Agent with the realistic fraud detection query
"""

import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter


# The fraud detection query from long_query_postgres_compute.py
FRAUD_DETECTION_QUERY = """
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


def main():
    print("=" * 60)
    print("SQL Query Optimizer Agent - Test")
    print("=" * 60)
    print()
    print("Testing with the Fraud Detection Query...")
    print()
    
    # Initialize orchestrator
    orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
    formatter = OutputFormatter()
    
    try:
        # Run optimization
        print("Analyzing query...")
        result = orchestrator.optimize(FRAUD_DETECTION_QUERY)
        
        # Print results
        formatter.print_result(result)
        
        # Print line-by-line report
        print("\n" + "=" * 60)
        print("LINE-BY-LINE OPTIMIZATION REPORT")
        print("=" * 60)
        
        report = orchestrator.get_line_by_line_report(result)
        for entry in report:
            print(f"\nLine {entry['line']}:")
            print(f"  Original:   {entry.get('original', 'N/A')}")
            print(f"  Issue:      {entry.get('issue', 'N/A')}")
            print(f"  Severity:   {entry.get('severity', 'N/A')}")
            if 'optimized' in entry:
                print(f"  Optimized:  {entry['optimized']}")
            print(f"  Suggestion: {entry.get('suggestion', 'N/A')}")
        
        print("\n" + "=" * 60)
        print("TEST COMPLETED")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        orchestrator.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
