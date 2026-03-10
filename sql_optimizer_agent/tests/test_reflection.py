
import sys
import os

# Ensure the agent modules are in path
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
import time
import logging

# Configure logging to see reflection progress
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def test_reflection_on_fraud_query():
    orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
    
    fraud_query = """
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
    
    print("Starting optimization with reflection...")
    start_time = time.time()
    result = orchestrator.optimize(fraud_query)
    end_time = time.time()
    
    print(f"\nOptimization completed in {end_time - start_time:.2f} seconds.")
    print("\n--- ORIGINAL QUERY ---")
    # print(fraud_query[:200] + "...")
    
    print("\n--- OPTIMIZED QUERY ---")
    print(result.optimized_query)
    
    print("\n--- IMPROVEMENT ESTIMATE ---")
    print(result.expected_improvement)
    
    # Check if the optimized query still has the LIMIT 500 at the end (correct)
    # but doesn't have the arbitrary LIMIT 1000 inside CTEs (which was likely causing the issue)
    
if __name__ == "__main__":
    test_reflection_on_fraud_query()
