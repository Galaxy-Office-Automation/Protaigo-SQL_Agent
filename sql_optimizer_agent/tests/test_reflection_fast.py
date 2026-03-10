
import sys
import os
import logging

# Ensure the agent modules are in path
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
import time

# Configure logging to see reflection progress
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def test_reflection_fast():
    orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
    
    # Bounded query for faster execution
    fast_query = """
    WITH account_data AS (
        SELECT 
            a.aid,
            a.bid,
            a.abalance,
            b.bbalance,
            NTILE(100) OVER (ORDER BY a.abalance) as percentile
        FROM pgbench_accounts a
        JOIN pgbench_branches b ON a.bid = b.bid
        WHERE a.aid <= 1000 -- Bounded
    ),
    peer_comparison AS (
        SELECT 
            a1.aid as account_id,
            a2.aid as peer_id,
            ABS(a1.abalance - a2.abalance) as difference
        FROM account_data a1
        JOIN account_data a2 ON a1.bid = a2.bid 
            AND a1.aid != a2.aid
            AND ABS(a1.percentile - a2.percentile) <= 5
        WHERE a1.aid <= 500 AND a2.aid <= 500
    )
    SELECT 
        account_id,
        COUNT(DISTINCT peer_id) as peer_count,
        AVG(difference) as avg_diff
    FROM peer_comparison
    GROUP BY account_id
    HAVING COUNT(DISTINCT peer_id) >= 2
    ORDER BY account_id
    LIMIT 10;
    """
    
    print("Starting fast optimization with reflection...")
    start_time = time.time()
    result = orchestrator.optimize(fast_query)
    end_time = time.time()
    
    print(f"\nOptimization completed in {end_time - start_time:.2f} seconds.")
    print("\n--- OPTIMIZED QUERY ---")
    print(result.optimized_query)
    
    print("\n--- IMPROVEMENT ESTIMATE ---")
    print(result.expected_improvement)
    
if __name__ == "__main__":
    test_reflection_fast()
