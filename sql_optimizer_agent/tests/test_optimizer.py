#!/usr/bin/env python3
"""
Test the SQL Query Optimizer Agent with the long-running query
"""

import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter


# The slow query from long_query_postgres_compute.py
SLOW_QUERY = """
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


def main():
    print("=" * 60)
    print("SQL Query Optimizer Agent - Test")
    print("=" * 60)
    print()
    print("Testing with the long-running query from Database_queries...")
    print()
    
    # Initialize orchestrator (disable LLM for faster testing initially)
    orchestrator = AgentOrchestrator(use_llm=True, use_explain=False)
    formatter = OutputFormatter()
    
    try:
        # Run optimization
        print("Analyzing query...")
        result = orchestrator.optimize(SLOW_QUERY)
        
        # Print results using rich formatter
        formatter.print_result(result)
        
        # Also print the line-by-line report
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
        print("TEST COMPLETED SUCCESSFULLY")
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
