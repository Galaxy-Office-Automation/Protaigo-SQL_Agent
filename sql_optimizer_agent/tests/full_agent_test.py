#!/usr/bin/env python3
"""
SQL Optimizer Agent — Full End-to-End Test
==========================================
Tests that the agent produces optimized SQL which:
  1. Passes syntax validation (static + EXPLAIN dry-run)
  2. Executes without errors against the live database
  3. Completes in under 10 seconds

Queries tested:
  Q1 – Fraud Detection (self-join explosion)
  Q2 – Window Function Overlap (rolling metrics + self-join on 2.5M rows)
  Q3 – Recursive CTE Depth (WITH RECURSIVE, depth < 6)
"""

import sys
import time
import textwrap
import psycopg2
from datetime import datetime

sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
from validator.syntax_validator import SyntaxValidator

# ─── Database config ────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}

MAX_EXECUTION_SECONDS = 10  # target: optimized query must finish in < 10s

# ─── Test Queries ───────────────────────────────────────────────────

QUERIES = {
    "Q1_FraudDetection": textwrap.dedent("""\
        WITH account_data AS (
            SELECT 
                a.aid, a.bid, a.abalance, b.bbalance,
                NTILE(100) OVER (ORDER BY a.abalance) as percentile
            FROM pgbench_accounts a
            JOIN pgbench_branches b ON a.bid = b.bid
        ),
        peer_comparison AS (
            SELECT 
                a1.aid as account_id, a1.bid as branch_id,
                a1.abalance as balance, a2.aid as peer_id,
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
                account_id, branch_id, balance,
                COUNT(DISTINCT peer_id) as peer_count,
                AVG(difference) as avg_diff,
                AVG(similarity) as avg_similarity,
                STDDEV(difference) as diff_volatility
            FROM peer_comparison
            GROUP BY account_id, branch_id, balance
            HAVING COUNT(DISTINCT peer_id) >= 10
        ),
        risk_scores AS (
            SELECT r.*,
                CASE 
                    WHEN avg_similarity < 0.3 THEN 'HIGH_RISK'
                    WHEN avg_similarity < 0.5 THEN 'MEDIUM_RISK'
                    WHEN avg_similarity < 0.7 THEN 'LOW_RISK'
                    ELSE 'NORMAL'
                END as risk_level,
                (1 - avg_similarity) * 100 as anomaly_score
            FROM risk_metrics r
        )
        SELECT account_id, branch_id, balance, peer_count,
               avg_diff, avg_similarity, anomaly_score, risk_level
        FROM risk_scores
        WHERE risk_level IN ('HIGH_RISK', 'MEDIUM_RISK')
        ORDER BY anomaly_score DESC
        LIMIT 500;
    """),

    "Q2_WindowFunctionOverlap": textwrap.dedent("""\
        WITH base_data AS (
            SELECT 
                aid, bid, abalance,
                (CURRENT_DATE - (aid % 365) * INTERVAL '1 day') as synthetic_date,
                LENGTH(filler) as filler_len
            FROM pgbench_accounts
            WHERE aid <= 2500000
        ),
        rolling_metrics AS (
            SELECT 
                aid, bid, abalance, synthetic_date,
                SUM(abalance) OVER (
                    PARTITION BY bid ORDER BY synthetic_date 
                    ROWS BETWEEN 2000 PRECEDING AND 2000 FOLLOWING
                ) as rolling_sum_4000,
                AVG(abalance) OVER (
                    PARTITION BY bid ORDER BY synthetic_date 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as cumulative_avg,
                STDDEV(abalance) OVER (
                    PARTITION BY bid ORDER BY synthetic_date 
                    ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW
                ) as rolling_volatility,
                MAX(abalance) OVER (
                    PARTITION BY bid ORDER BY synthetic_date
                    ROWS BETWEEN 5000 PRECEDING AND 5000 FOLLOWING
                ) as rolling_max
            FROM base_data
        ),
        volatility_clusters AS (
            SELECT 
                r1.aid as aid_1, r2.aid as aid_2, r1.bid,
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
            bid, synthetic_date,
            COUNT(*) as cluster_density,
            AVG(sum_divergence) as avg_divergence,
            CORR(vol_1, vol_2) as volatility_correlation,
            MAX(vol_1) * MAX(vol_2) as max_volatility_product
        FROM volatility_clusters
        GROUP BY bid, synthetic_date
        HAVING COUNT(*) > 100
        ORDER BY volatility_correlation DESC NULLS LAST, avg_divergence DESC
        LIMIT 2000;
    """),

    "Q3_RecursiveCTE": textwrap.dedent("""\
        WITH RECURSIVE transaction_chain AS (
            SELECT 
                aid as root_aid, aid as current_aid, bid, abalance,
                1 as chain_depth,
                CAST(aid AS VARCHAR) as path,
                md5(CAST(aid AS VARCHAR)) as hash_trail
            FROM pgbench_accounts
            WHERE aid % 1000 = 0 AND aid <= 500000
            
            UNION ALL
            
            SELECT 
                c.root_aid, a.aid as current_aid, a.bid, a.abalance,
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
                root_aid, chain_depth,
                COUNT(*) as variations,
                SUM(abalance) as total_chain_balance,
                MAX(LENGTH(path)) as max_path_len,
                SUM(CASE WHEN hash_trail LIKE '%a%' THEN 1 ELSE 0 END) as hash_a_count,
                SUM(CASE WHEN hash_trail LIKE '%b%' THEN 1 ELSE 0 END) as hash_b_count
            FROM transaction_chain
            GROUP BY root_aid, chain_depth
        )
        SELECT 
            c1.root_aid as primary_root, c2.root_aid as secondary_root,
            c1.chain_depth,
            c1.variations * c2.variations as interaction_complexity,
            c1.total_chain_balance + c2.total_chain_balance as combined_balance,
            POWER(c1.hash_a_count - c2.hash_b_count, 2) as hash_variance
        FROM chain_analytics c1
        JOIN chain_analytics c2 ON c1.chain_depth = c2.chain_depth AND c1.root_aid != c2.root_aid
        WHERE c1.variations > 50
        ORDER BY interaction_complexity DESC, hash_variance DESC
        LIMIT 1000;
    """),
}

# ─── Helpers ────────────────────────────────────────────────────────

def execute_query(query: str, timeout_seconds: int = MAX_EXECUTION_SECONDS):
    """Execute a query and return (elapsed, row_count, error_string|None)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = '{timeout_seconds * 1000}'")  # ms
        start = time.time()
        cur.execute(query)
        rows = cur.fetchall()
        elapsed = time.time() - start
        row_count = len(rows)
        cur.close()
        conn.close()
        return elapsed, row_count, None
    except Exception as e:
        return None, 0, str(e)


# ─── Main Test Runner ───────────────────────────────────────────────

def run_tests():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_lines = []

    def log(msg=""):
        print(msg)
        report_lines.append(msg)

    log("=" * 72)
    log("  SQL OPTIMIZER AGENT — FULL END-TO-END TEST REPORT")
    log("=" * 72)
    log(f"  Date      : {now}")
    log(f"  Threshold : optimized query must finish < {MAX_EXECUTION_SECONDS}s with 0 errors")
    log(f"  Mode      : Rule-based (no LLM)")
    log(f"  Queries   : {len(QUERIES)}")
    log("=" * 72)
    log()

    # Initialize agent once
    orchestrator = AgentOrchestrator(use_llm=False, use_explain=False)
    syntax_validator = SyntaxValidator()

    overall_pass = True
    results_summary = []

    for label, original_sql in QUERIES.items():
        log("-" * 72)
        log(f"  TEST: {label}")
        log("-" * 72)

        checks = {
            "optimization_produced": False,
            "syntax_valid": False,
            "execution_no_error": False,
            "execution_under_10s": False,
        }
        exec_time = None
        exec_rows = 0
        exec_error = None
        optimized_sql = ""
        bottleneck_count = 0
        suggestion_count = 0
        syntax_errors = []

        # ── Step 1: Run agent optimization ──
        try:
            result = orchestrator.optimize(original_sql)
            optimized_sql = result.optimized_query
            bottleneck_count = len(result.bottlenecks)
            suggestion_count = len(result.suggestions)
            # Check that something changed
            if optimized_sql.strip() and optimized_sql.strip() != original_sql.strip():
                checks["optimization_produced"] = True
            else:
                log("  ⚠  Agent returned the original query unchanged.")
        except Exception as e:
            log(f"  ✗  Agent optimization crashed: {e}")
            overall_pass = False
            results_summary.append((label, checks, None, 0, str(e)))
            log()
            continue

        log(f"  Bottlenecks detected : {bottleneck_count}")
        log(f"  Suggestions generated: {suggestion_count}")

        # ── Step 2: Syntax validation ──
        try:
            validation = syntax_validator.validate(optimized_sql)
            if validation.is_valid:
                checks["syntax_valid"] = True
            else:
                syntax_errors = validation.errors
                log(f"  ✗  Syntax validation failed: {validation.errors}")
        except Exception as e:
            log(f"  ✗  Syntax validator crashed: {e}")

        # ── Step 3: Execute optimized query ──
        if checks["syntax_valid"]:
            exec_time, exec_rows, exec_error = execute_query(
                optimized_sql, timeout_seconds=MAX_EXECUTION_SECONDS + 5  # slight grace
            )
            if exec_error is None:
                checks["execution_no_error"] = True
                if exec_time is not None and exec_time < MAX_EXECUTION_SECONDS:
                    checks["execution_under_10s"] = True
            else:
                log(f"  ✗  Execution error: {exec_error}")
        else:
            log("  ⏭  Skipping execution (syntax invalid)")

        # ── Print per-query results ──
        log()
        log(f"  {'CHECK':<30} {'RESULT':<8}")
        log(f"  {'─'*30} {'─'*8}")
        for check_name, passed in checks.items():
            icon = "✓ PASS" if passed else "✗ FAIL"
            log(f"  {check_name:<30} {icon}")

        if exec_time is not None:
            log(f"\n  Execution time : {exec_time:.3f}s")
            log(f"  Rows returned  : {exec_rows}")
        if syntax_errors:
            log(f"  Syntax errors  : {syntax_errors}")
        log()

        query_pass = all(checks.values())
        if not query_pass:
            overall_pass = False
        results_summary.append((label, checks, exec_time, exec_rows, exec_error))

    orchestrator.close()

    # ── Overall Summary ─────────────────────────────────────────────
    log("=" * 72)
    log("  SUMMARY")
    log("=" * 72)
    log()
    log(f"  {'QUERY':<30} {'OPT':<6} {'SYN':<6} {'EXEC':<6} {'<10s':<6} {'TIME':>8}")
    log(f"  {'─'*30} {'─'*6} {'─'*6} {'─'*6} {'─'*6} {'─'*8}")
    for label, checks, t, rows, err in results_summary:
        opt  = "✓" if checks["optimization_produced"] else "✗"
        syn  = "✓" if checks["syntax_valid"]          else "✗"
        exe  = "✓" if checks["execution_no_error"]    else "✗"
        spd  = "✓" if checks["execution_under_10s"]   else "✗"
        time_str = f"{t:.3f}s" if t is not None else "N/A"
        log(f"  {label:<30} {opt:<6} {syn:<6} {exe:<6} {spd:<6} {time_str:>8}")
    log()

    if overall_pass:
        log("  ✅ OVERALL RESULT: ALL TESTS PASSED")
    else:
        log("  ❌ OVERALL RESULT: SOME TESTS FAILED")
    log()
    log("=" * 72)

    # ── Save report ─────────────────────────────────────────────────
    report_path = '/home/galaxy/DB_setup/sql_optimizer_agent/tests/full_agent_report.txt'
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    print(f"\nReport saved to: {report_path}")

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(run_tests())
